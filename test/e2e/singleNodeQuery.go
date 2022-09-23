/*
Copyright 2022 Authors of Global Resource Service.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"

	"global-resource-service/resource-management/pkg/clientSdk/rmsclient"
	"global-resource-service/resource-management/pkg/common-lib/types"
	"global-resource-service/resource-management/pkg/common-lib/types/location"
	"global-resource-service/resource-management/pkg/store/redis"
	"global-resource-service/test/e2e/stats"
)

type testConfig struct {
	testDuration       time.Duration
	singleNodeNum      int
	singleNodeInterval time.Duration
	batchNodeNum       int
	remoteRedisPort    string
	batchNodeInterval  time.Duration
}

func main() {
	flag.Usage = printUsage

	testCfg := testConfig{}
	cfg := rmsclient.Config{}

	flag.StringVar(&cfg.ServiceUrl, "service_url", "localhost:8080", "Service IP address, if not set, default to localhost")
	flag.DurationVar(&cfg.RequestTimeout, "request_timeout", 30*time.Minute, "Timeout for client requests and responses")
	flag.DurationVar(&testCfg.testDuration, "test_duration", 30*time.Minute, "Test duration, measured by number minutes of watch of node changes. default 10 minutes")
	flag.IntVar(&testCfg.singleNodeNum, "single_node_num", 1, "Number of single node set requested from redis, default to 1")
	flag.DurationVar(&testCfg.singleNodeInterval, "single_node_interval", 1*time.Second, "Query interval of single node, default to 1s")
	flag.IntVar(&testCfg.batchNodeNum, "batch_node_num", 10, "Number of batch node, default to 0s")
	flag.StringVar(&testCfg.remoteRedisPort, "remote_redis_port", "7379", "Remote redis port, if not set, default to 7379")
	flag.DurationVar(&testCfg.batchNodeInterval, "batch_node_interval", 1*time.Minute, "Query interval of batch node, default to 1m")

	if !flag.Parsed() {
		klog.InitFlags(nil)
		flag.Parse()
	}
	klog.StartFlushDaemon(time.Second * 1)
	defer klog.Flush()

	//there is two test model:
	//1. singel node per query per second
	//2. batch nodes query every minuute.
	// all var with singelNode is for model #1
	// all var with batchNode is for nodel #2
	client := rmsclient.NewRmsClient(cfg)
	singleNodeStats := stats.NewNodeQueryStats()
	batchNodeStats := stats.NewNodeQueryStats()
	singleNodeStats.NodeQueryInterval = testCfg.singleNodeInterval
	singleNodeStats.NumberOfNodes = 1
	batchNodeStats.NodeQueryInterval = testCfg.batchNodeInterval
	batchNodeStats.NumberOfNodes = testCfg.batchNodeNum

	singleNodeTimeout := time.After(testCfg.testDuration)
	singleNodeFinish := make(chan bool)
	batchNodeTimeout := time.After(testCfg.testDuration)
	batchNodeFinish := make(chan bool)

	serviceInfo := strings.Split(cfg.ServiceUrl, ":")
	if len(serviceInfo) == 0 {
		klog.Errorf("Please ensure correct service url is provided, for example: 127.0.0.1:8080")
		os.Exit(0)
	}

	// get multi nodes from redis for single node model and batch node model
	remoteRedisIp := serviceInfo[0]
	remoteRedisPort := testCfg.remoteRedisPort
	klog.V(3).Infof("Connecting the Redis server at %v:%v", remoteRedisIp, remoteRedisPort)
	store := redis.NewRedisClient(remoteRedisIp, remoteRedisPort, false)
	requiredNum := testCfg.batchNodeNum + testCfg.singleNodeNum
	startTime := time.Now().UTC()
	klog.Infof("Requesting nodes from redis server")
	logicalNodes := store.BatchLogicalNodesInquiry(requiredNum)
	endTime := time.Since(startTime)
	klog.Infof("Total %v nodes required from redis server: %v, Total nodes got from redis: %v in duration: %v, detailes: %v\n", requiredNum, remoteRedisIp, len(logicalNodes), endTime, logicalNodes)

	//split nodes from redis to singleNodeSet and batchNodeSet
	singleNodeSet := make([]*types.LogicalNode, testCfg.singleNodeNum)
	batchNodeSet := make([]*types.LogicalNode, testCfg.batchNodeNum)

	for i := 0; i < requiredNum; i++ {
		if i < testCfg.singleNodeNum {
			singleNodeSet[i] = logicalNodes[i]
		} else {
			num := i - testCfg.singleNodeNum
			batchNodeSet[num] = logicalNodes[i]
		}
	}

	var wgNode sync.WaitGroup
	var wgMain sync.WaitGroup
	wgMain.Add(2)

	go func(wgmain *sync.WaitGroup, wgnode *sync.WaitGroup, client rmsclient.RmsInterface, testCfg *testConfig, nqs *stats.NodeQueryStats, timeout <-chan time.Time, finish chan bool) {
		defer wgmain.Done()
		for {
			select {
			case <-timeout:
				finish <- true
				return
			default:
				wgnode.Add(1)
				randomIndex := rand.Intn(len(singleNodeSet))
				pick := singleNodeSet[randomIndex]
				go queryNodeStatus(wgnode, client, nqs, pick)
			}
			time.Sleep(testCfg.singleNodeInterval)
		}

	}(&wgMain, &wgNode, client, &testCfg, singleNodeStats, singleNodeTimeout, singleNodeFinish)

	go func(wgmain *sync.WaitGroup, wgnode *sync.WaitGroup, client rmsclient.RmsInterface, testCfg *testConfig, nqs *stats.NodeQueryStats, timeout <-chan time.Time, finish chan bool) {
		defer wgmain.Done()
		for {
			select {
			case <-timeout:
				finish <- true
				return
			default:
				for i := 0; i < testCfg.batchNodeNum; i++ {
					wgnode.Add(1)
					go queryNodeStatus(wgnode, client, nqs, batchNodeSet[i])
				}
			}
			time.Sleep(testCfg.batchNodeInterval)
		}
	}(&wgMain, &wgNode, client, &testCfg, batchNodeStats, batchNodeTimeout, batchNodeFinish)

	wgNode.Wait()
	<-singleNodeFinish
	<-batchNodeFinish
	wgMain.Wait()
	printTestStats(singleNodeStats, batchNodeStats)
}

// function to print the usage info for the node query testing
func printUsage() {
	fmt.Println("Usage: ")
	fmt.Println("--service_ip=127.0.0.1 --service_port=8080 --batch_node_num=100 --remote_redis_port=<port>")

	os.Exit(0)
}

func printTestStats(sns *stats.NodeQueryStats, bns *stats.NodeQueryStats) {
	sns.PrintStats()
	bns.PrintStats()
}

func addNodeLatency(delay time.Duration, nqs *stats.NodeQueryStats) {

	nqs.NodeQueryLatency.AddLatencyMetrics(delay)

}

func queryNodeStatus(wg *sync.WaitGroup, client rmsclient.RmsInterface, nqs *stats.NodeQueryStats, node *types.LogicalNode) {
	defer wg.Done()

	start := time.Now().UTC()

	nodeId := node.Id
	regionName := location.Region(node.GeoInfo.Region).String()
	rpName := location.ResourcePartition(node.GeoInfo.ResourcePartition).String()

	respNode, err := client.Query(nodeId, regionName, rpName)

	duration := time.Since(start)
	addNodeLatency(duration, nqs)
	if err != nil {
		klog.Errorf("Failed to query node status for node ID: %s. error %v", nodeId, err)
	}
	klog.V(3).Infof("Request node (nodeId: %s, regionName: %s, rpName: %s), get node (nodeId: %s, regionName: %s, rpName: %s) in duration: %v", nodeId, regionName, rpName, respNode.Id, location.Region(respNode.GeoInfo.Region).String(), location.ResourcePartition(respNode.GeoInfo.ResourcePartition).String(), duration)

	if nodeId != respNode.Id {
		klog.Errorf("Nodes Id doesn't match! Required: %v, Actual: %v\n", nodeId, respNode.Id)
	}

	if regionName != location.Region(respNode.GeoInfo.Region).String() {
		klog.Errorf("Nodes regionname doesn't match! Required: %v, Actual: %v\n", regionName, location.Region(respNode.GeoInfo.Region).String())
	}

	if rpName != location.ResourcePartition(respNode.GeoInfo.ResourcePartition).String() {
		klog.Errorf("Nodes Id doesn't match! Required: %v, Actual: %v\n", rpName, location.ResourcePartition(respNode.GeoInfo.ResourcePartition).String())
	}

}

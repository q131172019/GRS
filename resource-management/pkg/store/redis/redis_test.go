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

package redis

import (
	"os"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"global-resource-service/resource-management/pkg/common-lib/interfaces/store"
	"global-resource-service/resource-management/pkg/common-lib/types"
	"global-resource-service/resource-management/pkg/common-lib/types/location"
)

var GR, GRInquiry *Goredis
var redisPort string

func setRedisPort() string {
	if os.Getenv("REDIS_NEW_PORT") == "" {
		redisPort = "7379"
	} else {
		redisPort = os.Getenv("REDIS_NEW_PORT")
	}

	return redisPort
}

func init() {
	redisPort = setRedisPort()
	GR = NewRedisClient("localhost", redisPort, true)
	GRInquiry = NewRedisClient("localhost", redisPort, false)
}

func TestNewRedisClient(t *testing.T) {
	GR = NewRedisClient("localhost", redisPort, true)
	GRInquiry = NewRedisClient("localhost", redisPort, false)
}

// Simply Test Set String and Get String without the need of Marshal and Unmarshal
//
func TestSetGettString(t *testing.T) {
	success := GR.setString("testkey1NM", "testvalue1NM")
	if success == false {
		t.Error("Store testkey1NM into Redis in failure")
	}

	value := GR.getString("testkey1NM")
	if value == "" {
		t.Error("Read value of testkey1NM from redis in failure")
	}

	if value != "testvalue1NM" {
		t.Errorf("Unexpected value %s", string(value))
	}
}

// Simply Test Persist Nodes and Get Nodes
//
func TestPersistNodes(t *testing.T) {
	testCases := make([]*types.LogicalNode, 1)

	var testCase0 = &types.LogicalNode{
		Id:              "0001",
		ResourceVersion: "0002",
		GeoInfo: types.NodeGeoInfo{
			Region:            1000,
			ResourcePartition: 1000,
		},
		Conditions:  255,
		Reserved:    true,
		MachineType: "machineType1",
	}

	testCases[0] = testCase0

	success := GR.PersistNodes(testCases)

	if success == false {
		t.Error("Store Logical Nodes of testCases into Redis in failure")
	}

	logicalNodes := GR.GetNodes()

	if len(logicalNodes) == 0 {
		t.Error("Zero Logical Node is read from Redis")
	}

	if logicalNodes[0].Id != testCases[0].Id || logicalNodes[0].ResourceVersion != testCases[0].ResourceVersion {
		t.Error("logicalNodes[0] is : ", *logicalNodes[0])
		t.Error("testCases[0]    is : ", *testCases[0])
	}

	if logicalNodes[0].GeoInfo.Region != testCases[0].GeoInfo.Region || logicalNodes[0].GeoInfo.ResourcePartition != testCases[0].GeoInfo.ResourcePartition {
		t.Error("logicalNodes[0].GeoInfo.Region is : ", logicalNodes[0].GeoInfo.Region)
		t.Error("testCases[0].GeoInfo.Region    is : ", testCases[0].GeoInfo.Region)
		t.Error("logicalNodes[0].GeoInfo.ResourcePartition is : ", logicalNodes[0].GeoInfo.ResourcePartition)
		t.Error("testCases[0].GeoInfo.ResourcePartition    is : ", testCases[0].GeoInfo.ResourcePartition)
	}
}

// Simply Test Persist Node Store Status
//
func TestPersistNodeStoreStatus(t *testing.T) {
	var CRV = make(types.TransitResourceVersionMap, 1)
	testLocation := types.RvLocation{Region: location.Beijing, Partition: location.ResourcePartition1}
	CRV[testLocation] = 1000

	testCase0 := &store.NodeStoreStatus{
		RegionNum:              1000,
		PartitionMaxNum:        1000,
		VirtualNodeNumPerRP:    1000,
		CurrentResourceVerions: CRV,
	}

	success := GR.PersistNodeStoreStatus(testCase0)

	if success == false {
		t.Error("Store Node Store Status of testCase into Redis in failure")
	}

	var nodeStoreStatus = GR.GetNodeStoreStatus()

	if reflect.ValueOf(nodeStoreStatus).IsNil() {
		t.Error("Read Node Store Status from Redis in failure")
	}

	if nodeStoreStatus.RegionNum != testCase0.RegionNum {
		t.Error("nodeStoreStatus.RegionNum is : ", nodeStoreStatus.RegionNum)
		t.Error("testCases0.RegionNum      is : ", testCase0.RegionNum)
	}

	if nodeStoreStatus.PartitionMaxNum != testCase0.PartitionMaxNum {
		t.Error("nodeStoreStatus.PartitionMaxNum is : ", nodeStoreStatus.PartitionMaxNum)
		t.Error("testCases0.PartitionMaxNum      is : ", testCase0.PartitionMaxNum)
	}

	if nodeStoreStatus.VirtualNodeNumPerRP != testCase0.VirtualNodeNumPerRP {
		t.Error("nodeStoreStatus.VirtualNodeNumPerRP is : ", nodeStoreStatus.VirtualNodeNumPerRP)
		t.Error("testCases0.VirtualNodeNumPerRP      is : ", testCase0.VirtualNodeNumPerRP)
	}

	if nodeStoreStatus.CurrentResourceVerions[testLocation] != testCase0.CurrentResourceVerions[testLocation] {
		t.Error("nodeStoreStatus.[testLocation] is : ", testLocation, nodeStoreStatus.CurrentResourceVerions[testLocation])
		t.Error("testCases0.[testLocation]      is : ", testLocation, testCase0.CurrentResourceVerions[testLocation])
	}
}

// Simply Test Persist Virtual Nodes Assignments
//
func TestPersistVirtualNodesAssignments(t *testing.T) {
	vNodeConfigs := make([]*store.VirtualNodeConfig, 1)

	vNodeToSave := &store.VirtualNodeConfig{
		Lowerbound: 1000.00,
		Upperbound: 2000.00,
		Location:   *location.NewLocation(location.Beijing, location.ResourcePartition1),
	}

	vNodeConfigs[0] = vNodeToSave

	testCase0 := &store.VirtualNodeAssignment{
		ClientId:     "1000",
		VirtualNodes: vNodeConfigs,
	}

	success := GR.PersistVirtualNodesAssignments(testCase0)

	if success == false {
		t.Error("Store Node Store Status of testCase into Redis in failure")
	}

	var virtualNodeAssignment = GR.GetVirtualNodesAssignments()

	if reflect.ValueOf(virtualNodeAssignment).IsNil() {
		t.Error("Read Virtual Node Assignment from Redis in failure")
	}

	if virtualNodeAssignment.ClientId != testCase0.ClientId {
		t.Error("virtualNodeAssignment.ClientId is : ", virtualNodeAssignment.ClientId)
		t.Error("testCases0.ClientId      is : ", testCase0.ClientId)
	}

	if virtualNodeAssignment.VirtualNodes[0].Lowerbound != testCase0.VirtualNodes[0].Lowerbound {
		t.Error("virtualNodeAssignment.VirtualNodes[0].Lowerbound is : ", virtualNodeAssignment.VirtualNodes[0].Lowerbound)
		t.Error("testCases0.VirtualNodes[0].Lowerbound      is : ", testCase0.VirtualNodes[0].Lowerbound)
	}

	if virtualNodeAssignment.VirtualNodes[0].Upperbound != testCase0.VirtualNodes[0].Upperbound {
		t.Error("virtualNodeAssignment.VirtualNodes[0].Upperbound is : ", virtualNodeAssignment.VirtualNodes[0].Upperbound)
		t.Error("testCases0.VirtualNodes[0].Upperbound      is : ", testCase0.VirtualNodes[0].Upperbound)
	}

	if float64(virtualNodeAssignment.VirtualNodes[0].Location.GetRegion()) != float64(testCase0.VirtualNodes[0].Location.GetRegion()) {
		t.Error("virtualNodeAssignment.VirtualNodes[0].Location.GetRegion() is : ", virtualNodeAssignment.VirtualNodes[0].Location.GetRegion())
		t.Error("testCases0.VirtualNodes[0].Location.GetRegion()      is : ", testCase0.VirtualNodes[0].Location.GetRegion())
	}

	if float64(virtualNodeAssignment.VirtualNodes[0].Location.GetResourcePartition()) != float64(testCase0.VirtualNodes[0].Location.GetResourcePartition()) {
		t.Error("virtualNodeAssignment.VirtualNodes[0].Location.GetResourcePartition() is : ", virtualNodeAssignment.VirtualNodes[0].Location.GetResourcePartition())
		t.Error("testCases0.VirtualNodes[0].Location.GetResourcePartition()      is : ", testCase0.VirtualNodes[0].Location.GetResourcePartition())
	}
}

// Simply Test to get batch node ids for node status inquiry
//
func TestBatchLogicalNodeInquiry(t *testing.T) {
	// Clean redis store to avoid test mess up due to previous tests
	GR = NewRedisClient("localhost", redisPort, true)

	// Start first test
	t.Log("\nFirst test is starting")
	testCases := make([]*types.LogicalNode, 2)

	var testCase0 = &types.LogicalNode{
		Id:              "0002",
		ResourceVersion: "0003",
		GeoInfo: types.NodeGeoInfo{
			Region:            1001,
			ResourcePartition: 1001,
		},
		Conditions:  255,
		Reserved:    true,
		MachineType: "machineType2",
	}

	testCases[0] = testCase0

	var testCase1 = &types.LogicalNode{
		Id:              "0003",
		ResourceVersion: "0004",
		GeoInfo: types.NodeGeoInfo{
			Region:            1002,
			ResourcePartition: 1002,
		},
		Conditions:  255,
		Reserved:    true,
		MachineType: "machineType3",
	}

	testCases[1] = testCase1

	success := GR.PersistNodes(testCases)

	if success == false {
		t.Error("Store 2 new Logical Nodes of testCases into Redis in failure")
	}

	// Set batchLength = 2
	var batchLength int = 2
	logicalNodes := GRInquiry.BatchLogicalNodesInquiry(batchLength)
	actualLength := len(logicalNodes)

	assert.NotEqual(t, 0, actualLength)
	assert.Equal(t, batchLength, actualLength)
	t.Logf("Scan (%v) logical nodes and actually get (%v) logical nodes", batchLength, actualLength)

	for i := 0; i < actualLength; i++ {
		t.Logf("Index #(%v): ", i)
		t.Log("=================")
		t.Logf("Id:        (%v)", logicalNodes[i].Id)
		t.Logf("Region:    (%v)", logicalNodes[i].GeoInfo.Region)
		t.Logf("RP:        (%v)", logicalNodes[i].GeoInfo.ResourcePartition)
	}
	t.Log("\nFirst test is OK\n")

	// Start second test
	t.Log("\nSecond test is starting")
	testNumber := 10000
	testCases10 := make([]*types.LogicalNode, testNumber)

	startID := 1000001
	startRV := 10001
	var startRegion types.RegionName = 10001
	var startRP types.ResourcePartitionName = 10001

	for i := 0; i < testNumber; i++ {
		testcase := &types.LogicalNode{
			Id:              strconv.Itoa(startID),
			ResourceVersion: strconv.Itoa(startRV),
			GeoInfo: types.NodeGeoInfo{
				Region:            startRegion,
				ResourcePartition: startRP,
			},
			Conditions:  255,
			Reserved:    true,
			MachineType: "machineType" + "strconv.Itoa(i)",
		}

		testCases10[i] = testcase
		startID++
		startRV++
		startRegion++
		startRP++
	}

	success = GR.PersistNodes(testCases10)
	if success == false {
		t.Errorf("Store (%v) new Logical Nodes of testCases into Redis in failure", testNumber)
	}

	// Set different batchLength = 10
	batchLength = 10
	logicalNodes = GRInquiry.BatchLogicalNodesInquiry(batchLength)
	actualLength = len(logicalNodes)

	assert.NotEqual(t, 0, actualLength)

	// https://redis.io/commands/scan/
	// The COUNT option
	// the server will usually return count or a bit more than count elements per call.
	assert.LessOrEqual(t, batchLength, actualLength)
	t.Logf("Scan (%v) logical nodes and actually get (%v) logical nodes", batchLength, actualLength)
	t.Log("\nSecond test is OK\n")

	// Start third test
	t.Log("\nThird test is starting")
	// Set different batchLength = 1000
	batchLength = 1000
	logicalNodes = GRInquiry.BatchLogicalNodesInquiry(batchLength)
	actualLength = len(logicalNodes)

	assert.NotEqual(t, 0, actualLength)

	// https://redis.io/commands/scan/
	// The COUNT option
	// the server will usually return count or a bit more than count elements per call.
	assert.LessOrEqual(t, batchLength, actualLength)
	t.Logf("Scan (%v) logical nodes and actually get (%v) logical nodes", batchLength, actualLength)
	t.Log("\nThird test is OK\n")

	// Start fourth test
	t.Log("\nFourth test is starting")
	// Set different batchLength = 2000
	batchLength = 2000
	logicalNodes = GRInquiry.BatchLogicalNodesInquiry(batchLength)
	actualLength = len(logicalNodes)

	assert.NotEqual(t, 0, actualLength)

	// https://redis.io/commands/scan/
	// The COUNT option
	// the server will usually return count or a bit more than count elements per call.
	assert.LessOrEqual(t, batchLength, actualLength)
	t.Logf("Scan (%v) logical nodes and actually get (%v) logical nodes", batchLength, actualLength)
	t.Log("\nFouth test is OK\n")

	// Start fifth test
	t.Log("\nThe fifth test is starting")
	// Set different batchLength = 20000
	batchLength = 20000
	logicalNodes = GRInquiry.BatchLogicalNodesInquiry(batchLength)
	actualLength = len(logicalNodes)

	assert.NotEqual(t, 0, actualLength)
	assert.NotEqual(t, batchLength, actualLength)
	t.Logf("Scan (%v) logical nodes and actually get (%v) logical nodes", batchLength, actualLength)
	t.Log("\nFifth test is OK\n")
}

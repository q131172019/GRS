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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"k8s.io/klog/v2"

	"global-resource-service/resource-management/pkg/common-lib/interfaces/store"
	"global-resource-service/resource-management/pkg/common-lib/types"

	"github.com/go-redis/redis/v9"
)

type Goredis struct {
	client *redis.Client
	ctx    context.Context
}

// Initialize Redis Client
func NewRedisClient(redisServerIP string, redisPort string, flushAllFlag bool) *Goredis {
	redisAddress := fmt.Sprintf("%s:%s", redisServerIP, redisPort)

	client := redis.NewClient(&redis.Options{
		Addr:         redisAddress,
		PoolSize:     1000,
		PoolTimeout:  2 * time.Minute,
		ReadTimeout:  2 * time.Minute,
		WriteTimeout: 1 * time.Minute,
		Password:     "", //no password set
		DB:           0,  // use default DB
	})

	ctx := context.Background()

	// Batch Logical Nodes Inquiry Client should not flushall database
	if flushAllFlag {
		if err := client.FlushAll(ctx).Err(); err != nil {
			klog.Errorf("Flush all dbs in error : (%v)", err)
			os.Exit(1)
		}
	}

	return &Goredis{
		client: client,
		ctx:    ctx,
	}
}

// To Test Persist Simple String
//
func (gr *Goredis) setString(myKey, myValue string) bool {
	if len(myKey) == 0 || len(myValue) == 0 {
		klog.Errorf("The Key or Value is blank")
		return false
	}

	err := gr.client.Set(gr.ctx, myKey, []byte(myValue), 0).Err()

	if err != nil {
		klog.Errorf("Error to persist String key and Value to Redis Store. error %v", err)
		return false
	}

	return true
}

// To Test Get Simple String
//
func (gr *Goredis) getString(myKey string) string {
	var myValue string

	if len(myKey) == 0 {
		klog.Errorf("The Key is blank")
		return ""
	}

	value, err := gr.client.Get(gr.ctx, myKey).Bytes()

	if err != nil {
		klog.Errorf("Error to get String Key from Redis Store. error %v", err)
		return ""
	}

	if err != redis.Nil {
		myValue = string(value)
	}

	return myValue
}

// Use Redis data type - Set to store Logical Nodes
// One key has one record
//
// Note: Need re-visit these codes to see whether using function pointer is much better
//
// TODO: Error handling for loop persistence failure in the middle
//
func (gr *Goredis) PersistNodes(logicalNodes []*types.LogicalNode) bool {
	if logicalNodes == nil {
		klog.Errorf("The array of Logical Nodes is nil")
		return false
	}

	for _, logicalNode := range logicalNodes {
		logicalNodeKey := logicalNode.GetKey()
		logicalNodeBytes, err := json.Marshal(logicalNode)

		if err != nil {
			klog.Errorf("Error from JSON Marshal for Logical Nodes. error %v", err)
			return false
		}

		err = gr.client.Set(gr.ctx, logicalNodeKey, logicalNodeBytes, 0).Err()

		if err != nil {
			klog.Errorf("Error to persist Logical Nodes to Redis Store. error %v", err)
			return false
		}
	}

	return true
}

// Use Redis data type - String to store Node Store Status
//
func (gr *Goredis) PersistNodeStoreStatus(nodeStoreStatus *store.NodeStoreStatus) bool {
	nodeStoreStatusBytes, err := json.Marshal(nodeStoreStatus)

	if err != nil {
		klog.Errorf("Error from JSON Marshal for Node Store Status. error %v", err)
		return false
	}

	err = gr.client.Set(gr.ctx, nodeStoreStatus.GetKey(), nodeStoreStatusBytes, 0).Err()

	if err != nil {
		klog.Errorf("Error to persist Node Store Status to Redis Store. error %v", err)
		return false
	}

	return true
}

// Use Redis data type - String to store Virtual Node Assignment
//
func (gr *Goredis) PersistVirtualNodesAssignments(virtualNodeAssignment *store.VirtualNodeAssignment) bool {
	virtualNodeAssignmentBytes, err := json.Marshal(virtualNodeAssignment)

	if err != nil {
		klog.Errorf("Error from JSON Marshal for Virtual Node Assignment:", err)
		return false
	}

	err = gr.client.Set(gr.ctx, virtualNodeAssignment.GetKey(), virtualNodeAssignmentBytes, 0).Err()

	if err != nil {
		klog.Errorf("Error to persist Virtual Node Assignment to Redis Store. error %v", err)
		return false
	}

	return true
}

// For batch Logical Nodes inquiry
func (gr *Goredis) BatchLogicalNodesInquiry(batchSize int) []*types.LogicalNode {

	// Retrive all keys of MiniNode based on batch size
	pattern := types.PreserveNode_KeyPrefix + "." + "*"

	logicalNodeKeys, currentCursor := gr.client.Scan(gr.ctx, 0, pattern, int64(batchSize)).Val()

	countLogicalNodeKeys := len(logicalNodeKeys)
	if countLogicalNodeKeys == 0 {
		klog.Errorf("Error to scan (%v) keys from redis store : ", batchSize)
		return nil
	}

	// Check whether the size of scanned logical node keys are less than batch size
	// OR whether SCAN operation is a 'full iteration" when starting cursor is 0 and
	// the returning cursor is 0
	for countLogicalNodeKeys < batchSize {
		klog.V(3).Infof("Scan (%v) keys but get (%v) keys : ", batchSize, countLogicalNodeKeys)

		if currentCursor != 0 {
			extraNeededSize := batchSize - countLogicalNodeKeys
			klog.V(3).Infof("Need scan to get extra (%v) keys : ", extraNeededSize)
			extraLogicalNodeKeys, newCursor := gr.client.Scan(gr.ctx, currentCursor, pattern, int64(extraNeededSize)).Val()
			logicalNodeKeys = append(logicalNodeKeys, extraLogicalNodeKeys...)
			countLogicalNodeKeys += len(extraLogicalNodeKeys)
			currentCursor = newCursor
			klog.V(3).Infof("Current cursor is (%v) : ", newCursor)
		} else {
			klog.V(3).Infof("Scanning (%v) keys is a full iteration in the end: ", countLogicalNodeKeys)
			break
		}
	}

	// 1. Retrive value of each logical node based on each logical node key from Redis store
	// 2. Then unmarshal value to each logical node and store it into array LogicalNodes
	logicalNodes := make([]*types.LogicalNode, len(logicalNodeKeys))

	for i, logicalNodeKey := range logicalNodeKeys {
		value, err := gr.client.Get(gr.ctx, logicalNodeKey).Bytes()

		if err != nil {
			klog.Errorf("Error to get LogicalNode from Redis Store. error %v", err)
			return nil
		}

		var logicalNode *types.LogicalNode
		if err != redis.Nil {
			err = json.Unmarshal(value, &logicalNode)

			if err != nil {
				klog.Errorf("Error from JSON Unmarshal for LogicalNode. error %v", err)
				return nil
			}
		}

		logicalNodes[i] = logicalNode
	}

	return logicalNodes
}

// Get all Logical Nodes based on PreserveNode_KeyPrefix = "MinNode"
//
// Note: Need re-visit these codes to see whether using function pointer is much better
//
func (gr *Goredis) GetNodes() []*types.LogicalNode {
	keys := gr.client.Keys(gr.ctx, types.PreserveNode_KeyPrefix+"*").Val()

	logicalNodes := make([]*types.LogicalNode, len(keys))
	for i, logicalNodeKey := range keys {
		value, err := gr.client.Get(gr.ctx, logicalNodeKey).Bytes()

		if err != nil {
			klog.Errorf("Error to get LogicalNode from Redis Store. error %v", err)
			return nil
		}

		var logicalNode *types.LogicalNode
		if err != redis.Nil {
			err = json.Unmarshal(value, &logicalNode)

			if err != nil {
				klog.Errorf("Error from JSON Unmarshal for LogicalNode. error %v", err)
				return nil
			}

			logicalNodes[i] = logicalNode

		}
	}

	return logicalNodes
}

// Get Node Store Status
//
func (gr *Goredis) GetNodeStoreStatus() *store.NodeStoreStatus {
	var nodeStoreStatus *store.NodeStoreStatus

	value, err := gr.client.Get(gr.ctx, nodeStoreStatus.GetKey()).Bytes()

	if err != nil {
		klog.Errorf("Error to get NodeStoreStatus from Redis Store. error %v", err)
		return nil
	}

	if err != redis.Nil {
		err = json.Unmarshal(value, &nodeStoreStatus)

		if err != nil {
			klog.Errorf("Error from JSON Unmarshal for NodeStoreStatus. error %v", err)
			return nil
		}
	}

	return nodeStoreStatus
}

// Get Virtual Nodes Assignments
//
func (gr *Goredis) GetVirtualNodesAssignments() *store.VirtualNodeAssignment {
	var virtualNodeAssignment *store.VirtualNodeAssignment

	value, err := gr.client.Get(gr.ctx, virtualNodeAssignment.GetKey()).Bytes()

	if err != nil {
		klog.Errorf("Error to get VirtualNodeAssignment from Redis Store. error %v", err)
		return nil
	}

	if err != redis.Nil {
		err = json.Unmarshal(value, &virtualNodeAssignment)

		if err != nil {
			klog.Errorf("Error from JSON Unmarshal for VirtualNodeAssignment. error %v", err)
			return nil
		}
	}

	return virtualNodeAssignment
}

func (gr *Goredis) PersistClient(clientId string, client *types.Client) error {
	ci, err := json.Marshal(client)

	if err != nil {
		klog.Errorf("Error marshalling client. error %v", err)
		return err
	}

	err = gr.client.Set(gr.ctx, clientId, ci, 0).Err()

	if err != nil {
		klog.Errorf("Error persisting client to Redis Store. error %v", err)
		return err
	}

	return nil
}

func (gr *Goredis) GetClient(clientId string) (*types.Client, error) {
	ci := &types.Client{}

	value, err := gr.client.Get(gr.ctx, clientId).Bytes()

	if err != nil {
		klog.Errorf("Error getting client from Redis Store. error %v", err)
		return nil, err
	}

	if err == redis.Nil {
		klog.Errorf("Client %s, is not found in store", clientId)
		return nil, fmt.Errorf("client not found")
	}

	err = json.Unmarshal(value, ci)

	if err != nil {
		klog.Errorf("Error unmarshal client type. error %v", err)
		return nil, err
	}

	return ci, nil
}

func (gr *Goredis) UpdateClient(clientId string, client *types.Client) error {
	return fmt.Errorf("not implemented")
}

func (gr *Goredis) GetClients() ([]*types.Client, error) {
	return nil, fmt.Errorf("not implemented")
}

func (gr *Goredis) InitNodeIdCache() {
	klog.Errorf("InitNodeIdCache not implemented")
}

func (gr *Goredis) GetNodeIdCount() int {
	klog.Errorf("GetNodeIdCount not implemented")
	return -1
}

func (gr *Goredis) SetTestNodeIdMatch(isMatch bool) {
	klog.Errorf("SetTestNodeIdMatch not implemented")
}

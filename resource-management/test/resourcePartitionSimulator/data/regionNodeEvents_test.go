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

package data

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"

	"global-resource-service/resource-management/pkg/common-lib/types"
	"global-resource-service/resource-management/pkg/common-lib/types/location"
	"global-resource-service/resource-management/pkg/common-lib/types/runtime"
	"global-resource-service/resource-management/test/resourceRegionMgrSimulator/config"
)

func TestGetRegionNodeModifiedEventsCRV(t *testing.T) {
	// create nodes
	rpNum := 10
	nodesPerRP := 50000
	start := time.Now()
	Init("Beijing", rpNum, nodesPerRP)
	// 2.827539846s
	// new: 2.920060761s
	duration := time.Since(start)
	assert.Equal(t, rpNum, len(RegionNodeEventsList))
	assert.Equal(t, nodesPerRP, len(RegionNodeEventsList[0]))
	t.Logf("Time to generate %d init events: %v", rpNum*nodesPerRP, duration)

	// List nodes
	start = time.Now()
	nodesEventList, count, rvs := ListNodes()
	duration = time.Since(start)
	assert.Equal(t, uint64(rpNum*nodesPerRP), count)
	assert.NotNil(t, nodesEventList)
	for i := 0; i < rpNum; i++ {
		loc := types.RvLocation{
			Region:    location.Beijing,
			Partition: location.ResourcePartition(i),
		}
		currentRV := rvs[loc]
		assert.Equal(t, uint64(nodesPerRP), currentRV)
	}

	// 500K nodes, list duration 179.605574ms
	// new: duration 231.215888ms
	t.Logf("List %v nodes, duration %v, return RVS %v.", count, duration, rvs)

	// Watch node events
	watchCh := make(chan runtime.Object)
	stopCh := make(chan struct{})
	err := Watch(rvs, watchCh, stopCh)
	if err != nil {
		assert.Fail(t, "Encountered error while building watch connection.", "Encountered error while building watch connection. Error %v", err)
		return
	}
	allWaitGroup := new(sync.WaitGroup)
	allWaitGroup.Add(1)
	updateEventCount := atEachMin10

	runWatch(t, updateEventCount, rvs, watchCh, stopCh, allWaitGroup)

	// generate update node events
	makeDataUpdate(atEachMin10)
	allWaitGroup.Wait()
	time.Sleep(1 * time.Millisecond)
	t.Logf("Watch %d events succeed!\n", updateEventCount)

	// watch from previous resource versions again
	watchCh = make(chan runtime.Object)
	stopCh = make(chan struct{})
	err = Watch(rvs, watchCh, stopCh)
	if err != nil {
		assert.Fail(t, "Encountered error while building watch connection.", "Encountered error while building watch connection. Error %v", err)
		return
	}

	allWaitGroup.Add(1)
	start = time.Now()
	runWatch(t, updateEventCount, rvs, watchCh, stopCh, allWaitGroup)

	allWaitGroup.Wait()
	duration = time.Since(start)
	time.Sleep(1 * time.Millisecond)
	// Duration 27.405µs
	// new Duration 39.651µs
	t.Logf("Re-watch %d events succeed! Duration %v\n", updateEventCount, duration)

	// Test RP down event watches
	watchCh = make(chan runtime.Object)
	stopCh = make(chan struct{})
	err = Watch(rvs, watchCh, stopCh)
	if err != nil {
		assert.Fail(t, "Encountered error while building watch connection.", "Encountered error while building watch connection. Error %v", err)
		return
	}

	allWaitGroup.Add(1)
	runWatch(t, config.NodesPerRP+atEachMin10, rvs, watchCh, stopCh, allWaitGroup)
	makeRPDown(1)
	allWaitGroup.Wait()
	time.Sleep(1 * time.Millisecond)
	t.Logf("RP down test: Watch %d events succeed!\n", config.NodesPerRP+atEachMin10)
}

func runWatch(t *testing.T, expectedEventCount int, rvs types.TransitResourceVersionMap, watchCh chan runtime.Object, stopCh chan struct{}, wg *sync.WaitGroup) {
	go func(t *testing.T, expectedEventCount int, rvs types.TransitResourceVersionMap, watchCh chan runtime.Object, stopCh chan struct{}, wg *sync.WaitGroup) {
		eventCount := 0

		for e := range watchCh {
			assert.Equal(t, runtime.Modified, e.GetEventType())
			loc := types.RvLocation{
				Region:    location.Beijing,
				Partition: location.ResourcePartition(e.GetGeoInfo().ResourcePartition),
			}
			requestedRVForRP := rvs[loc]
			assert.True(t, requestedRVForRP < e.GetResourceVersionInt64())

			eventCount++

			if eventCount >= expectedEventCount {
				wg.Done()
				close(stopCh)
				close(watchCh)
				return
			}
		}
	}(t, expectedEventCount, rvs, watchCh, stopCh, wg)
}

func TestMakeRPDownPerformance(t *testing.T) {
	// create nodes
	rpNum := 40
	nodesPerRP := 25000
	start := time.Now()
	Init("Beijing", rpNum, nodesPerRP)
	// 5.701659636s
	// new: 5.88300605s
	duration := time.Since(start)
	assert.Equal(t, rpNum, len(RegionNodeEventsList))
	assert.Equal(t, nodesPerRP, len(RegionNodeEventsList[0]))
	t.Logf("Time to generate %d init events: %v", rpNum*nodesPerRP, duration)

	// make rp down
	// 1M: 1.583124953s
	// 1M new: 1.67627057s
	for i := 1; i <= rpNum; i++ {
		makeRPDown(i)
	}
}

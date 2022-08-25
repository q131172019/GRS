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
	"math/rand"
	"strconv"
	"time"

	"github.com/google/uuid"
	"k8s.io/klog/v2"

	"global-resource-service/resource-management/pkg/common-lib/types"
	"global-resource-service/resource-management/pkg/common-lib/types/event"
	"global-resource-service/resource-management/pkg/common-lib/types/location"

	simulatorTypes "global-resource-service/resource-management/test/resourceRegionMgrSimulator/types"
)

// The following varables are used to create Region Node Event List in multiply RPs of one region
//
// RegionNodeEventsList      - for initpull
// SnapshotNodeListEvents    - for subsequent pull and
//                                 post CRV to discard all old node events
var RegionNodeList simulatorTypes.RegionNodes
var RegionNodeEventsList simulatorTypes.RegionNodeEvents
var RegionNodeUpdateEventList []*[]*event.NodeEvent
var CurrentRVs types.TransitResourceVersionMap

var RegionId, RpNum, NodesPerRP int

// The constants are for repeatly generate new modified events
// Outage pattern - one RP down

// Daily Patttern - 10 modified changes per minute
const atEachMin10 = 10

// Initialize two events list
// RegionNodeEventsList - for initpull
//
func Init(regionName string, rpNum, nodesPerRP int) {
	RegionNodeEventsList, RegionNodeList, CurrentRVs = generateAddedNodeEvents(regionName, rpNum, nodesPerRP)
	RegionId = int(location.GetRegionFromRegionName(regionName))
	RpNum = rpNum
	NodesPerRP = nodesPerRP

	RegionNodeUpdateEventList = make([]*[]*event.NodeEvent, rpNum)
}

// Generate region node update event changes to
// add them into RegionNodeEventsList
//
func MakeDataUpdate(data_pattern string, wait_time_for_make_rp_down int) {
	go func(data_pattern string, wait_time_for_make_rp_down int) {
		switch data_pattern {
		case "Outage":
			for {
				// Generate one RP down event during specfied interval
				time.Sleep(time.Duration(wait_time_for_make_rp_down) * time.Minute)
				makeOneRPDown()
				klog.V(3).Info("Generating one RP down event is completed")

				time.Sleep(120 * time.Minute)
				klog.V(6).Info("Simulate to delay 2 hours")
			}
		case "Daily":
			for {
				// At each minute mark, generate 10 modified node events
				time.Sleep(1 * time.Minute)
				makeDataUpdate(atEachMin10)

				klog.V(3).Info("At each minute mark, generating 10 modified and added node events is completed")
			}
		default:
			klog.V(3).Infof("Current Simulator Data Pattern (%v) is supported", data_pattern)
			return
		}
	}(data_pattern, wait_time_for_make_rp_down)
}

///////////////////////////////////////////////
// The following functions are for handler.
//////////////////////////////////////////////

// Return region node added events in BATCH LENGTH from all RPs
// TO DO: paginate support
//
func GetRegionNodeAddedEvents(batchLength uint64) (simulatorTypes.RegionNodeEvents, uint64) {
	klog.V(6).Infof("Total (%v) Added events are to be pulled", RpNum*NodesPerRP)
	return RegionNodeEventsList, uint64(RpNum * NodesPerRP)

}

// Return region node modified events with CRVs in BATCH LENGTH from all RPs
// TO DO: paginate support
//
func GetRegionNodeModifiedEventsCRV(rvs types.TransitResourceVersionMap) (simulatorTypes.RegionNodeEvents, uint64) {
	pulledNodeListEvents := make(simulatorTypes.RegionNodeEvents, RpNum)
	region := RegionId

	var count uint64 = 0
	for j := 0; j < RpNum; j++ {
		loc := types.RvLocation{Region: location.Region(region), Partition: location.ResourcePartition(j)}
		requestedRV := rvs[loc]

		eventsForRP := RegionNodeUpdateEventList[j]
		if eventsForRP == nil {
			continue
		}

		pulledNodeListEventsPerRP := make([]*event.NodeEvent, 0)
		indexPerRP := 0
		for i := 0; i < len(*eventsForRP); i++ {
			nodeRV := (*eventsForRP)[i].Node.GetResourceVersionInt64()
			if nodeRV > requestedRV {
				count += 1
				pulledNodeListEventsPerRP = append(pulledNodeListEventsPerRP, (*eventsForRP)[i])
				indexPerRP += 1
			}
		}

		pulledNodeListEvents[j] = pulledNodeListEventsPerRP

		// clean up event cache
		RegionNodeUpdateEventList[j] = nil
	}

	klog.V(9).Infof("Total (%v) Modified events are to be pulled", count)
	return pulledNodeListEvents, count
}

////////////////////////////////////////
// The below are all helper functions
////////////////////////////////////////

// This function is used to initialize the region node added event
//
func generateAddedNodeEvents(regionName string, rpNum, nodesPerRP int) (simulatorTypes.RegionNodeEvents, simulatorTypes.RegionNodes, types.TransitResourceVersionMap) {
	regionId := location.GetRegionFromRegionName(regionName)
	eventsAdd := make(simulatorTypes.RegionNodeEvents, rpNum)
	nodesAdd := make(simulatorTypes.RegionNodes, rpNum)
	cvs := make(types.TransitResourceVersionMap)

	for j := 0; j < rpNum; j++ {
		rpName := location.ResourcePartitions[j]
		loc := location.NewLocation(regionId, rpName)
		rvLoc := types.RvLocation{
			Region:    regionId,
			Partition: rpName,
		}

		// Initialize the resource version starting from 0 for each RP
		var rvToGenerateRPs = 0
		eventsAdd[j] = make([]*event.NodeEvent, nodesPerRP)
		nodesAdd[j] = make([]*types.LogicalNode, nodesPerRP)
		for i := 0; i < nodesPerRP; i++ {
			rvToGenerateRPs += 1

			nodesAdd[j][i] = createRandomNode(rvToGenerateRPs, loc)
			nodeEvent := event.NewNodeEvent(nodesAdd[j][i], event.Added)
			eventsAdd[j][i] = nodeEvent
		}

		cvs[rvLoc] = uint64(rvToGenerateRPs)
	}
	return eventsAdd, nodesAdd, cvs
}

//This function simulates one random RP down
func makeOneRPDown() {
	selectedRP := int(rand.Intn(RpNum))
	klog.V(3).Infof("Generating all node down events in selected RP (%v) is starting", selectedRP)

	// Get the highestRVForRP of selectRP
	rvLoc := types.RvLocation{
		Region:    location.Region(RegionId),
		Partition: location.ResourcePartition(selectedRP),
	}
	highestRVForRP := CurrentRVs[rvLoc]

	// Make the modified changes for all nodes of selected RP
	rvToGenerateRPs := highestRVForRP + 1
	nodeChangeEvents := make([]*event.NodeEvent, 0)
	for i := 0; i < NodesPerRP; i++ {

		// Update the version of node with the current rvToGenerateRPs
		node := RegionNodeList[selectedRP][i]
		node.ResourceVersion = strconv.FormatUint(rvToGenerateRPs, 10)

		// record the time to change resource version in resource partition
		node.LastUpdatedTime = time.Now().UTC()

		newEvent := event.NewNodeEvent(node, event.Modified)
		nodeChangeEvents = append(nodeChangeEvents, newEvent)

		rvToGenerateRPs++
	}

	// Record the highest RV for selected RP
	if NodesPerRP > 0 {
		CurrentRVs[rvLoc] = rvToGenerateRPs - 1
	}

	// Update node change events for selected RP into Region Node Update Event List
	RegionNodeUpdateEventList[selectedRP] = &nodeChangeEvents
}

// This function is used to create region node modified event by go routine
//
func makeDataUpdate(changesThreshold int) {
	// Calculate how many node changes per RP
	var nodeChangesPerRP = 1
	if changesThreshold >= 2*RpNum {
		nodeChangesPerRP = changesThreshold / RpNum
	}

	// Make data update for each RP
	for j := 0; j < RpNum; j++ {
		// get the highestRV
		rvLoc := types.RvLocation{
			Region:    location.Region(RegionId),
			Partition: location.ResourcePartition(j),
		}
		highestRVForRP := CurrentRVs[rvLoc]

		// Pick up 'nodeChangesPerRP' nodes and make changes and assign the node RV to highestRV + 1
		count := 0
		rvToGenerateRPs := highestRVForRP + 1
		nodeChangeEvents := make([]*event.NodeEvent, 0)
		if RegionNodeUpdateEventList[j] != nil && len(*RegionNodeUpdateEventList[j]) > 0 {
			nodeChangeEvents = *RegionNodeUpdateEventList[j]
		}
		for count < nodeChangesPerRP {
			// Randonly create data update per RP node events list
			i := rand.Intn(NodesPerRP)
			node := RegionNodeList[j][i]

			// special case: Consider 5000 changes per RP for 500 nodes per RP
			// each node has 10 changes within this cycle
			node.ResourceVersion = strconv.FormatUint(rvToGenerateRPs, 10)
			// record the time to change resource version in resource partition
			node.LastUpdatedTime = time.Now().UTC()

			newEvent := event.NewNodeEvent(node, event.Modified)
			nodeChangeEvents = append(nodeChangeEvents, newEvent)

			count++
			rvToGenerateRPs++
		}
		if nodeChangesPerRP > 0 {
			CurrentRVs[rvLoc] = rvToGenerateRPs - 1
		}
		RegionNodeUpdateEventList[j] = &nodeChangeEvents
	}

	klog.V(6).Infof("Actually total (%v) new modified events are created.", changesThreshold)
}

// Create logical node with random UUID
//
func createRandomNode(rv int, loc *location.Location) *types.LogicalNode {
	id := uuid.New()

	return &types.LogicalNode{
		Id:              id.String(),
		ResourceVersion: strconv.Itoa(rv),
		GeoInfo: types.NodeGeoInfo{
			Region:            types.RegionName(loc.GetRegion()),
			ResourcePartition: types.ResourcePartitionName(loc.GetResourcePartition()),
			DataCenter:        types.DataCenterName(strconv.Itoa(int(rand.Intn(10000))) + "-DataCenter"),
			AvailabilityZone:  types.AvailabilityZoneName("AZ-" + strconv.Itoa(int(rand.Intn(6)))),
			FaultDomain:       types.FaultDomainName(id.String() + "-FaultDomain"),
		},
		Taints: types.NodeTaints{
			NoSchedule: false,
			NoExecute:  false,
		},
		SpecialHardwareTypes: types.NodeSpecialHardWareTypeInfo{
			HasGpu:  true,
			HasFPGA: true,
		},
		AllocatableResource: types.NodeResource{
			MilliCPU:         int64(rand.Intn(200) + 20),
			Memory:           int64(rand.Intn(2000)),
			EphemeralStorage: int64(rand.Intn(2000000)),
			AllowedPodNumber: int(rand.Intn(20000000)),
			ScalarResources: map[types.ResourceName]int64{
				"GPU":  int64(rand.Intn(200)),
				"FPGA": int64(rand.Intn(200)),
			},
		},
		Conditions:      111,
		Reserved:        false,
		MachineType:     types.NodeMachineType(id.String() + "-highend"),
		LastUpdatedTime: time.Now().UTC(),
	}
}

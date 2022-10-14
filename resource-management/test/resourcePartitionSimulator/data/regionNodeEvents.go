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
	"errors"
	"math/rand"
	"strconv"
	"time"

	//arktosCoreV1 "github.com/CentaurusInfra/arktos/pkg/apis/core"

	"github.com/google/uuid"
	k8sCoreV1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sTypes "k8s.io/apimachinery/pkg/types"

	//k8sCoreV1 "github.com/kubernetes/api/core/v1"
	"k8s.io/klog/v2"

	"global-resource-service/resource-management/pkg/common-lib/types"
	"global-resource-service/resource-management/pkg/common-lib/types/cache"
	"global-resource-service/resource-management/pkg/common-lib/types/location"
	"global-resource-service/resource-management/pkg/common-lib/types/runtime"
	"global-resource-service/resource-management/test/resourceRegionMgrSimulator/config"
	simulatorTypes "global-resource-service/resource-management/test/resourceRegionMgrSimulator/types"
)

// The following varables are used to create Region Node Event List in multiply RPs of one region
//
// RegionNodeEventsList      - for initpull
// SnapshotNodeListEvents    - for subsequent pull and
//                                 post CRV to discard all old node events
var RegionNodeEventsList simulatorTypes.RegionNodeEvents
var RegionNodeEventQueue *cache.EventQueuesByLocation
var CurrentRVs types.TransitResourceVersionMap

// The constants are for repeatly generate new modified events
// Outage pattern - one RP down

// Daily Patttern - 10 modified changes per minute
const atEachMin10 = 10

// Initialize two events list
// RegionNodeEventsList - for initpull
//
func Init(regionName string, rpNum, nodesPerRP int) {
	config.RegionId = int(location.GetRegionFromRegionName(regionName))
	config.RpNum = rpNum
	config.NodesPerRP = nodesPerRP

	RegionNodeEventQueue = cache.NewEventQueuesByLocation()
	RegionNodeEventsList, CurrentRVs = generateAddedNodeEvents(regionName, rpNum, nodesPerRP)
}

// Generate region node update event changes to
// add them into RegionNodeEventsList
//
func MakeDataUpdate(data_pattern string, wait_time_for_data_change_pattern int, rpDownNumber int) {
	go func(data_pattern string, wait_time_for_data_change_pattern int, rpDownNumber int) {
		switch data_pattern {
		case "Outage":
			for {
				// Generate one RP down event during specfied interval
				time.Sleep(time.Duration(wait_time_for_data_change_pattern) * time.Minute)
				makeRPDown(rpDownNumber)
				klog.V(3).Info("Generating one RP down event is completed")

				time.Sleep(120 * time.Minute)
				klog.V(6).Info("Simulate to delay 2 hours")
			}
		case "Daily":
			//Sleep time ensures schedulers complete 25K-node list before modified events are created
			time.Sleep(time.Duration(wait_time_for_data_change_pattern) * time.Minute)

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
	}(data_pattern, wait_time_for_data_change_pattern, rpDownNumber)
}

///////////////////////////////////////////////
// The following functions are for handler.
//////////////////////////////////////////////

// Return region node added events in BATCH LENGTH from all RPs
// TO DO: paginate support
//
func ListNodes() (simulatorTypes.RegionNodeEvents, uint64, types.TransitResourceVersionMap) {
	klog.V(6).Infof("Total (%v) Added events are to be pulled", config.RpNum*config.NodesPerRP)

	nodeEventsByRP := make(simulatorTypes.RegionNodeEvents, config.RpNum)
	for i := 0; i < config.RpNum; i++ {
		nodeEventsByRP[i] = make([]*runtime.NodeEvent, config.NodesPerRP)
	}

	RegionNodeEventQueue.AcquireSnapshotRLock()
	for i := 0; i < config.RpNum; i++ {
		for j := 0; j < config.NodesPerRP; j++ {
			node := RegionNodeEventsList[i][j].Node.Copy()
			nodeEventsByRP[i][j] = runtime.NewNodeEvent(node, runtime.Added)
		}
	}

	currentRVs := CurrentRVs.Copy()
	RegionNodeEventQueue.ReleaseSnapshotRLock()

	return nodeEventsByRP, uint64(config.RpNum * config.NodesPerRP), currentRVs

}

// Return region node modified events with CRVs in BATCH LENGTH from all RPs
// TO DO: paginate support
//
func Watch(rvs types.TransitResourceVersionMap, watchChan chan runtime.Object, stopCh chan struct{}) error {
	if rvs == nil {
		return errors.New("Invalid resource versions: nil")
	}
	if watchChan == nil {
		return errors.New("Watch channel not provided")
	}
	if stopCh == nil {
		return errors.New("Stop watch channel not provided")
	}

	internal_rvs := types.ConvertToInternalResourceVersionMap(rvs)
	return RegionNodeEventQueue.Watch(internal_rvs, watchChan, stopCh)
}

////////////////////////////////////////
// The below are all helper functions
////////////////////////////////////////

// This function is used to initialize the region node added event
//
func generateAddedNodeEvents(regionName string, rpNum, nodesPerRP int) (simulatorTypes.RegionNodeEvents, types.TransitResourceVersionMap) {
	regionId := location.GetRegionFromRegionName(regionName)
	eventsAdd := make(simulatorTypes.RegionNodeEvents, rpNum)
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
		eventsAdd[j] = make([]*runtime.NodeEvent, nodesPerRP)
		for i := 0; i < nodesPerRP; i++ {
			rvToGenerateRPs += 1

			nodeToAdd := createRandomNode(rvToGenerateRPs, loc)
			nodeEvent := runtime.NewNodeEvent(nodeToAdd, runtime.Added)
			eventsAdd[j][i] = nodeEvent

			// node event enqueue
			RegionNodeEventQueue.EnqueueEvent(nodeEvent)
		}

		cvs[rvLoc] = uint64(rvToGenerateRPs)
	}
	return eventsAdd, cvs
}

//This function simulates specified number of RP down
func makeRPDown(rpDownNumber int) {
	klog.V(3).Infof("Generating all node down events in %v RPs is starting", rpDownNumber)

	// Make the modified changes for all nodes of selected RP
	start := time.Now()
	klog.Infof("[Throughput] Start to generating %v RP down event", config.NodesPerRP*rpDownNumber)
	for j := 0; j < rpDownNumber; j++ {
		// Get the highestRVForRP of selectRP
		rvLoc := types.RvLocation{
			Region:    location.Region(config.RegionId),
			Partition: location.ResourcePartition(j),
		}
		highestRVForRP := CurrentRVs[rvLoc]
		rvToGenerateRPs := highestRVForRP + 1

		for i := 0; i < config.NodesPerRP; i++ {
			// Update the version of node with the current rvToGenerateRPs
			node := RegionNodeEventsList[j][i].Node
			node.ResourceVersion = strconv.FormatUint(rvToGenerateRPs, 10)

			// record the time to change resource version in resource partition
			node.LastUpdatedTime = time.Now().UTC()

			newEvent := runtime.NewNodeEvent(node, runtime.Modified)

			//RegionNodeEventsList[selectedRP][i] = no need: keep event as added, node will be updated as pointer
			RegionNodeEventQueue.EnqueueEvent(newEvent)

			rvToGenerateRPs++
		}
		// Record the highest RV for selected RP
		if config.NodesPerRP > 0 {
			CurrentRVs[rvLoc] = rvToGenerateRPs - 1
		}
	}
	duration := time.Since(start)
	klog.Infof("[Throughput] Time to generate %v events: %v", config.NodesPerRP*rpDownNumber, duration)
}

// This function is used to create region node modified event by go routine
//
func makeDataUpdate(changesThreshold int) {
	// Calculate how many node changes per RP
	var nodeChangesPerRP = 1
	if changesThreshold >= 2*config.RpNum {
		nodeChangesPerRP = changesThreshold / config.RpNum
	}

	// Make data update for each RP
	for j := 0; j < config.RpNum; j++ {
		// get the highestRV
		rvLoc := types.RvLocation{
			Region:    location.Region(config.RegionId),
			Partition: location.ResourcePartition(j),
		}
		highestRVForRP := CurrentRVs[rvLoc]

		// Pick up 'nodeChangesPerRP' nodes and make changes and assign the node RV to highestRV + 1
		count := 0
		rvToGenerateRPs := highestRVForRP + 1
		for count < nodeChangesPerRP {
			// Randonly create data update per RP node events list
			i := rand.Intn(config.NodesPerRP)
			node := RegionNodeEventsList[j][i].Node

			// special case: Consider 5000 changes per RP for 500 nodes per RP
			// each node has 10 changes within this cycle
			node.ResourceVersion = strconv.FormatUint(rvToGenerateRPs, 10)
			// record the time to change resource version in resource partition
			node.LastUpdatedTime = time.Now().UTC()

			newEvent := runtime.NewNodeEvent(node, runtime.Modified)
			//RegionNodeEventsList[j][i] = newEvent - no need: keep event as added, node will be updated as pointer
			RegionNodeEventQueue.EnqueueEvent(newEvent)

			count++
			rvToGenerateRPs++
		}
		if nodeChangesPerRP > 0 {
			CurrentRVs[rvLoc] = rvToGenerateRPs - 1
		}
	}

	klog.V(6).Infof("Actually total (%v) new modified events are created.", changesThreshold)
}

func createRandomRPNode(rv int) *k8sCoreV1.Node {
	id := uuid.New()
	var deleteGracePeriodSeconds int64 = 10

	return &k8sCoreV1.Node{
		TypeMeta: metav1.TypeMeta{Kind: "Node", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			Name:                       "testRPNodeName",
			GenerateName:               "testGeneratedName",
			Namespace:                  "default",
			UID:                        k8sTypes.UID(id.String()),
			ResourceVersion:            strconv.Itoa(rv),
			Generation:                 0000001,
			CreationTimestamp:          metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
			DeletionTimestamp:          &metav1.Time{},
			DeletionGracePeriodSeconds: &deleteGracePeriodSeconds,
			Labels: map[string]string{
				"beta.kubernetes.io/arch":         "amd64",
				"beta.kubernets.io/os":            "linux",
				"kubernetes.io/arch":              "amd64",
				"kubernets.io/os":                 "linux",
				"beta.amd.com/gpu.cu-count.64":    "1",
				"beta.amd.com/gpu.device-id.6860": "1",
				"beta.amd.com/gpu.family.AI":      "1",
				"beta.amd.com/gpu.simd-count.256": "1",
				"beta.amd.com/gpu.vram.16G":       "1",
			},
			Annotations: map[string]string{
				"flannel.alpha.coreos.com/backend-data":                  "null",
				"flannel.alpha.coreos.com/backend-type":                  "host-gw",
				"flannel.alpha.coreos.com/kube-subnet-manager":           "true",
				"flannel.alpha.coreos.com/public-ip":                     "",
				"node.alpha.kubernetes.io/ttl":                           "0",
				"volumes.kubernetes.io/controller-managed-attach-detach": "true",
				"fpga.intel.com/region-ce48969398f05f33946d560708be108a": "1",
				"fpga.intel.com/af-ce48969398f05f33946d560708be108a":     "1",
			},
			OwnerReferences: []metav1.OwnerReference{},
			Finalizers:      []string{},
			ManagedFields:   []metav1.ManagedFieldsEntry{},
		},
		Spec: k8sCoreV1.NodeSpec{
			PodCIDR:       "10.244.1.0/24",
			PodCIDRs:      []string{"10.245.0.0", "10.246.0.0"},
			ProviderID:    "AWS",
			Unschedulable: false,
			Taints: []k8sCoreV1.Taint{
				{
					Key:    "foo_1",
					Value:  "bar_1",
					Effect: k8sCoreV1.TaintEffectNoExecute,
				},
				{
					Key:    "foo_2",
					Value:  "bar_2",
					Effect: k8sCoreV1.TaintEffectNoSchedule,
				},
			},
		},
		Status: k8sCoreV1.NodeStatus{
			Capacity: k8sCoreV1.ResourceList{
				k8sCoreV1.ResourceCPU:              *resource.NewQuantity(8, resource.DecimalSI),
				k8sCoreV1.ResourceMemory:           *resource.NewQuantity(32886512000, resource.BinarySI),
				k8sCoreV1.ResourceEphemeralStorage: *resource.NewQuantity(203234260000, resource.DecimalSI),
				k8sCoreV1.ResourcePods:             *resource.NewQuantity(110, resource.DecimalSI),
				"hugepages-1Gi":                    *resource.NewQuantity(0, resource.DecimalSI),
				"hugepages-2Mi":                    *resource.NewQuantity(0, resource.DecimalSI),
			},
			Allocatable: k8sCoreV1.ResourceList{
				k8sCoreV1.ResourceCPU:              *resource.NewQuantity(8, resource.DecimalSI),
				k8sCoreV1.ResourceMemory:           *resource.NewQuantity(32784112000, resource.BinarySI),
				k8sCoreV1.ResourceEphemeralStorage: *resource.NewQuantity(187300693706, resource.DecimalSI),
				k8sCoreV1.ResourcePods:             *resource.NewQuantity(110, resource.DecimalSI),
				"hugepages-1Gi":                    *resource.NewQuantity(0, resource.DecimalSI),
				"hugepages-2Mi":                    *resource.NewQuantity(0, resource.DecimalSI),
			},
			Phase: k8sCoreV1.NodePhase("Running"),
			Conditions: []k8sCoreV1.NodeCondition{
				{
					Type:               k8sCoreV1.NodeReady,
					Status:             k8sCoreV1.ConditionTrue,
					LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
					LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
					Reason:             "Node initialization",
					Message:            "N/A",
				},
			},
			Addresses: []k8sCoreV1.NodeAddress{
				{Type: k8sCoreV1.NodeInternalIP, Address: "10.1.1.1"},
				{Type: k8sCoreV1.NodeHostName, Address: "ip-10-1-1-1"},
				{Type: k8sCoreV1.NodeExternalIP, Address: "172.31.29.128"},
			},
			DaemonEndpoints: k8sCoreV1.NodeDaemonEndpoints{
				KubeletEndpoint: k8sCoreV1.DaemonEndpoint{
					Port: 9000,
				},
			},
			NodeInfo: k8sCoreV1.NodeSystemInfo{
				MachineID:               "5c43df28985a4cec815b53269ac8c12e",
				SystemUUID:              id.String(),
				BootID:                  id.String(),
				KernelVersion:           "5.4.0-1059-aws",
				OSImage:                 "Ubuntu 20.04.6 LTS",
				OperatingSystem:         "linux",
				Architecture:            "amd64",
				ContainerRuntimeVersion: "containerd://1.5.5",
				KubeletVersion:          "v1.0.0",
				KubeProxyVersion:        "v0.9.0",
			},
			Images: []k8sCoreV1.ContainerImage{
				{
					Names:     []string{"gcr.io/100:v1"},
					SizeBytes: int64(10000000),
				},
				{
					Names:     []string{"gcr.io/200:v1"},
					SizeBytes: int64(20000000),
				},
			},
			VolumesInUse: []k8sCoreV1.UniqueVolumeName{
				"fake/fake-device1",
				"fake/fake-device2",
			},
			VolumesAttached: []k8sCoreV1.AttachedVolume{
				{
					Name:       "fake/fake-device1",
					DevicePath: "fake/fake-path1",
				},
				{
					Name:       "fake/fake-device2",
					DevicePath: "fake/fake-path2",
				},
			},
			Config: &k8sCoreV1.NodeConfigStatus{
				Assigned: &k8sCoreV1.NodeConfigSource{
					ConfigMap: &k8sCoreV1.ConfigMapNodeConfigSource{
						Namespace:        "default",
						Name:             "bar",
						KubeletConfigKey: "kubelet",
						UID:              k8sTypes.UID(id.String()),
						ResourceVersion:  "1000000",
					},
				},
				Active: &k8sCoreV1.NodeConfigSource{
					ConfigMap: &k8sCoreV1.ConfigMapNodeConfigSource{
						Namespace:        "default",
						Name:             "bar",
						KubeletConfigKey: "kubelet",
						UID:              k8sTypes.UID(id.String()),
						ResourceVersion:  "1000000",
					},
				},
				LastKnownGood: &k8sCoreV1.NodeConfigSource{
					ConfigMap: &k8sCoreV1.ConfigMapNodeConfigSource{
						Namespace:        "default",
						Name:             "bar",
						KubeletConfigKey: "kubelet",
						UID:              k8sTypes.UID(id.String()),
						ResourceVersion:  "1000000",
					},
				},
				Error: "No error",
			},
		},
	}
}

//func createRandomRPNodebyArktos(rv int) *arktosCoreV1.Node {
//
//	return &arktosCoreV1.Node{}
//}

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

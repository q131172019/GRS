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

package aggregrator

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"k8s.io/klog/v2"

	distributor "global-resource-service/resource-management/pkg/common-lib/interfaces/distributor"
	"global-resource-service/resource-management/pkg/common-lib/metrics"
	"global-resource-service/resource-management/pkg/common-lib/types"
	"global-resource-service/resource-management/pkg/common-lib/types/event"
)

type Aggregator struct {
	urls           []string
	EventProcessor distributor.Interface
}

// To be client of Resource Region Manager
// RRM: Resource Region Manager
//
type ClientOfRRM struct {
	BaseURL    string
	HTTPClient *http.Client
}

// RRM: Resource Region Manager
//
type ResponseFromRRM struct {
	RegionNodeEvents [][]*event.NodeEvent
	RvMap            types.TransitResourceVersionMap
	Length           uint64
}

// RRM: Resource Region Manager
//
type PullDataFromRRM struct {
	BatchLength uint64
	DefaultCRV  uint64
	CRV         types.TransitResourceVersionMap
}

const (
	DefaultBatchLength = 20000
	httpPrefix         = "http://"
	defaultPullInterval = 10 * time.Millisecond // 10ms as default pull interval
)

// Initialize aggregator
//
func NewAggregator(urls []string, EventProcessor distributor.Interface) *Aggregator {
	return &Aggregator{
		urls:           urls,
		EventProcessor: EventProcessor,
	}
}

// Main loop to get resources from resource region managers and send to distributor
// This is only initial code structure for aggregator method
// TODO:
//    Based on the speed of process events from resource distributor, dynamic decision of batch length
//    will be made by aggregator and this batch length will be used to pull resources from resource region manager
// TODO: error handling
func (a *Aggregator) Run() (err error) {
	numberOfURLs := len(a.urls)

	klog.V(3).Infof("Running for loop to connect to to resource region manager...")

	for i := 0; i < numberOfURLs; i++ {
		go func(i int) {
			klog.V(3).Infof("Starting goroutine for region: %v", a.urls[i])
			defer func() {
				klog.V(3).Infof("Existing goroutine for region: %v", a.urls[i])
			}()

			var crv types.TransitResourceVersionMap
			var regionNodeEvents [][]*event.NodeEvent
			var length uint64
			var eventProcess bool

			// Connect to resource region manager
			c := a.createClient(a.urls[i])

			klog.V(3).Infof("Starting loop pulling nodes from region: %v", a.urls[i])

			for {
				// Call the Pull methods
				// when composite RV is nil, the method initPull is called;
				// otherwise the method subsequentPull is called.
				// To simplify the codes, we use one method initPullOrSubsequentPull instead
				startPullAndProcess := time.Now()
				regionNodeEvents, length = a.initPullOrSubsequentPull(c, DefaultBatchLength, crv)
				if length != 0 {
					klog.V(4).Infof("Total (%v) region node events are pulled successfully in (%v) RPs", length, len(regionNodeEvents))

					// Convert 2D array to 1D array
					minRecordNodeEvents := make([]*event.NodeEvent, 0, length)
					for j := 0; j < len(regionNodeEvents); j++ {
						minRecordNodeEvents = append(minRecordNodeEvents, regionNodeEvents[j]...)
					}
					klog.V(6).Infof("Total (%v) mini node events are converted successfully with length (%v)", len(minRecordNodeEvents), length)

					// Call ProcessEvents() and get the CRV from distributor as default success
					// TODO:
					//    1. Call the ProcessEvents Per RP to unload some cost from the Distributor
					//       The performance tested in development Mac is not good
					//    2. Unfortunately we cannot process the events in separated thread since the returned CRV is needed for the next PULL
					//       so the true pull interval is 100ms + time of ProcessEvent() + time of PULL() + time of converting arrays + logging
					//       TODO: re-evaluate the pull mode vs push for performance
					start := time.Now()
					eventProcess, crv = a.EventProcessor.ProcessEvents(minRecordNodeEvents)
					end := time.Now()
					klog.V(6).Infof("Event Processor Processed nodes results : %v. duration: %v", eventProcess, end.Sub(start))

					if eventProcess {
						a.postCRV(c, crv)
					}
				}

				endPullAndProcess := time.Now()

				completeDuration := endPullAndProcess.Sub(startPullAndProcess)
				if defaultPullInterval > completeDuration {
					time.Sleep(defaultPullInterval - completeDuration)
				}
			}
		}(i)
	}

	klog.V(3).Infof("Finished for loop to connect to to resource region manager...")
	return nil
}

// Connect to resource region manager
//
func (a *Aggregator) createClient(url string) *ClientOfRRM {
	return &ClientOfRRM{
		BaseURL: url,
		HTTPClient: &http.Client{
			Timeout: time.Minute * 3600,
		},
	}
}

// Call resource region manager's InitPull method {url}/resources/initpull when crv is nil
// or
// Call the resource region manager's SubsequentPull method {url}/resources/subsequentpull when crv is not nil
//
func (a *Aggregator) initPullOrSubsequentPull(c *ClientOfRRM, batchLength uint64, crv types.TransitResourceVersionMap) ([][]*event.NodeEvent, uint64) {
	var path string

	if len(crv) == 0 {
		path = httpPrefix + c.BaseURL + "/resources/initpull"
	} else {
		path = httpPrefix + c.BaseURL + "/resources/subsequentpull"
	}

	bytes, _ := json.Marshal(PullDataFromRRM{BatchLength: batchLength, CRV: crv.Copy()})
	req, err := http.NewRequest(http.MethodGet, path, strings.NewReader((string(bytes))))
	if err != nil {
		klog.Errorf(err.Error())
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		klog.Errorf(err.Error())
		return nil, 0
	}
	defer resp.Body.Close()

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		klog.Errorf(err.Error())
		return nil, 0
	}

	var ResponseObject ResponseFromRRM
	err = json.Unmarshal(bodyBytes, &ResponseObject)
	if err != nil {
		klog.Errorf("Error from JSON Unmarshal:", err)
		return nil, 0
	}

	// log out node ids for debugging some prolonged node transitions
	if klog.V(9).Enabled() {
		for rp, rpNodes := range ResponseObject.RegionNodeEvents {
			if len(rpNodes) == 0 {
				continue
			}
			buf := make([]string, len(rpNodes))
			for i, node := range rpNodes {
				buf[i] = node.Node.Id
			}

			klog.V(9).Infof("Pulled nodes from RP %v: %v", rp, buf)
		}
	}

	if metrics.ResourceManagementMeasurement_Enabled {
		for i := 0; i < len(ResponseObject.RegionNodeEvents); i++ {
			for j := 0; j < len(ResponseObject.RegionNodeEvents[i]); j++ {
				if ResponseObject.RegionNodeEvents[i][j] != nil {
					ResponseObject.RegionNodeEvents[i][j].SetCheckpoint(metrics.Aggregator_Received)
				}
			}
		}
	}

	return ResponseObject.RegionNodeEvents, ResponseObject.Length
}

// Call resource region manager's POST method {url}/resources/crv to update the CRV
// error indicate failed POST, CRV means Composite Resource Version
//
func (a *Aggregator) postCRV(c *ClientOfRRM, crv types.TransitResourceVersionMap) error {
	path := httpPrefix + c.BaseURL + "/resources/crv"
	bytes, _ := json.Marshal(PullDataFromRRM{CRV: crv.Copy()})
	req, err := http.NewRequest(http.MethodPost, path, strings.NewReader((string(bytes))))

	if err != nil {
		klog.Errorf(err.Error())
	}

	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")
	_, err = c.HTTPClient.Do(req)

	return err
}

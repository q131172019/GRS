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

package app

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"k8s.io/klog/v2"

	"global-resource-service/resource-management/pkg/aggregrator"
	localMetrics "global-resource-service/resource-management/pkg/common-lib/metrics"
	"global-resource-service/resource-management/pkg/common-lib/types/event"
	"global-resource-service/resource-management/pkg/distributor"
	"global-resource-service/resource-management/pkg/service-api/endpoints"
	"global-resource-service/resource-management/pkg/store/redis"
)

type Config struct {
	ResourceUrls              []string
	MasterIp                  string
	MasterPort                string
	RedisPort		  string
	EventMetricsDumpFrequency time.Duration
}

// Run and create new service-api.  This should never exit.
func Run(c *Config) error {
	klog.V(3).Infof("Starting the API server...")
	klog.V(3).Infof("Connecting the Redis server via port (%v)...", c.RedisPort)

	store := redis.NewRedisClient(c.MasterIp, c.RedisPort, false)
	dist := distributor.GetResourceDistributor()
	dist.SetPersistHelper(store)
	installer := endpoints.NewInstaller(dist)

	r := mux.NewRouter().StrictSlash(true)

	// Setup pprof handlers.
	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// TODO: reuse k8s mux wrapper, pathrecorder.go for simplify this handler by each path
	r.HandleFunc(endpoints.NodeStatusPath, installer.NodeHandler)

	r.HandleFunc(endpoints.ListWatchResourcePath, installer.ResourceHandler)
	r.HandleFunc(endpoints.UpdateResourcePath, installer.ResourceHandler)
	r.HandleFunc(endpoints.ReduceResourcePath, installer.ResourceHandler)

	r.HandleFunc(endpoints.ClientAdminitrationPath, installer.ClientAdministrationHandler)
	r.HandleFunc(endpoints.ClientAdminitrationPath+"/{clientId}", installer.ClientAdministrationHandler)

	address := fmt.Sprintf("%s:%s", c.MasterIp, c.MasterPort)
	klog.Infof("Serving at %s", address)
	server := &http.Server{
		Handler:      r,
		Addr:         address,
		WriteTimeout: 30 * time.Minute,
		ReadTimeout:  30 * time.Minute,
	}

	// start the service and aggregator in go routines
	var wg sync.WaitGroup
	var err error

	klog.V(3).Infof("Starting the resource management service ...")
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = server.ListenAndServe()
	}()

	if err != nil {
		return err
	}

	// start the aggregator instance
	klog.V(3).Infof("Starting the Aggregator ...")
	wg.Add(1)
	go func() {
		defer wg.Done()
		aggregator := aggregrator.NewAggregator(c.ResourceUrls, dist)
		err = aggregator.LwRun()
	}()

	if err != nil {
		return err
	}

	if localMetrics.ResourceManagementMeasurement_Enabled {
		// start the event metrics report
		klog.V(3).Infof("Starting the event metrics reporting routine...")
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				time.Sleep(c.EventMetricsDumpFrequency)
				event.PrintLatencyReport()
			}
		}()
	}

	wg.Wait()
	return nil
}

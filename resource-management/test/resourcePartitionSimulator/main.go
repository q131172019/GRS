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
	"os"
	"time"

	"k8s.io/klog/v2"

	"global-resource-service/resource-management/test/resourceRegionMgrSimulator/app"
	"global-resource-service/resource-management/test/resourceRegionMgrSimulator/data"
)

func main() {
	flag.Usage = printUsage

	// Get the commandline arguments
	c := &app.RegionConfig{}

	flag.StringVar(&c.RegionName, "region_name", "Beijing", "Region name, if not set, default to Beijing")
	flag.IntVar(&c.RpNum, "rp_num", 10, "The number of RPs per region, if not set, default to 10")
	flag.IntVar(&c.NodesPerRP, "nodes_per_rp", 25000, "The number of RPs per region, if not set, default to 25000")
	flag.IntVar(&c.RPDownNumber, "rp_down_number", 1, "The number of RPs to be pull down, needs to be <= rp_num")
	flag.StringVar(&c.MasterPort, "master_port", "9119", "Service port, if not set, default to 9119")
	flag.StringVar(&c.DataPattern, "data_pattern", "Outage", "Simulator data pattern, if not set, default to Outage Mode")
	flag.IntVar(&c.WaitTimeForDataChangePattern, "wait_time_for_data_change_pattern", 5, "Wait time for Outage or Daily pattern, if not set, default to 5")

	if !flag.Parsed() {
		klog.InitFlags(nil)
		flag.Parse()
	}

	// Input parameter error handling for c.RpNum and c.NodesPerRP
	// Fix bug #104
	klog.Info("")
	if c.RpNum < 1 {
		klog.Errorf("Error: Region resource manager simulator config / rp number per region:  (%v) is less than 1", c.RpNum)
		os.Exit(1)
	}

	if c.NodesPerRP < 1 {
		klog.Errorf("Error: Region resource manager simulator config / node number per rp:  (%v) is less than 1", c.NodesPerRP)
		os.Exit(1)
	}
	klog.Info("")

	// Keep a more frequent flush frequency as 1 second
	klog.StartFlushDaemon(time.Second * 1)

	defer klog.Flush()
	klog.Info("")
	klog.Infof("Region resource manager simulator config / region name:    (No.%v)", c.RegionName)
	klog.Infof("Region resource manager simulator config / rp number per region: (%v)", c.RpNum)
	klog.Infof("Region resource manager simulator config / node number per rp:  (%v)", c.NodesPerRP)
	klog.Infof("Region resource manager simulator config / simulator data pattern:  (%v)", c.DataPattern)
	klog.Infof("Region resource manager simulator config / number of rp down: (%v)", c.RPDownNumber)
	klog.Infof("Region resource manager simulator config / wait time for Outage or Daily pattern:  (%v)", c.WaitTimeForDataChangePattern)

	klog.Info("")
	klog.Infof("Starting resource region manager simulator (%v)", c.RegionName)
	klog.Info("")

	// Initialize Added Event List and Modified Event List
	// Region node Added Event List - for initpull
	data.Init(c.RegionName, c.RpNum, c.NodesPerRP)

	// Generate update changes of Default Pattern
	// - simulate random RP down
	// OR
	// Generate update changes of Daily Pattern
	// - simulate 10 changes each minute
	data.MakeDataUpdate(c.DataPattern, c.WaitTimeForDataChangePattern, c.RPDownNumber)

	// Run simulater RSET API server
	if err := app.Run(c); err != nil {
		klog.Errorf("Error: %v\n", err)
	}

	klog.Infof("Exiting reesource management service")
}

// function to print the usage info for the resource management api server
func printUsage() {
	fmt.Println("\nUsage: Region Resource Manager Simulator")
	fmt.Println("\n Per region config options: --region_name=<region name> --rp_num=<number of rp> --nodes_per_rp=<number of nodes> --rp_down_number=<number of RP to pull down> --master_port=<port> --data_pattern=<outage or daily> --wait_time_for_data_change_pattern=<number of minutes>")
	fmt.Println("\n       Per region config options: --region_name=<region name>  --rp_num=<number of rp>  --nodes_per_rp=<number of nodes> --master_port=<port> --data_pattern=<outage or daily> --wait_time_for_data_change_pattern=<number of minutes>")
	fmt.Println()

	os.Exit(0)
}

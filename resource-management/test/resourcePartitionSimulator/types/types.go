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

package types

import (
	"encoding/json"
	"io"

	"global-resource-service/resource-management/pkg/common-lib/types"
	"global-resource-service/resource-management/pkg/common-lib/types/runtime"
)

type RegionNodeEvents [][]*runtime.NodeEvent

type PostCRVstatus bool

// RRM: Resource Region Manager
//
type ResponseFromRRM struct {
	RegionNodeEvents [][]*runtime.NodeEvent
	RvMap            types.TransitResourceVersionMap
	Length           uint64
}

// The type is for pulling data with batch from RRM - Resource Region Manager
//
type PullDataFromRRM struct {
	BatchLength uint64
	DefaultCRV  uint64
	CRV         types.TransitResourceVersionMap
}

// ToJSON serializes the contents of the collection to JSON
// NewEncoder provides better performance than json.Unmarshal
// as it does not have to buffer the output into an in memory
// slice of bytes. This reduces allocations and the overheads
// of the service
//
// https://golang.org/pkg/encoding/json/#NewEncoder
//
// ToJSON serializes the given interface into a string based JSON format
//
func ToJSON(i interface{}, w io.Writer) error {
	e := json.NewEncoder(w)

	return e.Encode(i)
}

func (p *RegionNodeEvents) ToJSON(w io.Writer) error {
	e := json.NewEncoder(w)

	return e.Encode(p)
}

func (p *ResponseFromRRM) ToJSON(w io.Writer) error {
	e := json.NewEncoder(w)

	return e.Encode(p)
}

func (p *PostCRVstatus) ToJSON(w io.Writer) error {
	e := json.NewEncoder(w)

	return e.Encode(p)
}

// FromJSON deserializes the object from JSON string
// in an io.Reader to the given interface
//
func FromJSON(i interface{}, r io.Reader) error {
	d := json.NewDecoder(r)
	return d.Decode(i)
}

func (p *RegionNodeEvents) FromJSON(r io.Reader) error {
	e := json.NewDecoder(r)

	return e.Decode(p)
}

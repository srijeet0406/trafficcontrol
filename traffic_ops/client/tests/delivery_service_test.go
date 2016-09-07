/*
   Copyright 2015 Comcast Cable Communications Management, LLC

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

package test

import (
	"net/http"
	"testing"

	"github.com/Comcast/traffic_control/traffic_ops/client"
	"github.com/Comcast/traffic_control/traffic_ops/client/fixtures"
	"github.com/jheitz200/test_helper"
)

func TestDeliveryServices(t *testing.T) {
	resp := fixtures.DeliveryServices()
	server := testHelper.ValidHTTPServer(resp)
	defer server.Close()

	var httpClient http.Client
	to := client.Session{
		URL:       server.URL,
		UserAgent: &httpClient,
	}

	testHelper.Context(t, "Given the need to test a successful Traffic Ops request for DeliveryServices")

	ds, err := to.DeliveryServices()
	if err != nil {
		testHelper.Error(t, "Should be able to make a request to Traffic Ops")
	} else {
		testHelper.Success(t, "Should be able to make a request to Traffic Ops")
	}

	if len(ds) != 1 {
		testHelper.Error(t, "Should get back \"1\" DeliveryService, got: %d", len(ds))
	} else {
		testHelper.Success(t, "Should get back \"1\" DeliveryService")
	}

	for _, s := range ds {
		if s.XMLID != "ds-test" {
			testHelper.Error(t, "Should get back \"ds-test\" for \"XMLID\", got: %s", s.XMLID)
		} else {
			testHelper.Success(t, "Should get back \"ds-test\" for \"XMLID\"")
		}

		if s.MissLong != "-99.123456" {
			testHelper.Error(t, "Should get back \"-99.123456\" for \"MissLong\", got: %s", s.MissLong)
		} else {
			testHelper.Success(t, "Should get back \"-99.123456\" for \"MissLong\"")
		}
	}
}

func TestDeliveryServicesUnauthorized(t *testing.T) {
	server := testHelper.InvalidHTTPServer(http.StatusUnauthorized)
	defer server.Close()

	var httpClient http.Client
	to := client.Session{
		URL:       server.URL,
		UserAgent: &httpClient,
	}

	testHelper.Context(t, "Given the need to test a failed Traffic Ops request for DeliveryServices")

	_, err := to.DeliveryServices()
	if err == nil {
		testHelper.Error(t, "Should not be able to make a request to Traffic Ops")
	} else {
		testHelper.Success(t, "Should not be able to make a request to Traffic Ops")
	}
}

func TestDeliveryService(t *testing.T) {
	resp := fixtures.DeliveryServices()
	server := testHelper.ValidHTTPServer(resp)
	defer server.Close()

	var httpClient http.Client
	to := client.Session{
		URL:       server.URL,
		UserAgent: &httpClient,
	}

	testHelper.Context(t, "Given the need to test a successful Traffic Ops request for a DeliveryService")

	ds, err := to.DeliveryService("123")
	if err != nil {
		testHelper.Error(t, "Should be able to make a request to Traffic Ops")
	} else {
		testHelper.Success(t, "Should be able to make a request to Traffic Ops")
	}

	if ds.XMLID != "ds-test" {
		testHelper.Error(t, "Should get back \"ds-test\" for \"XMLID\", got: %s", ds.XMLID)
	} else {
		testHelper.Success(t, "Should get back \"ds-test\" for \"XMLID\"")
	}

	if ds.MissLong != "-99.123456" {
		testHelper.Error(t, "Should get back \"-99.123456\" for \"MissLong\", got: %s", ds.MissLong)
	} else {
		testHelper.Success(t, "Should get back \"-99.123456\" for \"MissLong\"")
	}
}

func TestDeliveryServiceUnauthorized(t *testing.T) {
	server := testHelper.InvalidHTTPServer(http.StatusUnauthorized)
	defer server.Close()

	var httpClient http.Client
	to := client.Session{
		URL:       server.URL,
		UserAgent: &httpClient,
	}

	testHelper.Context(t, "Given the need to test a failed Traffic Ops request for a DeliveryService")

	_, err := to.DeliveryService("123")
	if err == nil {
		testHelper.Error(t, "Should not be able to make a request to Traffic Ops")
	} else {
		testHelper.Success(t, "Should not be able to make a request to Traffic Ops")
	}
}

func TestDeliveryServiceState(t *testing.T) {
	resp := fixtures.DeliveryServiceState()
	server := testHelper.ValidHTTPServer(resp)
	defer server.Close()

	var httpClient http.Client
	to := client.Session{
		URL:       server.URL,
		UserAgent: &httpClient,
	}

	testHelper.Context(t, "Given the need to test a successful Traffic Ops request for a DeliveryServiceState")

	state, err := to.DeliveryServiceState("123")
	if err != nil {
		testHelper.Error(t, "Should be able to make a request to Traffic Ops")
	} else {
		testHelper.Success(t, "Should be able to make a request to Traffic Ops")
	}

	if state.Enabled != true {
		testHelper.Error(t, "Should get back \"true\" for \"Enabled\", got: %s", state.Enabled)
	} else {
		testHelper.Success(t, "Should get back \"true\" for \"Enabled\"")
	}
}

func TestDeliveryServiceStateUnauthorized(t *testing.T) {
	server := testHelper.InvalidHTTPServer(http.StatusUnauthorized)
	defer server.Close()

	var httpClient http.Client
	to := client.Session{
		URL:       server.URL,
		UserAgent: &httpClient,
	}

	testHelper.Context(t, "Given the need to test a failed Traffic Ops request for a DeliveryServiceState")

	_, err := to.DeliveryServiceState("123")
	if err == nil {
		testHelper.Error(t, "Should not be able to make a request to Traffic Ops")
	} else {
		testHelper.Success(t, "Should not be able to make a request to Traffic Ops")
	}
}

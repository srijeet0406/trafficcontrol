package plugin

/*
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

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/apache/trafficcontrol/lib/go-tc"
	"github.com/apache/trafficcontrol/traffic_ops/traffic_ops_golang/api"
	"github.com/apache/trafficcontrol/traffic_ops/traffic_ops_golang/maple"
	"github.com/apache/trafficcontrol/traffic_ops/traffic_ops_golang/tenant"
	"github.com/apache/trafficcontrol/traffic_ops/traffic_ops_golang/trafficstats"
	"io/ioutil"
	"net/http"
	"strings"
)

func init() {
	AddPlugin(10000, Funcs{onRequest: getDSStats}, "ds stats plugin", "1.0.0")
}

const DSStatsPath = "/api/3.0/deliveryservice_stats"
const ComcastDatabse = "comcast_ott"

var bearer_token string

func getTokenFromMaple(auth *maple.MapleAuth) (string, error, int) {
	var req *http.Request
	var err error
	client := &http.Client{}
	req, err = http.NewRequest("GET", auth.URL, nil)
	if err != nil {
		return "", err, http.StatusBadRequest
	}
	b64Encoded := base64.URLEncoding.EncodeToString([]byte(auth.User+":"+auth.Password))
	req.Header.Add("Authorization", "Basic " + b64Encoded)
	resp, err := client.Do(req)
	if err != nil {
		return "", err, http.StatusBadRequest
	}
	if resp != nil && resp.StatusCode != http.StatusOK {
		return "", errors.New("unauthorized"), resp.StatusCode
	}
	defer resp.Body.Close()
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err, http.StatusInternalServerError
	}
	return string(respBytes), nil, http.StatusOK
}

func prep(d OnRequestData) {
	inf, userErr, sysErr, errCode := api.NewInfo(d.R, []string{"metricType", "startDate", "endDate"}, nil)
	tx := inf.Tx.Tx
	if userErr != nil || sysErr != nil {
		api.HandleErr(d.W, d.R, tx, errCode, userErr, sysErr)
		return
	}
	defer inf.Close()

	var c tc.TrafficDSStatsConfig
	if c, errCode, userErr = trafficstats.DsConfigFromRequest(d.R, inf); userErr != nil {
		sysErr = fmt.Errorf("Unable to process deliveryservice_stats request: %v", userErr)
		api.HandleErr(d.W, d.R, tx, errCode, userErr, sysErr)
		return
	}

	exists, dsTenant, e := trafficstats.DsTenantIDFromXMLID(c.DeliveryService, tx)
	if e != nil {
		sysErr = e
		errCode = http.StatusInternalServerError
		api.HandleErr(d.W, d.R, tx, errCode, nil, sysErr)
		return
	} else if !exists {
		userErr = fmt.Errorf("No such Delivery Service: %s", c.DeliveryService)
		errCode = http.StatusNotFound
		api.HandleErr(d.W, d.R, tx, errCode, userErr, nil)
		return
	}

	authorized, e := tenant.IsResourceAuthorizedToUserTx(int(dsTenant), inf.User, tx)
	if e != nil {
		api.HandleErr(d.W, d.R, tx, http.StatusInternalServerError, nil, e)
		return
	} else if !authorized {
		// If the Tenant is not authorized to use the resource, then we DON'T tell them that.
		// Instead, we don't disclose that such a Delivery Service exists at all - in keeping with
		// the behavior of /deliveryservices
		// This is different from what Perl used to do, but then again Perl didn't check tenancy at
		// all.
		userErr = fmt.Errorf("No such Delivery Service: %s", c.DeliveryService)
		sysErr = fmt.Errorf("GetDSStats: unauthorized Tenant (#%d) access", inf.User.TenantID)
		errCode = http.StatusNotFound
		api.HandleErr(d.W, d.R, tx, errCode, userErr, sysErr)
		return
	}
	//ToDo: handleRequest after this
}

func makeMapleQuery(d OnRequestData, token string) (error, int) {
	prep(d)
	var req *http.Request
	var err error
	client := &http.Client{}
	query := `SELECT chi
FROM comcast_ott_maple.tr_dns 
WHERE datetime > now() - 10 
limit 10`
	req, err = http.NewRequest("POST", "https://api.maple.comcast.net/logs/event/rawSQL", bytes.NewBuffer([]byte(query)))
	if err != nil {
		return err, http.StatusBadRequest
	}
	req.Header.Add(maple.MapleDatabase, ComcastDatabse)
	req.Header.Add("Authorization", "Bearer " + token)
	req.Header.Add("Content-Type", "text/plain")
	resp, err := client.Do(req)
	if err != nil {
		return err, http.StatusBadRequest
	}
	if resp != nil && resp.StatusCode != http.StatusOK {
		return err, resp.StatusCode
	}
	defer resp.Body.Close()
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err, http.StatusInternalServerError
	}
	fmt.Println("Response from Maple: ", string(respBytes))
	return nil, http.StatusOK
}

func getDSStats(d OnRequestData) IsRequestHandled {
	var token string
	var sc int
	var err error

	if !strings.HasPrefix(d.R.URL.Path, DSStatsPath) {
		return RequestUnhandled
	}
	// make the request first, and check to see if the response code is 401, if so, login, get the bearer token and try again
	err, sc = makeMapleQuery(d, bearer_token)
	if sc == http.StatusUnauthorized {
		token, err, sc = getTokenFromMaple(d.AppCfg.MapleAuthOptions)
		if sc != http.StatusOK {
			d.W.WriteHeader(sc)
			if err != nil {
				d.W.Write([]byte(err.Error()))
			}
			return RequestHandled
		}
		bearer_token = token
		err, sc = makeMapleQuery(d, bearer_token)
		if sc != http.StatusOK {
			d.W.WriteHeader(sc)
			if err != nil {
				d.W.Write([]byte(err.Error()))
			}
			return RequestHandled
		}
	}
	if sc != http.StatusOK {
		d.W.WriteHeader(sc)
		if err != nil {
			d.W.Write([]byte(err.Error()))
		}
		return RequestHandled
	}
	d.W.Header().Set("Content-Type", "text/plain")
	d.W.Write([]byte("DS Stats"))
	return RequestHandled

}

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
	"fmt"
	"strings"
)

func init() {
	AddPlugin(10000, Funcs{onRequest: getDSStats}, "ds stats plugin", "1.0.0")
}

const DSStatsPath = "/api/3.0/deliveryservice_stats"

func getDSStats(d OnRequestData) IsRequestHandled {
	if !strings.HasPrefix(d.R.URL.Path, DSStatsPath) {
		return RequestUnhandled
	}
	fmt.Println(d.R.URL.String())
	fmt.Println(d.AppCfg.MapleAuthOptions.User)
	fmt.Println(d.AppCfg.MapleAuthOptions.Password)
	fmt.Println(d.AppCfg.MapleAuthOptions.URL)
	d.W.Header().Set("Content-Type", "text/plain")
	d.W.Write([]byte("DS Stats"))
	return RequestHandled

}

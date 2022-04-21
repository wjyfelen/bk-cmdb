/*
 * Tencent is pleased to support the open source community by making 蓝鲸 available.
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 * Licensed under the MIT License (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * http://opensource.org/licenses/MIT
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package util

import (
	"bytes"
	"configcenter/src/common/metadata"
	"io/ioutil"
	"net/http"
	"net/url"
)

// GetDailAddress returns the address for net.Dail
func GetDailAddress(URL string) (string, error) {
	uri, err := url.Parse(URL)
	if err != nil {
		return "", err
	}
	var port = uri.Port()
	if uri.Port() == "" {
		port = "80"
	}
	return uri.Hostname() + ":" + port, err
}

func PeekRequest(req *http.Request) ([]byte, error) {
	if req.Body != nil {
		byt, err := ioutil.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		req.Body = ioutil.NopCloser(bytes.NewBuffer(byt))
		return byt, nil
	}
	return make([]byte, 0), nil
}

// JudgeIpAddressV4OrV6 determine whether the input ip address is a v4 or v6 address
func JudgeIpAddressV4OrV6(s string) string {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '.':
			return metadata.IPv4Address
		case ':':
			return metadata.IPv6Address
		}
	}
	return ""
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.ranger.services.nifi.client;

import java.util.HashMap;
import java.util.Map;

public class NiFiConnectionMgr {

    static final String PROP_NIFI_URL = "nifi.url";

    /**
     *
     * @param serviceName
     * @param configs
     *
     * @return
     *
     * @throws Exception
     */
    static public NiFiClient getNiFiClient(String serviceName, Map<String, String> configs) throws Exception {
        final String url = configs.get(PROP_NIFI_URL);

        if (url == null || url.trim().isEmpty()) {
            throw new Exception("Required properties are not set for " + serviceName + ". URL not provided.");
        }

        if (!url.endsWith("/nifi-api")) {
            throw new Exception("Url must end with /nifi-api");
        }

        // TODO create an SSLContext if SSL properties populated
        return new NiFiClient(url.trim(), null);
    }

    /**
     *
     * @param serviceName
     * @param configs
     *
     * @return
     *
     * @throws Exception
     */
    public static HashMap<String, Object> connectionTest(String serviceName, Map<String, String> configs) throws Exception {
        NiFiClient client = getNiFiClient(serviceName, configs);
        return client.connectionTest();
    }

}

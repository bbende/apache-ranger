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

import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.service.ResourceLookupContext;

import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Client to communicate with NiFi and retrieve available resources.
 */
public class NiFiClient {

    private final String url;
    private final SSLContext sslContext;

    public NiFiClient(final String url, final SSLContext sslContext) {
        this.url = url;
        this.sslContext = sslContext;
    }

    /**
     *
     * @return
     * @throws Exception
     */
    public HashMap<String, Object> connectionTest() throws Exception {
        String errMsg = "";
        boolean connectivityStatus;
        HashMap<String, Object> responseData = new HashMap<String, Object>();

        try {
            final String target = url + "/access";
            final Client client = ClientBuilder.newBuilder().sslContext(sslContext).build();
            final Response response = client.target(target).request().get();

            if (Response.Status.ACCEPTED.getStatusCode() == response.getStatus()) {
                connectivityStatus = true;
            } else {
                connectivityStatus = false;
                errMsg = "Status Code = " + response.getStatus();
            }

        } catch (Exception e) {
            connectivityStatus = false;
            errMsg = e.getMessage();
        }

        if (connectivityStatus) {
            String successMsg = "ConnectionTest Successful";
            BaseClient.generateResponseDataMap(connectivityStatus, successMsg, successMsg,
                    null, null, responseData);
        } else {
            String failureMsg = "Unable to retrieve any resources using given parameters. ";
            BaseClient.generateResponseDataMap(connectivityStatus, failureMsg, failureMsg + errMsg,
                    null, null, responseData);
        }

        return responseData;
    }

    /**
     *
     * @param context
     * @return
     */
    public List<String> getResource(ResourceLookupContext context) {
        // TODO retrieve the resources from NiFi
        return Arrays.asList("Resource 1", "Resource 2");
    }

}

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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import org.apache.log4j.Logger;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.service.ResourceLookupContext;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

/**
 * Client to communicate with NiFi and retrieve available resources.
 */
public class NiFiClient {

    static final Logger LOG = Logger.getLogger(NiFiClient.class) ;

    private final String url;
    private final SSLContext sslContext;
    private final HostnameVerifier hostnameVerifier;

    public NiFiClient(final String url, final SSLContext sslContext) {
        this.url = url;
        this.sslContext = sslContext;
        this.hostnameVerifier = new NiFiHostnameVerifier();
    }

    /**
     *
     * @return
     * @throws Exception
     */
    public HashMap<String, Object> connectionTest() throws Exception {
        String errMsg = "";
        boolean connectivityStatus;
        HashMap<String, Object> responseData = new HashMap<>();

        try {
            final ClientConfig config = new DefaultClientConfig();
            if (sslContext != null) {
                config.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES,
                        new HTTPSProperties(hostnameVerifier, sslContext));
            }

            final Client client = Client.create(config);
            final WebResource resource = client.resource(url);
            final Response response = resource.accept("application/json").get(Response.class);

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

    public String getUrl() {
        return url;
    }

    public SSLContext getSslContext() {
        return sslContext;
    }

    public HostnameVerifier getHostnameVerifier() {
        return hostnameVerifier;
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

    /**
     * Custom hostname verifier that checks subject alternative names against the hostname of the URI.
     */
    private static class NiFiHostnameVerifier implements HostnameVerifier {

        @Override
        public boolean verify(final String hostname, final SSLSession ssls) {
            try {
                for (final Certificate peerCertificate : ssls.getPeerCertificates()) {
                    if (peerCertificate instanceof X509Certificate) {
                        final X509Certificate x509Cert = (X509Certificate) peerCertificate;
                        final List<String> subjectAltNames = getSubjectAlternativeNames(x509Cert);
                        if (subjectAltNames.contains(hostname.toLowerCase())) {
                            return true;
                        }
                    }
                }
            } catch (final SSLPeerUnverifiedException | CertificateParsingException ex) {
                LOG.warn("Hostname Verification encountered exception verifying hostname due to: " + ex, ex);
            }

            return false;
        }

        private List<String> getSubjectAlternativeNames(final X509Certificate certificate) throws CertificateParsingException {
            final Collection<List<?>> altNames = certificate.getSubjectAlternativeNames();
            if (altNames == null) {
                return new ArrayList<>();
            }

            final List<String> result = new ArrayList<>();
            for (final List<?> generalName : altNames) {
                /**
                 * generalName has the name type as the first element a String or byte array for the second element. We return any general names that are String types.
                 *
                 * We don't inspect the numeric name type because some certificates incorrectly put IPs and DNS names under the wrong name types.
                 */
                final Object value = generalName.get(1);
                if (value instanceof String) {
                    result.add(((String) value).toLowerCase());
                }

            }

            return result;
        }
    }

}

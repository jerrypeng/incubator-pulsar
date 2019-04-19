/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.admin.internal;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.RequestBuilder;

import java.util.Map;

public class ComponentResource extends BaseResource {

    protected ComponentResource(Authentication auth) {
        super(auth);
    }

    public RequestBuilder addAuthHeaders(RequestBuilder requestBuilder) throws PulsarAdminException {

        try {
            if (auth != null && auth.getAuthData().hasDataForHttp()) {
                for (Map.Entry<String, String> header : auth.getAuthData().getHttpHeaders()) {
                    requestBuilder.addHeader(header.getKey(), header.getValue());
                }
            }

            return requestBuilder;
        } catch (Throwable t) {
            throw new PulsarAdminException.GettingAuthenticationDataException(t);
        }






//        try {
//            Invocation.Builder builder = target.request(MediaType.APPLICATION_JSON);
//            // Add headers for authentication if any
//            if (auth != null && auth.getAuthData().hasDataForHttp()) {
//                for (Map.Entry<String, String> header : auth.getAuthData().getHttpHeaders()) {
//                    builder.header(header.getKey(), header.getValue());
//                }
//            }
//            return builder;
//        } catch (Throwable t) {
//            throw new PulsarAdminException.GettingAuthenticationDataException(t);
//        }
    }
}

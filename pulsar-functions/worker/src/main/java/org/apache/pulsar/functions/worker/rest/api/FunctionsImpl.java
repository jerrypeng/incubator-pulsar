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
package org.apache.pulsar.functions.worker.rest.api;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.policies.data.ExceptionInformation;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.RestException;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.function.Supplier;

import static org.apache.pulsar.functions.worker.rest.RestUtils.throwUnavailableException;

@Slf4j
public class FunctionsImpl extends ComponentImpl {

    public FunctionsImpl(Supplier<WorkerService> workerServiceSupplier) {
        super(workerServiceSupplier, Function.FunctionDetails.ComponentType.FUNCTION);
    }

    /**
     *
     * @param tenant
     * @param namespace
     * @param functionName
     * @param uploadedInputStream
     * @param fileDetail
     * @param functionPkgUrl
     * @param functionConfig
     * @param clientRole
     * @param clientAuthenticationDataHttps
     */
    public void registerFunction(final String tenant,
                                 final String namespace,
                                 final String functionName,
                                 final InputStream uploadedInputStream,
                                 final FormDataContentDisposition fileDetail,
                                 final String functionPkgUrl,
                                 final FunctionConfig functionConfig,
                                 final String clientRole,
                                 final AuthenticationDataHttps clientAuthenticationDataHttps) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        new RegisterFunction(worker()).register(tenant, namespace, functionName, uploadedInputStream, fileDetail,
                functionPkgUrl, functionConfig, clientRole, clientAuthenticationDataHttps);
    }

    /**
     *
     * @param tenant
     * @param namespace
     * @param functionName
     * @param uploadedInputStream
     * @param fileDetail
     * @param functionPkgUrl
     * @param functionConfig
     * @param clientRole
     * @param clientAuthenticationDataHttps
     * @param updateOptions
     */
    public void updateFunction(final String tenant,
                               final String namespace,
                               final String functionName,
                               final InputStream uploadedInputStream,
                               final FormDataContentDisposition fileDetail,
                               final String functionPkgUrl,
                               final FunctionConfig functionConfig,
                               final String clientRole,
                               AuthenticationDataHttps clientAuthenticationDataHttps,
                               UpdateOptions updateOptions) {

    }

    /**
     * Get status of a function instance.  If this worker is not running the function instance,
     * @param tenant the tenant the function belongs to
     * @param namespace the namespace the function belongs to
     * @param functionName the function name
     * @param instanceId the function instance id
     * @return the function status
     */
    public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData getFunctionInstanceStatus(
            final String tenant,
            final String namespace,
            final String functionName,
            final String instanceId,
            final URI uri,
            final String clientRole,
            final AuthenticationDataSource clientAuthenticationDataHttps) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        return new GetFunctionStatus(worker()).getFunctionInstanceStatus(tenant, namespace, functionName,
                instanceId, uri, clientRole, clientAuthenticationDataHttps);
    }

    /**
     * Get statuses of all function instances.
     * @param tenant the tenant the function belongs to
     * @param namespace the namespace the function belongs to
     * @param functionName the function name
     * @return a list of function statuses
     * @throws PulsarAdminException
     */
    public FunctionStatus getFunctionStatus(final String tenant,
                                            final String namespace,
                                            final String functionName,
                                            final URI uri,
                                            final String clientRole,
                                            final AuthenticationDataSource clientAuthenticationDataHttps) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        return new GetFunctionStatus(worker()).getFunctionStatus(tenant, namespace, functionName, uri, clientRole, clientAuthenticationDataHttps);
    }
}
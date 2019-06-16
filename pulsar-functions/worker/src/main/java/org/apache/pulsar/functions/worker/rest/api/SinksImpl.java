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
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.utils.ComponentTypeUtils;
import org.apache.pulsar.functions.utils.SinkConfigUtils;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.RestException;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.net.URI;
import java.util.function.Supplier;

import static org.apache.pulsar.functions.worker.rest.RestUtils.throwUnavailableException;

@Slf4j
public class SinksImpl extends ComponentImpl {

    public void registerSink(final String tenant,
                               final String namespace,
                               final String sinkName,
                               final InputStream uploadedInputStream,
                               final FormDataContentDisposition fileDetail,
                               final String sinkPkgUrl,
                               final SinkConfig sinkConfig,
                               final String clientRole,
                               AuthenticationDataHttps clientAuthenticationDataHttps) {


    }

    public void updateSink(final String tenant,
                             final String namespace,
                             final String sinkName,
                             final InputStream uploadedInputStream,
                             final FormDataContentDisposition fileDetail,
                             final String sinkPkgUrl,
                             final SinkConfig sinkConfig,
                             final String clientRole,
                             AuthenticationDataHttps clientAuthenticationDataHttps,
                             UpdateOptions updateOptions) {


    }

    public SinksImpl(Supplier<WorkerService> workerServiceSupplier) {
        super(workerServiceSupplier, Function.FunctionDetails.ComponentType.SINK);
    }

    public SinkStatus.SinkInstanceStatus.SinkInstanceStatusData getSinkInstanceStatus(final String tenant,
                                                                                      final String namespace,
                                                                                      final String sinkName,
                                                                                      final String instanceId,
                                                                                      final URI uri,
                                                                                      final String clientRole,
                                                                                      final AuthenticationDataSource clientAuthenticationDataHttps) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        return new GetSinkStatus(worker()).getSinkInstanceStatus(tenant, namespace, sinkName,
                instanceId, uri, clientRole, clientAuthenticationDataHttps);
    }

    public SinkStatus getSinkStatus(final String tenant,
                                    final String namespace,
                                    final String sinkName,
                                    final URI uri,
                                    final String clientRole,
                                    final AuthenticationDataSource clientAuthenticationDataHttps) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        return new GetSinkStatus(worker()).getSinkStatus(tenant, namespace, sinkName,
                uri, clientRole, clientAuthenticationDataHttps);
    }

    public SinkConfig getSinkInfo(final String tenant,
                                  final String namespace,
                                  final String componentName) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        // validate parameters
        try {
            validateGetFunctionRequestParams(tenant, namespace, componentName, componentType);
        } catch (IllegalArgumentException e) {
            log.error("Invalid get {} request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, componentName, e);
            throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.error("{} does not exist @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, componentName);
            throw new RestException(Response.Status.NOT_FOUND, String.format(ComponentTypeUtils.toString(componentType) + " %s doesn't exist", componentName));
        }
        Function.FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName, ComponentTypeUtils.toString(componentType));
            throw new RestException(Response.Status.NOT_FOUND, String.format(ComponentTypeUtils.toString(componentType) + " %s doesn't exist", componentName));
        }
        SinkConfig config = SinkConfigUtils.convertFromDetails(functionMetaData.getFunctionDetails());
        return config;
    }
}

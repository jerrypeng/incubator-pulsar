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
package org.apache.pulsar.broker.intercept;

import com.google.common.annotations.Beta;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Service that manages intercepting API calls to a pulsar cluster
 */
@Beta
public class InterceptService {
    private static final Logger log = LoggerFactory.getLogger(InterceptService.class);

    private InterceptProvider provider;
    private final ServiceConfiguration conf;

    public InterceptService(ServiceConfiguration conf, PulsarAdmin pulsarAdmin)
            throws PulsarServerException {
        this.conf = conf;

        try {
            final String providerClassname = conf.getInterceptProvider();
            if (StringUtils.isNotBlank(providerClassname)) {
                provider = (InterceptProvider) Class.forName(providerClassname).newInstance();
                provider.initialize(conf, pulsarAdmin);
                log.info("Interceptor {} has been loaded.", providerClassname);
            } else {
                provider = new InterceptProvider() {};
            }
        } catch (Throwable e) {
            throw new PulsarServerException("Failed to load an intercept provider.", e);
        }
    }

    /**
     * Intercept call for create tenant
     *
     * @param tenant tenant name
     * @param tenantInfo tenant info
     * @param clientRole the role used to create tenant
     */
    public void createTenant(String tenant, TenantInfo tenantInfo, String clientRole) throws InterceptException {
        provider.createTenant(tenant, tenantInfo, clientRole);
    }

    /**
     * Intercept call for creating namespace
     *
     * @param namespaceName the namespace name
     * @param policies polices for this namespace
     * @param clientRole the role used to create namespace
     */
    public void createNamespace(NamespaceName namespaceName, Policies policies, String clientRole) throws InterceptException {
        provider.createNamespace(namespaceName, policies, clientRole);
    }

    /**
     * Intercept create partitioned topic
     * @param topicName the topic name
     * @param partitionedTopicMetadata metadata related to the partioned topic
     * @param clientRole the role used to create partitioned topic
     */
    public void createPartitionedTopic(TopicName topicName, PartitionedTopicMetadata partitionedTopicMetadata, String clientRole) throws InterceptException {
        provider.createPartitionedTopic(topicName, partitionedTopicMetadata, clientRole);
    }

    /**
     * Intercept call for create topic
     *
     * @param topicName the topic name
     * @param clientRole the role used to create topic
     */
    public void createTopic(TopicName topicName, String clientRole) throws InterceptException {
        provider.createTopic(topicName, clientRole);
    }

    /**
     * Intercept update partitioned topic
     * @param topicName the topic name
     * @param partitionedTopicMetadata metadata related to the partioned topic
     * @param clientRole the role used to update partitioned topic
     */
    public void updatePartitionedTopic(TopicName topicName, PartitionedTopicMetadata partitionedTopicMetadata, String clientRole) throws InterceptException {
        provider.updatePartitionedTopic(topicName, partitionedTopicMetadata, clientRole);
    }

    /**
     * Intercept call for create function
     *
     * @param functionConfig function config of the function to be created
     * @param clientRole the role used to create function
     */
    public void createFunction(FunctionConfig functionConfig, String clientRole) throws InterceptException {
        provider.createFunction(functionConfig, clientRole);
    }

    /**
     * Intercept call for update source
     *
     * @param updates updates to this function's function config
     * @param existingFunctionConfig the existing function config
     * @param clientRole the role used to update function
     */
    public void updateFunction(FunctionConfig updates, FunctionConfig existingFunctionConfig, String clientRole) throws InterceptException {
        provider.updateFunction(updates, existingFunctionConfig, clientRole);
    }

    /**
     * Intercept call for create source
     *
     * @param sourceConfig the source config of the source to be created
     * @param clientRole the role used to create source
     */
    public void createSource(SourceConfig sourceConfig, String clientRole) throws InterceptException {
        provider.createSource(sourceConfig, clientRole);
    }

    /**
     * Intercept call for update source
     *  @param updates updates to this source's source config
     * @param existingSourceConfig the existing source config
     * @param clientRole the role used to update source
     */
    public void updateSource(SourceConfig updates, SourceConfig existingSourceConfig, String clientRole) throws InterceptException {
        provider.updateSource(updates, existingSourceConfig, clientRole);
    }

    /**
     * Intercept call for create sink
     *
     * @param sinkConfig the sink config of the sink to be created
     * @param clientRole the role used to create sink
     */
    public void createSink(SinkConfig sinkConfig, String clientRole) throws InterceptException {
        provider.createSink(sinkConfig, clientRole);
    }

    /**
     * Intercept call for update sink
     *  @param updates updates to this sink's source config
     * @param existingSinkConfig the existing source config
     * @param clientRole the role used to update sink
     */
    public void updateSink(SinkConfig updates, SinkConfig existingSinkConfig, String clientRole) throws InterceptException {
        provider.updateSink(updates, existingSinkConfig, clientRole);
    }
}

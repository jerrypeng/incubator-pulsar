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

/**
 * This class provides a mechanism to intercept various API calls
 */
public interface InterceptProvider {

    /**
     * Perform initialization for the intercept provider
     *
     * @param conf broker config object
     */
    default void initialize(ServiceConfiguration conf, PulsarAdmin pulsarAdmin) throws InterceptException {}

    /**
     * Intercept call for create tenant
     *
     * @param tenant tenant name
     * @param tenantInfo tenant info
     * @param clientRole the role used to create tenant
     */
    default void createTenant(String tenant, TenantInfo tenantInfo, String clientRole) throws InterceptException {}

    /**
     * Intercept call for creating namespace
     *
     * @param namespaceName the namespace name
     * @param policies polices for this namespace
     * @param clientRole the role used to create namespace
     */
    default void createNamespace(NamespaceName namespaceName, Policies policies, String clientRole) throws InterceptException {}

    /**
     * Intercept call for create topic
     *
     * @param topicName the topic name
     * @param clientRole the role used to create topic
     */
    default void createTopic(TopicName topicName, String clientRole) throws InterceptException {}

    /**
     * Intercept create partitioned topic
     *  @param topicName the topic name
     * @param numPartitions number of partitions to create for this partitioned topic
     * @param clientRole the role used to create partitioned topic
     */
    default void createPartitionedTopic(TopicName topicName, PartitionedTopicMetadata numPartitions, String clientRole) throws InterceptException {}

    /**
     * Intercept update partitioned topic
     *  @param topicName the topic name
     * @param numPartitions number of partitions to update to
     * @param clientRole the role used to update partitioned topic
     */
    default void updatePartitionedTopic(TopicName topicName, PartitionedTopicMetadata numPartitions, String clientRole) throws InterceptException {}


    /**
     * Intercept call for create function
     *
     * @param functionConfig function config of the function to be created
     * @param clientRole the role used to create function
     */
    default void createFunction(FunctionConfig functionConfig, String clientRole) throws InterceptException {}

    /**
     * Intercept call for update function
     *  @param functionConfig function config of the function to be updated
     * @param existingFunctionConfig
     * @param clientRole the role used to update function
     */
    default void updateFunction(FunctionConfig functionConfig, FunctionConfig existingFunctionConfig, String clientRole) throws InterceptException {}

    /**
     * Intercept call for create source
     *
     * @param sourceConfig the source config of the source to be created
     * @param clientRole the role used to create source
     */
    default void createSource(SourceConfig sourceConfig, String clientRole) throws InterceptException {}

    /**
     * Intercept call for update source
     *  @param sourceConfig the source config of the source to be updated
     * @param existingSourceConfig
     * @param clientRole the role used to update source
     */
    default void updateSource(SourceConfig sourceConfig, SourceConfig existingSourceConfig, String clientRole) throws InterceptException {}

    /**
     * Intercept call for create sink
     *
     * @param sinkConfig the sink config of the sink to be created
     * @param clientRole the role used to create sink
     */
    default void createSink(SinkConfig sinkConfig, String clientRole) throws InterceptException {} ;

    /**
     * Intercept call for update sink
     *  @param sinkConfig the sink config of the sink to be updated
     * @param existingSinkConfig
     * @param clientRole the role used to update sink
     */
    default void updateSink(SinkConfig sinkConfig, SinkConfig existingSinkConfig, String clientRole) throws InterceptException {}

}

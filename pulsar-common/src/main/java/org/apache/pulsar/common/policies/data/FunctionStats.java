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
package org.apache.pulsar.common.policies.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.gson.annotations.SerializedName;
import lombok.Data;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Data
@JsonPropertyOrder({ "receivedTotal", "processedSuccessfullyTotal", "systemExceptionsTotal", "userExceptionsTotal", "avgProcessLatency", "1min", "lastInvocation", "instances" })
public class FunctionStats {

    /**
     * Overall total number of records function received from source
     **/
    public long receivedTotal;

    /**
     * Overall total number of records successfully processed by user function
     **/
    public long processedSuccessfullyTotal;

    /**
     * Overall total number of system exceptions thrown
     **/
    public long systemExceptionsTotal;

    /**
     * Overall total number of user exceptions thrown
     **/
    public long userExceptionsTotal;

    /**
     * Average process latency for function
     **/
    public double avgProcessLatency;

    @JsonProperty("1min")
    public FunctionInstanceStats.FunctionInstanceStatsDataBase oneMin = new FunctionInstanceStats.FunctionInstanceStatsDataBase();

    /**
     * Timestamp of when the function was last invoked by any instance
     **/
    public long lastInvocation;

    @Data
    @JsonPropertyOrder({ "instanceId", "metrics" })
    public static class FunctionInstanceStats {

        /** Instance Id of function instance **/
        public int instanceId;

        @Data
        @JsonPropertyOrder({ "receivedTotal", "processedSuccessfullyTotal", "systemExceptionsTotal", "userExceptionsTotal", "avgProcessLatency" })
        public static class FunctionInstanceStatsDataBase {
            /**
             * Total number of records function received from source for instance
             **/
            public long receivedTotal;

            /**
             * Total number of records successfully processed by user function for instance
             **/
            public long processedSuccessfullyTotal;

            /**
             * Total number of system exceptions thrown for instance
             **/
            public long systemExceptionsTotal;

            /**
             * Total number of user exceptions thrown for instance
             **/
            public long userExceptionsTotal;

            /**
             * Average process latency for function for instance
             **/
            public double avgProcessLatency;
        }

        @Data
        @JsonPropertyOrder({ "receivedTotal", "processedSuccessfullyTotal", "systemExceptionsTotal", "userExceptionsTotal", "avgProcessLatency", "1min", "lastInvocation", "userMetrics" })
        public static class FunctionInstanceStatsData extends FunctionInstanceStatsDataBase {

            @JsonProperty("1min")
            public FunctionInstanceStatsDataBase oneMin = new FunctionInstanceStatsDataBase();

            /**
             * Timestamp of when the function was last invoked for instance
             **/
            public long lastInvocation;

            /**
             * Map of user defined metrics
             **/
            public Map<String, Double> userMetrics = new HashMap<>();
        }

        public FunctionInstanceStatsData metrics = new FunctionInstanceStatsData();
    }

    public List<FunctionInstanceStats> instances = new LinkedList<>();

    public void addInstance(FunctionInstanceStats functionInstanceStats) {
        instances.add(functionInstanceStats);
    }

    public FunctionStats calculateOverall() {

        lastInvocation = 0;
        instances.forEach(new Consumer<FunctionInstanceStats>() {
            @Override
            public void accept(FunctionInstanceStats functionInstanceStats) {
                FunctionInstanceStats.FunctionInstanceStatsData functionInstanceStatsData = functionInstanceStats.getMetrics();
                receivedTotal += functionInstanceStatsData.receivedTotal;
                processedSuccessfullyTotal += functionInstanceStatsData.processedSuccessfullyTotal;
                systemExceptionsTotal += functionInstanceStatsData.systemExceptionsTotal;
                userExceptionsTotal += functionInstanceStatsData.userExceptionsTotal;
                avgProcessLatency += functionInstanceStatsData.avgProcessLatency;

                oneMin.receivedTotal += functionInstanceStatsData.oneMin.receivedTotal;
                oneMin.processedSuccessfullyTotal += functionInstanceStatsData.oneMin.processedSuccessfullyTotal;
                oneMin.systemExceptionsTotal += functionInstanceStatsData.oneMin.systemExceptionsTotal;
                oneMin.userExceptionsTotal += functionInstanceStatsData.oneMin.userExceptionsTotal;
                oneMin.avgProcessLatency += functionInstanceStatsData.oneMin.avgProcessLatency;

                if (functionInstanceStatsData.lastInvocation > lastInvocation) {
                    lastInvocation = functionInstanceStatsData.lastInvocation;
                }

            }
        });
        // calculate average from sum
        avgProcessLatency = avgProcessLatency / instances.size();

        // calculate 1min average from sum
        oneMin.avgProcessLatency = oneMin.avgProcessLatency / instances.size();

        return this;
    }
}

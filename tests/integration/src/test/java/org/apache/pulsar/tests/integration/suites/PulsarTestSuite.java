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
package org.apache.pulsar.tests.integration.suites;

import org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase;
import org.testng.ITest;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

public class PulsarTestSuite extends PulsarClusterTestBase implements ITest {

    @BeforeSuite
    @Override
    public void setupCluster() throws Exception {
        super.setupCluster();
    }

    @AfterSuite
    @Override
    public void tearDownCluster() {
        super.tearDownCluster();
    }

    @Override
    public String getTestName() {
        return "pulsar-test-suite";
    }
}

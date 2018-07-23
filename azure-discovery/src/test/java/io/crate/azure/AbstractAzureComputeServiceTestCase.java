/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.azure;

import io.crate.azure.management.AzureComputeService.Discovery;
import io.crate.azure.management.AzureComputeService.Management;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.test.ESIntegTestCase;


public abstract class AbstractAzureComputeServiceTestCase extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING.getKey(), "azure")
            .put(Management.SUBSCRIPTION_ID.getKey(), "fake")
            .put(Discovery.REFRESH.getKey(), "5s")
            .put(Management.APP_ID.getKey(), "dummy")
            .put(Management.TENANT_ID.getKey(), "dummy")
            .put(Management.APP_SECRET.getKey(), "dummy")
            .put(Management.RESOURCE_GROUP_NAME.getKey(), "dummy");
        return builder.build();
    }

    void checkNumberOfNodes(int expected) {
        NodesInfoResponse nodeInfos = client().admin().cluster().prepareNodesInfo().execute().actionGet();
        assertNotNull(nodeInfos);
        assertNotNull(nodeInfos.getNodes());
        assertEquals(expected, nodeInfos.getNodes().size());
    }
}

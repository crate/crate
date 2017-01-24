/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.crate.azure;

import io.crate.azure.management.AzureComputeService.Discovery;
import io.crate.azure.management.AzureComputeService.Management;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST,
    numDataNodes = 0,
    transportClientRatio = 0,
    numClientNodes = 0)
public class AzureSimpleTests extends AbstractAzureComputeServiceTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(AzureComputeServiceSimpleMock.TestPlugin.class);
    }

    @Test
    public void one_node_should_run_using_private_ip() {
        Settings.Builder settings = Settings.builder()
            .put(Management.RESOURCE_GROUP_NAME.getKey(), "crate-azure")
            .put(Discovery.HOST_TYPE.getKey(), "private_ip");

        logger.info("--> start one node");
        internalCluster().startNode(settings);
        assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").execute().actionGet().getState().nodes().getMasterNodeId(), notNullValue());

        // We expect having 1 node as part of the cluster, let's test that
        checkNumberOfNodes(1);
    }

    @Test
    public void one_node_should_run_using_public_ip() {
        Settings.Builder settings = Settings.builder()
            .put(Management.RESOURCE_GROUP_NAME.getKey(), "crate-azure")
            .put(Discovery.HOST_TYPE.getKey(), "public_ip");
        logger.info("--> start one node");
        internalCluster().startNode(settings);
        assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").execute().actionGet().getState().nodes().getMasterNodeId(), notNullValue());

        // We expect having 1 node as part of the cluster, let's test that
        checkNumberOfNodes(1);
    }

    @Test
    public void one_node_should_run_using_wrong_settings() throws IOException {
        Settings.Builder settings = Settings.builder()
            .put(Management.RESOURCE_GROUP_NAME.getKey(), "crate-azure")
            .put(Discovery.HOST_TYPE.getKey(), "does_not_exist")
            .put(Discovery.DISCOVERY_METHOD.getKey(), "does_not_exist");
        logger.info("--> start one node");
        internalCluster().startNode(settings);
        assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").execute().actionGet().getState().nodes().getMasterNodeId(), notNullValue());

        // We expect having 1 node as part of the cluster, let's test that
        checkNumberOfNodes(1);
    }

}

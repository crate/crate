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

package org.elasticsearch.indices.recovery;

import io.crate.integrationtests.SQLIntegrationTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexGraveyard.SETTING_MAX_TOMBSTONES;
import static org.elasticsearch.gateway.DanglingIndicesState.AUTO_IMPORT_DANGLING_INDICES_SETTING;
import static org.hamcrest.Matchers.is;

@ClusterScope(numDataNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class DanglingIndicesIT extends SQLIntegrationTestCase {

    private Settings buildSettings(boolean writeDanglingIndices, boolean importDanglingIndices) {
        return Settings.builder()
            // Don't keep any indices in the graveyard, so that when we delete an index,
            // it's definitely considered to be dangling.
            .put(SETTING_MAX_TOMBSTONES.getKey(), 0)
            .put(IndicesService.WRITE_DANGLING_INDICES_INFO_SETTING.getKey(), writeDanglingIndices)
            .put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), importDanglingIndices)
            .build();
    }

    /**
     * Check that when dangling indices are discovered, then they are recovered into
     * the cluster, so long as the recovery setting is enabled.
     */
    public void testDanglingIndicesAreRecoveredWhenSettingIsEnabled() throws Exception {
        final Settings settings = buildSettings(true, true);
        internalCluster().startNodes(3, settings);

        execute("create table doc.test(id integer) clustered into 2 shards with(number_of_replicas = 2)");
        ensureGreen("test");
        assertBusy(() -> internalCluster().getInstances(IndicesService.class).forEach(
            indicesService -> assertTrue(indicesService.allPendingDanglingIndicesWritten())));

        boolean refreshIntervalChanged = randomBoolean();
        if (refreshIntervalChanged) {
            client().admin().indices().prepareUpdateSettings("test").setSettings(
                Settings.builder().put("index.refresh_interval", "42s").build()).get();
            assertBusy(() -> internalCluster().getInstances(IndicesService.class).forEach(
                indicesService -> assertTrue(indicesService.allPendingDanglingIndicesWritten())));
        }

        // Restart node, deleting the index in its absence, so that there is a dangling index to recover
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ensureClusterSizeConsistency();
                execute("drop table doc.test");
                return super.onNodeStopped(nodeName);
            }
        });

        assertBusy(() -> assertThat("Expected dangling index test to be recovered",
                                    execute("select 1 from information_schema.tables where table_name='test'").rowCount(),
                                    is((1L))));
        ensureGreen("test");
    }

    /**
     * Check that when dangling indices are discovered, then they are not recovered into
     * the cluster when the recovery setting is disabled.
     */
    public void testDanglingIndicesAreNotRecoveredWhenSettingIsDisabled() throws Exception {
        internalCluster().startNodes(3, buildSettings(false, true));

        execute("create table doc.test(id integer) clustered into 2 shards with(number_of_replicas = 2)");
        ensureGreen("test");

        assertBusy(() -> internalCluster().getInstances(IndicesService.class).forEach(
            indicesService -> assertTrue(indicesService.allPendingDanglingIndicesWritten())));

        // Restart node, deleting the index in its absence, so that there is a dangling index to recover
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ensureClusterSizeConsistency();
                execute("drop table doc.test");
                return super.onNodeStopped(nodeName);
            }
        });

        // Since index recovery is async, we can't prove index recovery will never occur, just that it doesn't occur within some reasonable
        // amount of time
        assertBusy(() -> assertThat("Did not expect dangling index test to be recovered",
                                    execute("select 1 from information_schema.tables where table_name='test'").rowCount(),
                                    is(0L)), 1, TimeUnit.SECONDS);
    }
}

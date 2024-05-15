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
package org.elasticsearch.indices.state;

import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.test.IntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.execution.ddl.tables.TransportCloseTable;

public class CloseIndexIT extends IntegTestCase {

    /**
     * Test for https://github.com/elastic/elasticsearch/issues/47276 which checks that the persisted metadata on a data node does not
     * become inconsistent when using replicated closed indices.
     */
    @Test
    public void testRelocatedClosedIndexIssue() throws Exception {
        final List<String> dataNodes = cluster().startDataOnlyNodes(2);
        // allocate shard to first data node
        execute("create table doc.test(x int) clustered into 1 shards with (number_of_replicas=0, \"routing.allocation.include._name\" = ?)", new Object[] {dataNodes.get(0)});
        var numDocs = randomIntBetween(0, 50);
        var bulkArgs = new Object[numDocs][];
        for(var i = 0; i < numDocs; i++) {
            bulkArgs[i] = new Object[] { i };
        }
        if (numDocs > 0) {
            execute("insert into doc.test values(?)", bulkArgs);
        }

        execute("alter table doc.test close");
        execute("alter table doc.test set (\"routing.allocation.include._name\" = ?)", new Object[] { dataNodes.get(1) });
        ensureGreen("test");
        cluster().fullRestart();
        ensureGreen("test");
        assertIndexIsClosed("test");
    }

    static void assertIndexIsClosed(final String... indices) {
        var clusterState = FutureUtils.get(client().admin().cluster().state(new ClusterStateRequest())).getState();
        var availableIndices = clusterState.metadata().indices();
        assertThat(availableIndices.keys().toArray(String.class), Matchers.arrayContaining(indices));
        for (String index : indices) {
            final IndexMetadata indexMetadata = availableIndices.get(index);
            assertThat(indexMetadata.getState()).isEqualTo(IndexMetadata.State.CLOSE);
            final Settings indexSettings = indexMetadata.getSettings();
            assertThat(indexSettings.hasValue(IndexMetadata.VERIFIED_BEFORE_CLOSE_SETTING.getKey())).isTrue();
            assertThat(indexSettings.getAsBoolean(IndexMetadata.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), false)).isTrue();
            assertThat(clusterState.routingTable().index(index)).isNotNull();
            assertThat(clusterState.blocks().hasIndexBlock(index, IndexMetadata.INDEX_CLOSED_BLOCK)).isTrue();
            assertThat(clusterState.blocks().indices().getOrDefault(index, emptySet()).stream()
                           .filter(clusterBlock -> clusterBlock.id() == TransportCloseTable.INDEX_CLOSED_BLOCK_ID).count()).as("Index " + index + " must have only 1 block with [id=" + TransportCloseTable.INDEX_CLOSED_BLOCK_ID + "]").isEqualTo(1L);
        }
    }
}

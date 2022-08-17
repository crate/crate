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

package org.elasticsearch.gateway;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.IntegTestCase.ClusterScope;
import org.elasticsearch.test.IntegTestCase.Scope;
import org.elasticsearch.test.TestCluster;

import org.elasticsearch.test.IntegTestCase;


@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class MetadataWriteDataNodesIT extends IntegTestCase {

    public void testMetaWrittenAlsoOnDataNode() throws Exception {
        // this test checks that index state is written on data only nodes if they have a shard allocated
        String masterNode = internalCluster().startMasterOnlyNode(Settings.EMPTY);
        String dataNode = internalCluster().startDataOnlyNode(Settings.EMPTY);
        execute("create table doc.test(x int) with (number_of_replicas = 0)");
        execute("insert into doc.test values(1)");
        ensureGreen("test");
        assertIndexInMetaState(dataNode, "test");
        assertIndexInMetaState(masterNode, "test");
    }

    public void testIndexFilesAreRemovedIfAllShardsFromIndexRemoved() throws Exception {
        // this test checks that the index data is removed from a data only node once all shards have been allocated away from it
        String masterNode = internalCluster().startMasterOnlyNode(Settings.EMPTY);
        List<String> nodeNames= internalCluster().startDataOnlyNodes(2);
        String node1 = nodeNames.get(0);
        String node2 = nodeNames.get(1);

        execute("create table doc.test(x int) with (number_of_replicas = 0, \"routing.allocation.include._name\" = ?)", new Object[]{node1});
        execute("insert into doc.test values(1)");
        ensureGreen();
        assertIndexInMetaState(node1, "test");
        Index resolveIndex = resolveIndex("test");
        assertIndexDirectoryExists(node1, resolveIndex);
        assertIndexDirectoryDeleted(node2, resolveIndex);
        assertIndexInMetaState(masterNode, "test");
        assertIndexDirectoryDeleted(masterNode, resolveIndex);

        logger.debug("relocating index...");
        execute("alter table doc.test set(\"routing.allocation.include._name\" = ?)", new Object[] {node2});
        client().admin().cluster().health(new ClusterHealthRequest().waitForNoRelocatingShards(true)).get();
        ensureGreen();
        assertIndexDirectoryDeleted(node1, resolveIndex);
        assertIndexInMetaState(node2, "test");
        assertIndexDirectoryExists(node2, resolveIndex);
        assertIndexInMetaState(masterNode, "test");
        assertIndexDirectoryDeleted(masterNode, resolveIndex);

        execute("drop table doc.test");
        assertIndexDirectoryDeleted(node1, resolveIndex);
        assertIndexDirectoryDeleted(node2, resolveIndex);
    }

    protected void assertIndexDirectoryDeleted(final String nodeName, final Index index) throws Exception {
        assertBusy(() -> {
                       logger.info("checking if index directory exists...");
                       assertFalse("Expecting index directory of " + index + " to be deleted from node " + nodeName,
                                   indexDirectoryExists(nodeName, index));
                   }
        );
    }

    protected void assertIndexDirectoryExists(final String nodeName, final Index index) throws Exception {
        assertBusy(() -> assertTrue("Expecting index directory of " + index + " to exist on node " + nodeName,
                                    indexDirectoryExists(nodeName, index))
        );
    }

    protected void assertIndexInMetaState(final String nodeName, final String indexName) throws Exception {
        assertBusy(() -> {
                       logger.info("checking if meta state exists...");
                       try {
                           assertTrue("Expecting meta state of index " + indexName + " to be on node " + nodeName,
                                      getIndicesMetadataOnNode(nodeName).containsKey(indexName));
                       } catch (Exception e) {
                           logger.info("failed to load meta state", e);
                           fail("could not load meta state");
                       }
                   }
        );
    }


    private boolean indexDirectoryExists(String nodeName, Index index) {
        NodeEnvironment nodeEnv = ((TestCluster) cluster()).getInstance(NodeEnvironment.class, nodeName);
        Path[] paths = nodeEnv.indexPaths(index);
        for (Path path : paths) {
            if (Files.exists(path)) {
                return true;
            }
        }
        return false;
    }

    private ImmutableOpenMap<String, IndexMetadata> getIndicesMetadataOnNode(String nodeName) {
        final Coordinator coordinator = (Coordinator) internalCluster().getInstance(Discovery.class, nodeName);
        return coordinator.getApplierState().getMetadata().getIndices();
    }
}

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

package io.crate.integrationtests;

import io.crate.common.Glob;
import io.crate.testing.SQLResponse;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.close.TransportVerifyShardBeforeCloseAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.crate.execution.ddl.tables.TransportCloseTable.INDEX_CLOSED_BLOCK_ID;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(
    minNumDataNodes = 2,
    maxNumDataNodes = 3,
    numClientNodes = 0)
public class ReopenWhileClosingIntegrationTest extends SQLTransportIntegrationTest {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final HashSet<Class<? extends Plugin>> classes = new HashSet<>(super.nodePlugins());
        classes.add(MockTransportService.TestPlugin.class);
        return classes;
    }

    @Test
    public void test_reopen_during_close_succeeds() throws Exception {
        execute("create table doc.test (x int)");
        int bulkSize = randomIntBetween(10, 20);
        Object[][] bulkArgs = new Object[bulkSize][];
        for (int i = 0; i < bulkSize; i++) {
            bulkArgs[i] = new Object[]{i};
        }
        execute("insert into doc.test values (?)", bulkArgs);
        ensureYellowAndNoInitializingShards();

        CountDownLatch block = new CountDownLatch(1);
        Releasable releaseBlock = interceptVerifyShardBeforeCloseActions("test", block::countDown);

        ActionFuture<SQLResponse> closeTableResponse = sqlExecutor
            .execute("alter table doc.test close", null);
        assertThat(
            "Waiting for index to have a closing blocked",
            block.await(60, TimeUnit.SECONDS),
            is(true)
        );
        assertIndexIsBlocked("test");
        assertThat(closeTableResponse.isDone(), is(false));

        execute("alter table doc.test open");

        releaseBlock.close();

        assertBusy(() -> assertThat(isClosed("doc", "test"), is(false)), 5, TimeUnit.SECONDS);
    }

    /**
     * Intercepts and blocks the {@link TransportVerifyShardBeforeCloseAction}
     * executed for the given table name pattern.
     */
    private Releasable interceptVerifyShardBeforeCloseActions(String indexNamePattern,
                                                              Runnable onIntercept) {
        MockTransportService mockTransportService = (MockTransportService) internalCluster()
            .getInstance(TransportService.class, internalCluster().getMasterName());

        CountDownLatch release = new CountDownLatch(1);
        for (DiscoveryNode node : internalCluster().clusterService().state().getNodes()) {
            mockTransportService.addSendBehavior(
                internalCluster().getInstance(TransportService.class, node.getName()),
                (connection, requestId, action, request, options) -> {
                    if (action.startsWith(TransportVerifyShardBeforeCloseAction.NAME)) {
                        if (request instanceof TransportVerifyShardBeforeCloseAction.ShardRequest) {
                            String index = Objects.requireNonNull(
                                ((TransportVerifyShardBeforeCloseAction.ShardRequest) request).shardId()
                            ).getIndexName();
                            if (Glob.globMatch(indexNamePattern, index)) {
                                logger.info("request {} intercepted for index {}", requestId, index);
                                onIntercept.run();
                                try {
                                    release.await();
                                    logger.info("request {} released for index {}", requestId, index);
                                } catch (final InterruptedException e) {
                                    throw new AssertionError(e);
                                }
                            }
                        }

                    }
                    connection.sendRequest(requestId, action, request, options);
                });
        }
        final RunOnce releaseOnce = new RunOnce(release::countDown);
        return releaseOnce::run;
    }

    private boolean isClosed(String schema, String table) {
        execute(
            "select closed " +
            "from information_schema.tables " +
            "where table_name = ? and table_schema = ?", new Object[]{table, schema});
        return (boolean) response.rows()[0][0];
    }

    private static void assertIndexIsBlocked(String... indices) {
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        for (String index : indices) {
            assertThat(
                "Table " + index + " must have only 1 block with [id=" + INDEX_CLOSED_BLOCK_ID + "]",
                clusterState.blocks().indices().getOrDefault(index, emptySet()).stream()
                    .filter(clusterBlock -> clusterBlock.id() == INDEX_CLOSED_BLOCK_ID).count(),
                is(1L)
            );
        }
    }
}

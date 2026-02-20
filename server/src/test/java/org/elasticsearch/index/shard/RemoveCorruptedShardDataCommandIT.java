/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.elasticsearch.index.shard;


import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.translog.TestTranslog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.CorruptionUtils;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Test;

import io.crate.server.cli.MockTerminal;
import io.crate.testing.UseRandomizedSchema;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

@UseRandomizedSchema(random = false)
@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoveCorruptedShardDataCommandIT extends IntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockEngineFactoryPlugin.class, InternalSettingsPlugin.class);
    }

    @Test
    public void test_corrupted_table() throws Exception {
        run_corrupted_index_test(false);
    }

    @Test
    public void test_corrupted_table_partition() throws Exception {
        run_corrupted_index_test(true);
    }

    @SuppressWarnings({"resource", "ResultOfMethodCallIgnored", "CatchMayIgnoreException"})
    public void run_corrupted_index_test(boolean partitioned) throws Exception {
        boolean truncateTranslog = randomBoolean();

        final String node = cluster().startNode();

        String stmt = "CREATE TABLE t (a INT, p INT) CLUSTERED INTO 1 SHARDS";
        if (partitioned) {
            stmt += " PARTITIONED BY (p)";
        }
        stmt += " WITH (number_of_replicas = 0)";

        execute(stmt);
        int totalDocs = 0;
        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            int batchSize = randomIntBetween(10, 100);
            Object[][] bulkArgs = new Object[batchSize][];
            for (int j = 0; j < batchSize; j++) {
                bulkArgs[j] = new Object[]{totalDocs + j, 1};
            }
            execute("INSERT INTO t (a, p) VALUES (?, ?)", bulkArgs);
            execute("OPTIMIZE TABLE t");
            totalDocs += batchSize;
        }
        execute("REFRESH TABLE t");
        execute("SELECT COUNT(*) FROM t");
        assertThat(response).hasRows(String.valueOf(totalDocs));

        IndicesService indicesService = cluster().getInstance(IndicesService.class, node);
        IndexService indexService = indicesService.iterator().next();
        IndexShard indexShard = indexService.getShard(0);
        ShardPath shardPath = indexShard.shardPath();

        final RemoveCorruptedShardDataCommand command = new RemoveCorruptedShardDataCommand();
        final MockTerminal terminal = new MockTerminal();
        final OptionParser parser = command.getParser();
        final Settings nodePathSettings = cluster().dataPathSettings(node);
        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(cluster().getDefaultSettings()).put(nodePathSettings).build());
        ArrayList<String> args = new ArrayList<>(Arrays.asList("-table", "doc.t", "-shard-id", String.valueOf(shardPath.getShardId().id())));
        if (partitioned) {
            args.add("-Pp=1");
        }
        final OptionSet options = parser.parse(args.toArray(new String[0]));

        // Try running it before the node is stopped (and shard is closed)
        try {
            command.execute(terminal, options, environment);
            fail("expected the command to fail as node is locked");
        } catch (Exception e) {
            assertThat(e.getMessage()).isEqualTo("failed to lock node's directory, is CrateDB still running?");
        }

        cluster().restartNode(node, new TestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                // Try running it before the shard is corrupted, it should flip out because there is no corruption file marker
                try {
                    command.execute(terminal, options, environment);
                    fail("expected the command to fail as there is no corruption file marker");
                } catch (Exception e) {
                    assertThat(e.getMessage()).startsWith("Shard does not seem to be corrupted at");
                }

                CorruptionUtils.corruptIndex(random(), shardPath.resolveIndex(), false);
                if (truncateTranslog) {
                    TestTranslog.corruptRandomTranslogFile(logger, random(), shardPath.resolveTranslog());
                }
                return super.onNodeStopped(nodeName);
            }
        });

        // shard should be failed due to a corrupted index
        assertBusy(() -> {
            execute("SELECT explanation FROM sys.allocations WHERE shard_id = 0 AND primary = true");
            assertThat(response.rowCount()).isEqualTo(1L);
            // The error message is not deterministic, it is either
            //  - "cannot allocate because all found copies of the shard are either stale or corrupt" or
            //  - "cannot allocate because allocation is not permitted to any of the nodes that hold an in-sync shard copy"
            // The latter happens when the shard is marked as stale before it is marked as failed, which can happen when
            // the node starts up and the shard is being checked for corruption, but the check is not finished before
            // the allocation deciders are checking if the shard can be allocated. In this case, the shard is marked
            // as stale and the allocation deciders will not allow allocating it until it is marked as failed,
            // which happens after the corruption check is finished.
            assertThat((String) response.rows()[0][0]).satisfiesAnyOf(
                msg -> assertThat(msg).startsWith("cannot allocate because all found copies of the shard are either stale or corrupt"),
                msg -> assertThat(msg).startsWith("cannot allocate because allocation is not permitted to any of the nodes that hold an in-sync shard copy")
            );
        });

        if (truncateTranslog) {
            args.add("-truncate-clean-translog");
        }

        cluster().restartNode(node, new TestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                terminal.addTextInput("y");
                command.execute(terminal, options, environment);

                return super.onNodeStopped(nodeName);
            }
        });

        logger.info("--> output:\n{}", terminal.getOutput());

        if (truncateTranslog) {
            assertThat(terminal.getOutput()).contains("Removing existing translog files");
        }

        String nodeId = clusterService().state().getRoutingNodes().iterator().next().nodeId();
        String tableIdent = partitioned ? "doc.t PARTITION (p = 1)" : "doc.t";
        assertThat(terminal.getOutput()).contains("ALTER TABLE " + tableIdent + " REROUTE ALLOCATE STALE PRIMARY SHARD 0 ON '" + nodeId + "' WITH (accept_data_loss = true);");

        // Ensure node has started
        ensureStableCluster(1, node);

        // Allocate the stale primary shard (this is actually happening by the `PROMOTE_REPLICA` command)
        execute("ALTER TABLE " + tableIdent + " REROUTE PROMOTE REPLICA SHARD 0 ON '" + node + "' WITH (accept_data_loss = true)");

        // wait until the stale shard is allocated and started
        assertBusy(() -> {
            execute("SELECT state FROM sys.shards WHERE id = 0 AND primary = true AND state = 'STARTED'");
            assertThat(response.rowCount()).isEqualTo(1);
        });

        execute("SELECT * FROM t");
        assertThat(response.rowCount()).isGreaterThan(0L);
        assertThat(response.rowCount()).isLessThan(totalDocs);
    }
}

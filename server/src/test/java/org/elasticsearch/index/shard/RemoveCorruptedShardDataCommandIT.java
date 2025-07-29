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

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.CorruptionUtils;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Test;

import io.crate.server.cli.MockTerminal;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoveCorruptedShardDataCommandIT extends IntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockEngineFactoryPlugin.class, InternalSettingsPlugin.class);
    }

    @Test
    public void testCorruptIndex() throws Exception {

        final String node = cluster().startNode();

        execute("CREATE TABLE t (a INT) CLUSTERED INTO 3 SHARDS WITH (number_of_replicas = 0)");
        int totalDocs = 0;
        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            int batchSize = randomIntBetween(10, 100);
            Object[][] bulkArgs = new Object[batchSize][];
            for (int j = 0; j < batchSize; j++) {
                bulkArgs[j] = new Object[]{totalDocs + j};
            }
            execute("INSERT INTO t (a) VALUES (?)", bulkArgs);
            execute("OPTIMIZE TABLE t");
            totalDocs += batchSize;
        }
        execute("REFRESH TABLE t");
        execute("SELECT COUNT(*) FROM t");
        assertThat(response).hasRows(String.valueOf(totalDocs));

        IndicesService indicesService = cluster().getInstance(IndicesService.class, node);
        IndexService indexService = indicesService.iterator().next();
        ShardPath shardPath = indexService.getShard(0).shardPath();

        final RemoveCorruptedShardDataCommand command = new RemoveCorruptedShardDataCommand();
        final MockTerminal terminal = new MockTerminal();
        final OptionParser parser = command.getParser();
        final Settings nodePathSettings = cluster().dataPathSettings(node);
        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(cluster().getDefaultSettings()).put(nodePathSettings).build());
        final OptionSet options = parser.parse("-index", shardPath.getShardId().getIndexUUID(), "-shard-id", String.valueOf(shardPath.getShardId().id()));

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
                return super.onNodeStopped(nodeName);
            }


        });

        // check that corrupt marker is there
        AtomicInteger corruptedMarkerCount = new AtomicInteger();
        SimpleFileVisitor<Path> corruptedVisitor = new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (Files.isRegularFile(file) && file.getFileName().toString().startsWith(Store.CORRUPTED_MARKER_NAME_PREFIX)) {
                    corruptedMarkerCount.incrementAndGet();
                }
                return FileVisitResult.CONTINUE;
            }
        };
        assertBusy(() -> {
            Files.walkFileTree(shardPath.resolveIndex(), corruptedVisitor);
            assertThat(corruptedMarkerCount.get()).as("store has to be marked as corrupted").isEqualTo(1);
        });

        cluster().restartNode(node, new TestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                terminal.addTextInput("y");
                command.execute(terminal, options, environment);

                return super.onNodeStopped(nodeName);
            }
        });

        waitNoPendingTasksOnAll();

        execute(String.format("SELECT COUNT(*) < %s FROM t", totalDocs));
        assertThat(response).hasRows("true");
    }
}

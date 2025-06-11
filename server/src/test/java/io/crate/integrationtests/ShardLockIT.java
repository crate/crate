/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.integrationtests;

import static io.crate.testing.Asserts.assertExpectedLogMessages;
import static io.crate.testing.Asserts.assertThat;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.IntegTestCase.ClusterScope;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.After;
import org.junit.Test;

@ClusterScope(numDataNodes = 1)
public class ShardLockIT extends IntegTestCase {

    @After
    @Override
    public void tearDown() throws Exception {
        execute("RESET GLOBAL stats.jobs_log_size");
        super.tearDown();
    }

    @Test
    @TestLogging("org.elasticsearch.indices.IndicesService:DEBUG,org.elasticsearch.index.engine:TRACE,org.elasticsearch.index.IndexService:TRACE")
    public void test_create_shard_retries_on_shard_lock() throws Exception {
        String nodeName = cluster().startDataOnlyNode();
        execute("""
            create table doc.tbl (x int)
            clustered into 1 shards
            with (
                number_of_replicas = 1,
                "routing.allocation.exclude._name" = ?
            )
            """,
            new Object[] { nodeName }
        );
        Index index = resolveIndex("doc.tbl");
        ShardId shardId = new ShardId(index, 0);
        NodeEnvironment nodeEnv = cluster().getInstance(NodeEnvironment.class, nodeName);

        Logger testLogger = LogManager.getLogger(IndicesClusterStateService.class);
        MockLogAppender appender = new MockLogAppender();
        Loggers.addAppender(testLogger, appender);
        appender.start();
        var expectationShardFailure = new MockLogAppender.UnseenEventExpectation(
            "Shard failure",
            IndicesClusterStateService.class.getName(),
            Level.WARN,
            "[tbl][0] marking and sending shard failed due to [failed to create shard]"
        );
        appender.addExpectation(expectationShardFailure);

        Thread thread = new Thread(() -> {
            int i = 0;
            // trigger some cluster state changes which the retry logic has to account for
            while (i++ < 10) {
                execute("SET GLOBAL PERSISTENT stats.jobs_log_size = ?", new Object[] { i });
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        thread.start();

        var expectation = new MockLogAppender.PatternSeenEventExcpectation(
            "Logs retries",
            IndicesService.class.getName(),
            Level.DEBUG,
            "Repeated attempts to acquire shardLock for .*\\. Retrying again in .*"
        );
        try (ShardLock shardLock = nodeEnv.shardLock(shardId, "block shard for test")) {
            assertExpectedLogMessages(
                () -> execute("alter table doc.tbl reset (\"routing.allocation.exclude._name\")"),
                IndicesService.class.getName(),
                expectation
            );
        } finally {
            thread.join();
        }

        // Verify that the shard was never marked as failed.
        // If new cluster state changes are applied, the shard would be created again even that it failed before.
        // But the shard creation should never fail, but the retry logic should succeed.
        try {
            appender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(testLogger, appender);
        }

        ensureGreen();
        execute("select health from sys.health where table_name = 'tbl'");
        assertThat(response).hasRows("GREEN");
    }
}

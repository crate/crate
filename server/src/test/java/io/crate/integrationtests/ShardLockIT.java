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
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.IntegTestCase.ClusterScope;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

@ClusterScope(numDataNodes = 1)
public class ShardLockIT extends IntegTestCase {

    @Test
    @TestLogging("org.elasticsearch.indices.IndicesService:DEBUG")
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
        Index index = resolveIndex("tbl");
        ShardId shardId = new ShardId(index, 0);
        NodeEnvironment nodeEnv = cluster().getInstance(NodeEnvironment.class, nodeName);
        MockLogAppender appender = new MockLogAppender();
        var expectation = new MockLogAppender.PatternSeenEventExcpectation(
            "Logs retries",
            IndicesService.class.getName(),
            Level.DEBUG,
            "Repeated attempts to acquire shardLock for .*\\. Retrying again in .*"
        );
        appender.addExpectation(expectation);
        appender.start();
        Logger logger = LogManager.getLogger(IndicesService.class);
        Loggers.addAppender(logger, appender);
        try (ShardLock shardLock = nodeEnv.shardLock(shardId, "block shard for test")) {
            execute("alter table doc.tbl reset (\"routing.allocation.exclude._name\")");
            assertBusy(() -> appender.assertAllExpectationsMatched());
        } finally {
            Loggers.removeAppender(logger, appender);
        }

        ensureGreen();
        execute("select health from sys.health where table_name = 'tbl'");
        assertThat(response).hasRows("GREEN");
    }
}

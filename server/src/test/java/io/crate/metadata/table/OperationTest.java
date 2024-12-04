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

package io.crate.metadata.table;

import static io.crate.metadata.table.Operation.ALL;
import static io.crate.metadata.table.Operation.ALTER;
import static io.crate.metadata.table.Operation.ALTER_BLOCKS;
import static io.crate.metadata.table.Operation.ALTER_CLOSE;
import static io.crate.metadata.table.Operation.ALTER_OPEN;
import static io.crate.metadata.table.Operation.ALTER_REROUTE;
import static io.crate.metadata.table.Operation.COPY_TO;
import static io.crate.metadata.table.Operation.CREATE_SNAPSHOT;
import static io.crate.metadata.table.Operation.DELETE;
import static io.crate.metadata.table.Operation.DROP;
import static io.crate.metadata.table.Operation.INSERT;
import static io.crate.metadata.table.Operation.OPTIMIZE;
import static io.crate.metadata.table.Operation.READ;
import static io.crate.metadata.table.Operation.READ_ONLY;
import static io.crate.metadata.table.Operation.REFRESH;
import static io.crate.metadata.table.Operation.SHOW_CREATE;
import static io.crate.metadata.table.Operation.UPDATE;
import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_SUBSCRIPTION_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class OperationTest extends ESTestCase {

    @Test
    public void testBuildFromEmptyIndexBlocks() throws Exception {
        assertThat(Operation.buildFromIndexSettingsAndState(Settings.EMPTY, IndexMetadata.State.OPEN, false)).isEqualTo(ALL);
    }

    @Test
    public void testBuildFromSingleIndexBlocks() throws Exception {
        assertThat(Operation.buildFromIndexSettingsAndState(Settings.builder().put(
                IndexMetadata.SETTING_READ_ONLY, true).build(), IndexMetadata.State.OPEN, false)).isEqualTo(READ_ONLY);

        assertThat(Operation.buildFromIndexSettingsAndState(Settings.builder()
                .put(IndexMetadata.SETTING_BLOCKS_READ, true).build(), IndexMetadata.State.OPEN, false))
            .containsExactlyInAnyOrder(
                UPDATE, INSERT, DELETE, DROP, ALTER, ALTER_OPEN, ALTER_CLOSE,
                ALTER_BLOCKS, REFRESH, OPTIMIZE, ALTER_REROUTE);

        assertThat(Operation.buildFromIndexSettingsAndState(Settings.builder()
                .put(IndexMetadata.SETTING_BLOCKS_WRITE, true).build(), IndexMetadata.State.OPEN, false))
            .containsExactlyInAnyOrder(
                READ, ALTER, ALTER_OPEN, ALTER_CLOSE, ALTER_BLOCKS, SHOW_CREATE,
                REFRESH, OPTIMIZE, COPY_TO, CREATE_SNAPSHOT, ALTER_REROUTE);

        assertThat(Operation.buildFromIndexSettingsAndState(Settings.builder()
                .put(IndexMetadata.SETTING_BLOCKS_METADATA, true).build(), IndexMetadata.State.OPEN, false))
            .containsExactlyInAnyOrder(
                READ, UPDATE, INSERT, DELETE, ALTER_BLOCKS, ALTER_OPEN,
                ALTER_CLOSE, REFRESH, SHOW_CREATE, OPTIMIZE, ALTER_REROUTE);
    }

    @Test
    public void testBuildFromCompoundIndexBlocks() throws Exception {
        assertThat(Operation.buildFromIndexSettingsAndState(Settings.builder()
                .put(IndexMetadata.SETTING_BLOCKS_READ, true)
                .put(IndexMetadata.SETTING_BLOCKS_WRITE, true).build(), IndexMetadata.State.OPEN, false))
            .containsExactlyInAnyOrder(
                ALTER, ALTER_OPEN, ALTER_CLOSE, ALTER_BLOCKS,
                REFRESH, OPTIMIZE, ALTER_REROUTE);

        assertThat(Operation.buildFromIndexSettingsAndState(Settings.builder()
                .put(IndexMetadata.SETTING_BLOCKS_WRITE, true)
                .put(IndexMetadata.SETTING_BLOCKS_METADATA, true).build(), IndexMetadata.State.OPEN, false))
            .containsExactlyInAnyOrder(
                READ, ALTER_OPEN, ALTER_CLOSE, ALTER_BLOCKS, REFRESH,
                SHOW_CREATE, OPTIMIZE, ALTER_REROUTE);

        assertThat(Operation.buildFromIndexSettingsAndState(Settings.builder()
                .put(IndexMetadata.SETTING_BLOCKS_READ, true)
                .put(IndexMetadata.SETTING_BLOCKS_METADATA, true).build(), IndexMetadata.State.OPEN, false))
            .containsExactlyInAnyOrder(
                INSERT, UPDATE, DELETE, ALTER_OPEN, ALTER_CLOSE,
                ALTER_BLOCKS, REFRESH, OPTIMIZE, ALTER_REROUTE);
    }

    @Test
    public void test_allowed_operations_for_replicated_table() {
        var replicatedIndexSettings = Settings.builder().put(REPLICATION_SUBSCRIPTION_NAME.getKey(), "sub1").build();
        assertThat(
            Operation.buildFromIndexSettingsAndState(replicatedIndexSettings, IndexMetadata.State.OPEN, false))
            .containsExactlyInAnyOrder(
                READ, ALTER, ALTER_BLOCKS, ALTER_REROUTE, OPTIMIZE, REFRESH, COPY_TO, SHOW_CREATE);
    }

    @Test
    public void test_allowed_operations_for_published_table() {
        assertThat(Operation.buildFromIndexSettingsAndState(Settings.EMPTY, IndexMetadata.State.OPEN, true))
            .isEqualTo(Operation.PUBLISHED_IN_LOGICAL_REPLICATION);
    }

    @Test
    public void test_allowed_operations_for_published_and_subcribed_table() {
        var replicatedIndexSettings = Settings.builder().put(REPLICATION_SUBSCRIPTION_NAME.getKey(), "sub1").build();
        assertThat(
            Operation.buildFromIndexSettingsAndState(replicatedIndexSettings, IndexMetadata.State.OPEN, true))
            .isEqualTo(Operation.SUBSCRIBED_IN_LOGICAL_REPLICATION);
    }
}

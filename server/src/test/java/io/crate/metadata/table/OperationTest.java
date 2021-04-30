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

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;

public class OperationTest extends ESTestCase {

    @Test
    public void testBuildFromEmptyIndexBlocks() throws Exception {
        assertThat(Operation.buildFromIndexSettingsAndState(Settings.EMPTY, IndexMetadata.State.OPEN), is(Operation.ALL));
    }

    @Test
    public void testBuildFromSingleIndexBlocks() throws Exception {
        assertThat(Operation.buildFromIndexSettingsAndState(Settings.builder().put(
                IndexMetadata.SETTING_READ_ONLY, true).build(), IndexMetadata.State.OPEN),
            is(Operation.READ_ONLY));

        assertThat(Operation.buildFromIndexSettingsAndState(Settings.builder()
                .put(IndexMetadata.SETTING_BLOCKS_READ, true).build(), IndexMetadata.State.OPEN),
            containsInAnyOrder(Operation.UPDATE, Operation.INSERT, Operation.DELETE, Operation.DROP, Operation.ALTER,
                Operation.ALTER_OPEN_CLOSE, Operation.ALTER_BLOCKS, Operation.REFRESH, Operation.OPTIMIZE, Operation.ALTER_REROUTE));

        assertThat(Operation.buildFromIndexSettingsAndState(Settings.builder()
                .put(IndexMetadata.SETTING_BLOCKS_WRITE, true).build(), IndexMetadata.State.OPEN),
            containsInAnyOrder(Operation.READ, Operation.ALTER, Operation.ALTER_OPEN_CLOSE, Operation.ALTER_BLOCKS,
                Operation.SHOW_CREATE, Operation.REFRESH, Operation.OPTIMIZE, Operation.COPY_TO,
                Operation.CREATE_SNAPSHOT, Operation.ALTER_REROUTE));

        assertThat(Operation.buildFromIndexSettingsAndState(Settings.builder()
                .put(IndexMetadata.SETTING_BLOCKS_METADATA, true).build(), IndexMetadata.State.OPEN),
            containsInAnyOrder(Operation.READ, Operation.UPDATE, Operation.INSERT, Operation.DELETE, Operation.ALTER_BLOCKS,
                Operation.ALTER_OPEN_CLOSE, Operation.REFRESH, Operation.SHOW_CREATE, Operation.OPTIMIZE, Operation.ALTER_REROUTE));
    }

    @Test
    public void testBuildFromCompoundIndexBlocks() throws Exception {
        assertThat(Operation.buildFromIndexSettingsAndState(Settings.builder()
                .put(IndexMetadata.SETTING_BLOCKS_READ, true)
                .put(IndexMetadata.SETTING_BLOCKS_WRITE, true).build(), IndexMetadata.State.OPEN),
            containsInAnyOrder(Operation.ALTER, Operation.ALTER_OPEN_CLOSE, Operation.ALTER_BLOCKS, Operation.REFRESH,
                Operation.OPTIMIZE, Operation.ALTER_REROUTE));

        assertThat(Operation.buildFromIndexSettingsAndState(Settings.builder()
                .put(IndexMetadata.SETTING_BLOCKS_WRITE, true)
                .put(IndexMetadata.SETTING_BLOCKS_METADATA, true).build(), IndexMetadata.State.OPEN),
            containsInAnyOrder(Operation.READ, Operation.ALTER_OPEN_CLOSE, Operation.ALTER_BLOCKS, Operation.REFRESH,
                Operation.SHOW_CREATE, Operation.OPTIMIZE, Operation.ALTER_REROUTE));

        assertThat(Operation.buildFromIndexSettingsAndState(Settings.builder()
                .put(IndexMetadata.SETTING_BLOCKS_READ, true)
                .put(IndexMetadata.SETTING_BLOCKS_METADATA, true).build(), IndexMetadata.State.OPEN),
            containsInAnyOrder(Operation.INSERT, Operation.UPDATE, Operation.DELETE, Operation.ALTER_OPEN_CLOSE,
                Operation.ALTER_BLOCKS, Operation.REFRESH, Operation.OPTIMIZE, Operation.ALTER_REROUTE));
    }
}

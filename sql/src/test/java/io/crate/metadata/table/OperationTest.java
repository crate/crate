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

package io.crate.metadata.table;

import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;

public class OperationTest extends CrateUnitTest {

    @Test
    public void testBuildFromEmptyIndexBlocks() throws Exception {
        assertThat(Operation.buildFromIndexSettings(Settings.EMPTY), is(Operation.ALL));
    }

    @Test
    public void testBuildFromSingleIndexBlocks() throws Exception {
        assertThat(Operation.buildFromIndexSettings(Settings.builder().put(
            IndexMetaData.SETTING_READ_ONLY, true).build()),
            is(Operation.READ_ONLY));

        assertThat(Operation.buildFromIndexSettings(Settings.builder()
                .put(IndexMetaData.SETTING_BLOCKS_READ, true).build()),
            containsInAnyOrder(Operation.INSERT, Operation.UPDATE, Operation.DELETE, Operation.ALTER, Operation.DROP));

        assertThat(Operation.buildFromIndexSettings(Settings.builder()
                .put(IndexMetaData.SETTING_BLOCKS_WRITE, true).build()),
            containsInAnyOrder(Operation.READ, Operation.ALTER));

        assertThat(Operation.buildFromIndexSettings(Settings.builder()
                .put(IndexMetaData.SETTING_BLOCKS_METADATA, true).build()),
            containsInAnyOrder(Operation.READ, Operation.INSERT, Operation.UPDATE, Operation.DELETE));
    }

    @Test
    public void testBuildFromCompoundIndexBlocks() throws Exception {
        assertThat(Operation.buildFromIndexSettings(Settings.builder()
                .put(IndexMetaData.SETTING_BLOCKS_READ, true)
                .put(IndexMetaData.SETTING_BLOCKS_WRITE, true).build()),
            containsInAnyOrder(Operation.ALTER));

        assertThat(Operation.buildFromIndexSettings(Settings.builder()
                .put(IndexMetaData.SETTING_BLOCKS_WRITE, true)
                .put(IndexMetaData.SETTING_BLOCKS_METADATA, true).build()),
            containsInAnyOrder(Operation.READ));

        assertThat(Operation.buildFromIndexSettings(Settings.builder()
                .put(IndexMetaData.SETTING_BLOCKS_READ, true)
                .put(IndexMetaData.SETTING_BLOCKS_METADATA, true).build()),
            containsInAnyOrder(Operation.INSERT, Operation.UPDATE, Operation.DELETE));
    }
}

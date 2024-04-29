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

package io.crate.execution.ddl.tables;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.common.settings.IndexScopedSettings.DEFAULT_SCOPED_SETTINGS;

import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.metadata.RelationName;
import io.crate.replication.logical.metadata.Publication;

public class AlterTableOperationTest extends ESTestCase {

    private static Settings baseIndexSettings() {
        return Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_NUMBER_OF_SHARDS, 5)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
    }

    @Test
    public void testValidateReadOnlyForResizeOperation() {
        Settings settings = Settings.builder()
            .put(baseIndexSettings())
            .put(SETTING_BLOCKS_WRITE, false)      // allow writes
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("t1")
            .settings(settings)
            .build();


        assertThatThrownBy(() -> AlterTableOperation.validateReadOnlyIndexForResize(indexMetadata))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageStartingWith("Table/Partition needs to be at a read-only state");
    }

    @Test
    public void testValidateSettingForPublishedTables() {
        Settings settings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 5L)
            .build();

        RelationName t1 = new RelationName("doc", "t1");
        var oneTablePublished = Map.of("pub1", new Publication("owner", false, List.of(t1)));

        assertThatThrownBy(() -> AlterTableOperation.validateSettingsForPublishedTables(t1, settings, oneTablePublished, DEFAULT_SCOPED_SETTINGS))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(
                    "Setting [index.number_of_shards] cannot be applied to table 'doc.t1' because it is included in a logical replication publication 'pub1'");

        var allTablesPublished = Map.of("pub1", new Publication("owner", true, List.of()));

        assertThatThrownBy(() -> AlterTableOperation.validateSettingsForPublishedTables(t1, settings, allTablesPublished, DEFAULT_SCOPED_SETTINGS))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(
                    "Setting [index.number_of_shards] cannot be applied to table 'doc.t1' because it is included in a logical replication publication 'pub1'");
    }

    @Test
    public void testEqualNumberOfShardsRequestedIsNotPermitted() {
        IndexMetadata indexMetadata = IndexMetadata.builder("t1")
            .settings(baseIndexSettings())
            .build();

        assertThatThrownBy(() -> AlterTableOperation.validateNumberOfShardsForResize(indexMetadata, 5))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Table/partition is already allocated <5> shards");
    }

    @Test
    public void testGreaterNumberOfShardsRequestedIsSupported() {
        IndexMetadata indexMetadata = IndexMetadata.builder("t1")
            .settings(baseIndexSettings())
            .build();
        AlterTableOperation.validateNumberOfShardsForResize(indexMetadata, 10);
    }

    @Test
    public void testNumberOfShardsRequestedNotAFactorOfCurrentIsNotSupported() {
        IndexMetadata indexMetadata = IndexMetadata.builder("t1")
            .settings(baseIndexSettings())
            .build();

        assertThatThrownBy(() -> AlterTableOperation.validateNumberOfShardsForResize(indexMetadata, 3))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Requested number of shards: <3> needs to be a factor of the current one: <5>");
    }

    //@Test
    //public void testNullNumberOfShardsRequestedIsNotPermitted() {
    //    assertThatThrownBy(() -> AlterTableOperation.getNumberOfShards(Settings.EMPTY))
    //        .isExactlyInstanceOf(NullPointerException.class)
    //        .hasMessage("Setting 'number_of_shards' is missing");
    //}
}

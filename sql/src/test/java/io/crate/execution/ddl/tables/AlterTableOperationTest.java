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

package io.crate.execution.ddl.tables;

import io.crate.Constants;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class AlterTableOperationTest extends CrateUnitTest {


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
        IndexMetaData indexMetaData = IndexMetaData.builder("t1")
            .settings(settings)
            .build();

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Table/Partition needs to be at a read-only state");
        AlterTableOperation.validateReadOnlyIndexForResize(indexMetaData);
    }

    @Test
    public void testEqualNumberOfShardsRequestedIsNotPermitted() {
        IndexMetaData indexMetaData = IndexMetaData.builder("t1")
            .settings(baseIndexSettings())
            .build();

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Table/partition is already allocated <5> shards");
        AlterTableOperation.validateNumberOfShardsForResize(indexMetaData, 5);
    }

    @Test
    public void testGreaterNumberOfShardsRequestedIsSupported() {
        IndexMetaData indexMetaData = IndexMetaData.builder("t1")
            .settings(baseIndexSettings())
            .build();
        AlterTableOperation.validateNumberOfShardsForResize(indexMetaData, 10);
    }

    @Test
    public void testNumberOfShardsRequestedNotAFactorOfCurrentIsNotSupported() {
        IndexMetaData indexMetaData = IndexMetaData.builder("t1")
            .settings(baseIndexSettings())
            .build();

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Requested number of shards: <3> needs to be a factor of the current one: <5>");
        AlterTableOperation.validateNumberOfShardsForResize(indexMetaData, 3);
    }

    @Test
    public void testNullNumberOfShardsRequestedIsNotPermitted() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("Setting 'number_of_shards' is missing");
        AlterTableOperation.getNumberOfShards(Settings.EMPTY);
    }
}

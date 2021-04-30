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

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;

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

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Table/Partition needs to be at a read-only state");
        AlterTableOperation.validateReadOnlyIndexForResize(indexMetadata);
    }

    @Test
    public void testEqualNumberOfShardsRequestedIsNotPermitted() {
        IndexMetadata indexMetadata = IndexMetadata.builder("t1")
            .settings(baseIndexSettings())
            .build();

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Table/partition is already allocated <5> shards");
        AlterTableOperation.validateNumberOfShardsForResize(indexMetadata, 5);
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

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Requested number of shards: <3> needs to be a factor of the current one: <5>");
        AlterTableOperation.validateNumberOfShardsForResize(indexMetadata, 3);
    }

    @Test
    public void testNullNumberOfShardsRequestedIsNotPermitted() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("Setting 'number_of_shards' is missing");
        AlterTableOperation.getNumberOfShards(Settings.EMPTY);
    }
}

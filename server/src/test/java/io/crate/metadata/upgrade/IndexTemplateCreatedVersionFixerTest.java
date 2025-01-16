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

package io.crate.metadata.upgrade;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

public class IndexTemplateCreatedVersionFixerTest {

    @Test
    public void test_does_not_affect_empty_partitioned_table() throws IOException {
        var metadataWithEmptyPartitionedTable = new Metadata.Builder().put(
            IndexTemplateMetadata.builder("empty_table")
                .patterns(List.of("*"))
                .putMapping("{\"default\": {}}")
                .build()
        ).build();

        assertThat(new IndexTemplateCreatedVersionFixer().apply(metadataWithEmptyPartitionedTable))
            .isSameAs(metadataWithEmptyPartitionedTable);
    }

    @Test
    public void test_does_not_affect_partitioned_table_with_single_created_version() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_5_4_0)
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .build();

        // create indexMetadata and templateMetadata as V_5_4_0
        IndexMetadata indexMetadata1 = IndexMetadata.builder("t.idx1").settings(settings).build();
        IndexMetadata indexMetadata2 = IndexMetadata.builder("t.idx2").settings(settings).build();
        IndexTemplateMetadata indexTemplateMetadata = IndexTemplateMetadata.builder("t")
            .settings(settings)
            .patterns(List.of("*"))
            .putMapping("{\"default\": {}}")
            .build();
        Metadata metadata = Metadata.builder()
            .put(indexTemplateMetadata)
            .put(indexMetadata1, true)
            .put(indexMetadata2, true)
            .build();
        assertThat(new IndexTemplateCreatedVersionFixer().apply(metadata)).isSameAs(metadata);
    }

    @Test
    public void test_does_not_affect_partitioned_table_with_valid_multiple_created_versions() throws IOException {
        Settings v540 = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_5_4_0)
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        Settings v470 = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_4_7_0)
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .build();

        // create indexMetadata as V_5_4_0, V_4_7_0
        // create templateMetadata as V_4_7_0(valid)
        IndexMetadata indexMetadata1 = IndexMetadata.builder("t.idx1").settings(v540).build();
        IndexMetadata indexMetadata2 = IndexMetadata.builder("t.idx2").settings(v470).build();
        IndexTemplateMetadata indexTemplateMetadata = IndexTemplateMetadata.builder("t")
            .settings(v470)
            .patterns(List.of("*"))
            .putMapping("{\"default\": {}}")
            .build();
        Metadata metadata = Metadata.builder()
            .put(indexTemplateMetadata)
            .put(indexMetadata1, true)
            .put(indexMetadata2, true)
            .build();
        assertThat(new IndexTemplateCreatedVersionFixer().apply(metadata)).isSameAs(metadata);
    }

    @Test
    public void test_does_not_affect_partitioned_table_with_invalid_multiple_created_versions() throws IOException {
        Settings v540 = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_5_4_0)
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        Settings v470 = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_4_7_0)
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .build();

        // create indexMetadata as V_5_4_0, V_4_7_0
        // create templateMetadata as V_5_4_0(invalid). An index with V_4_7_0 suggests that the created version
        // for the template must be on or before V_4_7_0
        IndexMetadata indexMetadata1 = IndexMetadata.builder("t.idx1").settings(v540).build();
        IndexMetadata indexMetadata2 = IndexMetadata.builder("t.idx2").settings(v470).build();
        IndexTemplateMetadata indexTemplateMetadata = IndexTemplateMetadata.builder("t")
            .settings(v540)
            .patterns(List.of("*"))
            .putMapping("{\"default\": {}}")
            .build();
        Metadata metadata = Metadata.builder()
            .put(indexTemplateMetadata)
            .put(indexMetadata1, true)
            .put(indexMetadata2, true)
            .build();
        Metadata fixed = new IndexTemplateCreatedVersionFixer().apply(metadata);
        assertThat(fixed.templates().size()).isEqualTo(1);
        IndexTemplateMetadata fixedTemplateMd = fixed.templates().get("t");
        assertThat(fixedTemplateMd.settings().getAsVersion(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .isEqualTo(Version.V_4_7_0);
    }
}

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

package org.elasticsearch.cluster.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.TestCustomMetadata;
import org.junit.Before;
import org.junit.Test;

import io.crate.expression.udf.UdfUnitTest;
import io.crate.expression.udf.UserDefinedFunctionMetadata;
import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.IndexName;
import io.crate.metadata.RelationName;
import io.crate.metadata.SearchPath;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class MetadataUpgradeServiceTest extends CrateDummyClusterServiceUnitTest {

    private MetadataUpgradeService metadataUpgradeService;

    @Before
    public void setUpUpgradeService() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService);
        e.udfService().registerLanguage(UdfUnitTest.DUMMY_LANG);
        metadataUpgradeService = new MetadataUpgradeService(
            e.nodeCtx,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            e.udfService());
    }


    @Test
    public void test_upgradeIndexMetadata_ensure_UDFs_are_loaded_before_checkMappingsCompatibility_is_called() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService);
        e.udfService().registerLanguage(UdfUnitTest.DUMMY_LANG);
        var metadataUpgradeService = new MetadataUpgradeService(
            e.nodeCtx,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            e.udfService()
        );
        metadataUpgradeService.upgradeIndexMetadata(
            IndexMetadata.builder("test")
                .settings(settings(Version.V_5_7_0))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build(),
            IndexTemplateMetadata.builder("test")
                .patterns(List.of("*"))
                .putMapping("{\"default\": {}}")
                .build(),
            Version.V_5_7_0,
            UserDefinedFunctionsMetadata.of(new UserDefinedFunctionMetadata(
                "custom",
                "foo",
                List.of(),
                DataTypes.INTEGER,
                "dummy",
                "def foo(): return 1"
            )));
        FunctionImplementation functionImplementation = e.nodeCtx.functions().get(
            "custom",
            "foo",
            List.of(),
            SearchPath.pathWithPGCatalogAndDoc()
        );
        assertThat(functionImplementation).isNotNull();
    }

    @Test
    public void test_no_metadata_upgrade_build_relation_metadata() {
        Metadata metadata = randomMetadata(false, new CustomMetadata("data"));
        Metadata upgrade = metadataUpgradeService.upgradeMetadata(metadata);
        assertThat(upgrade).isNotSameAs(metadata);
        assertThat(Metadata.isGlobalStateEquals(upgrade, metadata)).isFalse();
        for (IndexMetadata indexMetadata : upgrade) {
            assertThat(metadata.hasIndexMetadata(indexMetadata)).isTrue();
            RelationName relationName = IndexName.decode(indexMetadata.getIndex().getName()).toRelationName();
            RelationMetadata.Table relationMetadata = upgrade.getRelation(relationName);
            assertThat(relationMetadata.settings()).isEqualTo(indexMetadata.getSettings());
        }
    }

    @Test
    public void testCustomMetadataValidation() {
        Metadata metadata = randomMetadata(false, new CustomMetadata("data"));
        try {
            metadataUpgradeService.upgradeMetadata(metadata);
        } catch (IllegalStateException e) {
            assertThat(e.getMessage()).isEqualTo("custom meta data too old");
        }
    }

    @Test
    public void test_index_metadata_upgrade_and_build_relation_metadata() {
        Metadata metadata = randomMetadata(false);
        Metadata upgrade = metadataUpgradeService.upgradeMetadata(metadata);
        assertThat(upgrade).isNotSameAs(metadata);
        assertThat(Metadata.isGlobalStateEquals(upgrade, metadata)).isFalse();
        for (IndexMetadata indexMetadata : upgrade) {
            assertThat(metadata.hasIndexMetadata(indexMetadata)).isTrue();
            RelationName relationName = IndexName.decode(indexMetadata.getIndex().getName()).toRelationName();
            RelationMetadata.Table relationMetadata = upgrade.getRelation(relationName);
            assertThat(relationMetadata.settings()).isEqualTo(indexMetadata.getSettings());
        }
    }

    @Test
    public void test_index_metadata_upgrade_and_update_relation_metadata() {
        Metadata metadata = randomMetadata(true);
        Metadata upgrade = metadataUpgradeService.upgradeMetadata(metadata);
        assertThat(upgrade).isNotSameAs(metadata);
        assertThat(Metadata.isGlobalStateEquals(upgrade, metadata)).isFalse();
        for (IndexMetadata indexMetadata : upgrade) {
            assertThat(metadata.hasIndexMetadata(indexMetadata)).isTrue();
            RelationName relationName = IndexName.decode(indexMetadata.getIndex().getName()).toRelationName();
            RelationMetadata.Table relationMetadata = upgrade.getRelation(relationName);
            assertThat(relationMetadata.settings()).isEqualTo(indexMetadata.getSettings());
        }
    }

    @Test
    public void test_index_metadata_only_upgrade_when_not_on_current_version() {
        IndexMetadata indexMetadata = IndexMetadata.builder("old_index")
            .settings(settings(Version.V_5_7_0))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        IndexMetadata indexMetadataAlreadyUpgraded = IndexMetadata.builder("already_upgraded")
            .settings(settings(Version.V_5_7_0).put(IndexMetadata.SETTING_VERSION_UPGRADED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        IndexMetadata indexMetadataCreatedOnCurrentVersion = IndexMetadata.builder("created_on_current")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        Metadata metadata = Metadata.builder()
            .put(indexMetadata, false)
            .put(indexMetadataAlreadyUpgraded, false)
            .put(indexMetadataCreatedOnCurrentVersion, false)
            .build();
        Metadata upgrade = metadataUpgradeService.upgradeMetadata(metadata);

        IndexMetadata upgradedIndexMetadata = upgrade.index("old_index");
        assertThat(upgradedIndexMetadata).isNotEqualTo(indexMetadata);
        assertThat(upgradedIndexMetadata.getSettings().getAsVersion(IndexMetadata.SETTING_VERSION_UPGRADED, Version.V_5_7_0).equals(Version.CURRENT));

        IndexMetadata alreadyUpgraded = upgrade.index("already_upgraded");
        assertThat(alreadyUpgraded).isEqualTo(indexMetadataAlreadyUpgraded);

        IndexMetadata createdOnCurrentVersion = upgrade.index("created_on_current");
        assertThat(createdOnCurrentVersion).isEqualTo(indexMetadataCreatedOnCurrentVersion);
    }

    @Test
    public void test_blob_table_is_added_to_RelationMetadata() {
        var relationName = new RelationName("blob", "b1");
        var metadataBuilder = Metadata.builder();
        var indexMetadataBuilder = IndexMetadata.builder(relationName.indexNameOrAlias())
            .settings(Settings.builder().put("index.version.created", Version.V_5_10_12))
            .numberOfShards(1)
            .numberOfReplicas(0);

        metadataBuilder.put(indexMetadataBuilder);
        var upgradedMetadata = metadataUpgradeService.upgradeMetadata(metadataBuilder.build());
        assertThat((RelationMetadata) upgradedMetadata.getRelation(relationName))
            .isExactlyInstanceOf(RelationMetadata.BlobTable.class);
    }

    private static class CustomMetadata extends TestCustomMetadata {
        public static final String TYPE = "custom_md_1";

        protected CustomMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    public static Metadata randomMetadata(boolean withRelationMetadata, TestCustomMetadata... customMetadatas) {
        Metadata.Builder builder = Metadata.builder();
        for (TestCustomMetadata customMetadata : customMetadatas) {
            builder.putCustom(customMetadata.getWriteableName(), customMetadata);
        }
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            String indexName = randomAlphaOfLength(10);
            builder.put(
                IndexMetadata.builder(indexName)
                    .settings(settings(Version.CURRENT))
                    .numberOfReplicas(randomIntBetween(0, 3))
                    .numberOfShards(randomIntBetween(1, 5))
            );
            if (withRelationMetadata) {
                builder.setTable(
                    IndexName.decode(indexName).toRelationName(),
                    List.of(),
                    builder.get(indexName).getSettings(),
                    null,
                    ColumnPolicy.STRICT,
                    null,
                    Map.of(),
                    List.of(),
                    List.of(),
                    IndexMetadata.State.OPEN,
                    List.of(),
                    0
                );
            }
        }
        return builder.build();
    }
}

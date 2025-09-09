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
import java.util.Optional;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.AbstractScopedSettings;
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
import io.crate.metadata.IndexType;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SearchPath;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.SysColumns;
import io.crate.metadata.upgrade.IndexTemplateUpgrader;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

@SuppressWarnings("deprecation") // tests deprecated components for BWC
public class MetadataUpgradeServiceTest extends CrateDummyClusterServiceUnitTest {

    private MetadataUpgradeService metadataUpgradeService;
    private String minimalTemplateMappingSource =
        """
        {
            "default": {
                "_meta": {
                    "partitioned_by": [["p", "integer"]]
                },
                "properties": {
                    "p": {
                        "type": "integer"
                    }
                }
            }
        }
        """;

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
    public void test_default_template_is_removed() throws Exception {
        IndexTemplateMetadata oldTemplate = IndexTemplateMetadata.builder(IndexTemplateUpgrader.CRATE_DEFAULTS)
            .patterns(List.of("*"))
            .putMapping("{\"default\": {}}")
            .build();
        Metadata metadata = Metadata.builder(clusterService.state().metadata())
            .put(oldTemplate)
            .build();
        Metadata newMetadata = metadataUpgradeService.upgradeMetadata(metadata);
        assertThat(newMetadata.templates().get(IndexTemplateUpgrader.CRATE_DEFAULTS)).isNull();
    }


    @Test
    public void test_archived_settings_are_removed() throws Exception {
        Settings settings = Settings.builder()
            .put(AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX + "some.setting", true)   // archived, must be filtered out
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
            .build();

        String templateName = PartitionName.templateName("doc", "t1");
        IndexTemplateMetadata template = IndexTemplateMetadata.builder(templateName)
            .settings(settings)
            .putMapping(minimalTemplateMappingSource)
            .patterns(List.of("*"))
            .build();

        Metadata metadata = Metadata.builder(clusterService.state().metadata())
            .put(template)
            .build();

        Metadata newMetadata = metadataUpgradeService.upgradeMetadata(metadata);
        assertThat(newMetadata.templates().get(templateName)).isNull();
        RelationMetadata.Table relation = newMetadata.getRelation(new RelationName("doc", "t1"));
        assertThat(relation).isNotNull();
        assertThat(relation.settings().keySet()).containsExactly(
            IndexMetadata.SETTING_NUMBER_OF_SHARDS,
            IndexMetadata.SETTING_VERSION_CREATED,
            IndexMetadata.SETTING_VERSION_UPGRADED
        );
    }

    @Test
    public void test_invalid_setting_is_removed_for_template_in_custom_schema() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
            .put("index.recovery.initial_shards", "quorum")
            .build();
        String templateName = PartitionName.templateName("foobar", "t1");
        IndexTemplateMetadata template = IndexTemplateMetadata.builder(templateName)
            .settings(settings)
            .putMapping(minimalTemplateMappingSource)
            .patterns(List.of("*"))
            .build();

        Metadata metadata = Metadata.builder(clusterService.state().metadata())
            .put(template)
            .build();
        Metadata newMetadata = metadataUpgradeService.upgradeMetadata(metadata);

        assertThat(newMetadata.templates().get(templateName)).isNull();
        RelationMetadata.Table relation = newMetadata.getRelation(new RelationName("foobar", "t1"));
        assertThat(relation).isNotNull();
        assertThat(relation.settings().keySet()).containsExactly(
            IndexMetadata.SETTING_NUMBER_OF_SHARDS,
            IndexMetadata.SETTING_VERSION_CREATED,
            IndexMetadata.SETTING_VERSION_UPGRADED
        );
    }

    @Test
    public void test__dropped_0_is_removed_from_template_mapping() throws Throwable {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
            .build();
        String templateName = PartitionName.templateName("doc", "events");
        var template = IndexTemplateMetadata.builder(templateName)
            .patterns(List.of("*"))
            .settings(settings)
            .putMapping(
                """
                {
                    "default": {
                        "_meta": {
                            "partitioned_by": [["p", "integer"]]
                        },
                        "properties": {
                            "p": {
                                "type": "integer"
                            },
                            "name": {
                                "type": "keyword",
                                "_dropped_0": {
                                }
                            }
                        }
                    }
                }
                """
            )
            .build();

        Metadata metadata = Metadata.builder(clusterService.state().metadata())
            .put(template)
            .build();
        Metadata newMetadata = metadataUpgradeService.upgradeMetadata(metadata);
        RelationName relationName = new RelationName("doc", "events");
        RelationMetadata.Table relation = newMetadata.getRelation(relationName);
        assertThat(relation).isNotNull();
        SimpleReference expectedRef = new SimpleReference(
            new ReferenceIdent(relationName, "name"),
            RowGranularity.DOC,
            DataTypes.STRING,
            IndexType.PLAIN,
            true,
            true,
            1,
            0,
            false,
            null
        );
        Optional<Reference> match = relation.columns().stream().filter(x -> x.column().name().equals("name")).findAny();
        assertThat(match).hasValue(expectedRef);
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
        assertThat(Metadata.isGlobalStateEquals(upgrade, metadata)).isTrue();
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
    public void test_doesnot_create_relation_metadata_for_up2date_index_metadata_with_bogus_name() throws Exception {
        IndexMetadata indexMetadata = IndexMetadata.builder(UUIDs.randomBase64UUID())
            .indexName("kaputt")
            .settings(settings(Version.V_6_1_0))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        RelationName tblName = new RelationName("doc", "tbl");
        Metadata metadata = Metadata.builder()
            .put(indexMetadata, false)
            .setTable(
                tblName,
                List.of(),
                Settings.EMPTY,
                SysColumns.ID.COLUMN,
                ColumnPolicy.STRICT,
                null,
                Map.of(),
                List.of(),
                List.of(),
                State.OPEN,
                List.of(indexMetadata.getIndexUUID()),
                1
            )
            .build();

        Metadata upgraded = metadataUpgradeService.upgradeMetadata(metadata);
        RelationMetadata.Table tblRelation = upgraded.getRelation(tblName);
        assertThat(tblRelation).isNotNull();
        RelationMetadata.Table kaputtRelation = upgraded.getRelation(new RelationName("doc", "kaputt"));
        assertThat(kaputtRelation).isNull();
    }

    @Test
    public void test_creates_blob_relationmetadata_for_old_indexmetadata() throws Exception {
        RelationName tblName = new RelationName("blob", "myblobs");
        IndexMetadata indexMetadata = IndexMetadata.builder(UUIDs.randomBase64UUID())
            .indexName(tblName.indexNameOrAlias())
            .settings(settings(Version.V_5_10_11))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        Metadata metadata = Metadata.builder()
            .put(indexMetadata, false)
            .build();

        Metadata upgraded = metadataUpgradeService.upgradeMetadata(metadata);
        RelationMetadata relation = upgraded.getRelation(tblName);
        assertThat(relation).isExactlyInstanceOf(RelationMetadata.BlobTable.class);
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
            String indexUUID = UUIDs.randomBase64UUID();
            builder.put(
                IndexMetadata.builder(indexUUID)
                    .settings(settings(Version.CURRENT))
                    .indexName(indexName)
                    .numberOfReplicas(randomIntBetween(0, 3))
                    .numberOfShards(randomIntBetween(1, 5))
            );
            if (withRelationMetadata) {
                builder.setTable(
                    IndexName.decode(indexName).toRelationName(),
                    List.of(),
                    builder.get(indexUUID).getSettings(),
                    null,
                    ColumnPolicy.STRICT,
                    null,
                    Map.of(),
                    List.of(),
                    List.of(),
                    IndexMetadata.State.OPEN,
                    List.of(indexUUID),
                    0
                );
            }
        }
        return builder.build();
    }
}

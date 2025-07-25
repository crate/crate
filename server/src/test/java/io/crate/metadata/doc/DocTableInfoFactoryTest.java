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

package io.crate.metadata.doc;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiLettersOfLength;
import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataUpgradeService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;


public class DocTableInfoFactoryTest extends ESTestCase {

    private NodeContext nodeCtx = createNodeContext();
    private MetadataUpgradeService metadataUpgradeService;

    private String randomSchema() {
        if (randomBoolean()) {
            return DocSchemaInfo.NAME;
        } else {
            return randomAsciiLettersOfLength(3);
        }
    }

    @Before
    public void setUpUpgradeService() throws Exception {
        metadataUpgradeService = new MetadataUpgradeService(
            nodeCtx,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            null
        );
    }

    @Test
    public void testNoTableInfoFromOrphanedPartition() throws Exception {
        String schemaName = randomSchema();
        PartitionName partitionName = new PartitionName(
            new RelationName(schemaName, "test"), Collections.singletonList("boo"));
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(partitionName.asIndexName())
            .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
            .numberOfReplicas(0)
            .numberOfShards(5)
            .putMapping(
                "{" +
                "  \"default\": {" +
                "    \"properties\":{" +
                "      \"id\": {" +
                "         \"type\": \"integer\"," +
                "         \"position\": 1," +
                "         \"index\": \"not_analyzed\"" +
                "      }" +
                "    }" +
                "  }" +
                "}");
        Metadata metadata = Metadata.builder()
                .put(indexMetadataBuilder)
                .build();

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        DocTableInfoFactory docTableInfoFactory = new DocTableInfoFactory(nodeCtx);

        assertThatThrownBy(() ->
                docTableInfoFactory.create(new RelationName(schemaName, "test"), state.metadata()))
            .isExactlyInstanceOf(RelationUnknown.class)
            .hasMessage(String.format(Locale.ENGLISH, "Relation '%s.test' unknown", schemaName));
    }

    @Test
    public void test_sets_created_version_based_on_oldest_partition() throws Exception {
        // Mitigation for https://github.com/crate/crate/pull/17178

        String schema = randomSchema();
        RelationName tbl = new RelationName(schema, "tbl");
        PartitionName p1 = new PartitionName(tbl, List.of("p1"));

        String templateName = PartitionName.templateName(schema, "tbl");
        String mapping = """
            {
                "default": {
                    "properties": {
                        "id": {
                            "type": "integer",
                            "position": 1,
                            "index": "not_analyzed",
                            "oid": 1
                        }
                    }
                }
            }
            """;
        IndexTemplateMetadata template = IndexTemplateMetadata.builder(templateName)
            .patterns(List.of(PartitionName.templatePrefix(schema, "tbl")))
            .settings(Settings.builder()
                .put("index.version.created", Version.V_5_9_6)
                .put("index.number_of_shards", 5)
                .build()
            )
            .putMapping(mapping)
            .putAlias(new AliasMetadata(tbl.indexNameOrAlias()))
            .build();
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(UUIDs.randomBase64UUID())
            .settings(Settings.builder().put("index.version.created", Version.V_5_7_5).build())
            .indexName(p1.asIndexName())
            .numberOfReplicas(0)
            .numberOfShards(5)
            .putMapping(
                mapping);
        Metadata metadata = Metadata.builder()
                .put(indexMetadataBuilder)
                .put(template)
                .build();
        metadata = metadataUpgradeService.upgradeMetadata(metadata);

        DocTableInfoFactory docTableInfoFactory = new DocTableInfoFactory(nodeCtx);
        DocTableInfo docTableInfo = docTableInfoFactory.create(tbl, metadata);
        assertThat(docTableInfo.versionCreated()).isEqualTo(Version.V_5_7_5);
    }

    @Test
    public void test_uses_5_4_0_as_version_if_mapping_has_no_oid() throws Exception {
        String schema = randomSchema();
        RelationName tbl = new RelationName(schema, "tbl");
        String templateName = PartitionName.templateName(schema, "tbl");
        String mapping = """
            {
                "default": {
                    "properties": {
                        "id": {
                            "type": "integer",
                            "position": 1,
                            "index": "not_analyzed"
                        }
                    }
                }
            }
            """;
        IndexTemplateMetadata template = IndexTemplateMetadata.builder(templateName)
            .patterns(List.of(PartitionName.templatePrefix(schema, "tbl")))
            .settings(Settings.builder()
                .put("index.version.created", Version.V_5_9_6)
                .put("index.number_of_shards", 5)
                .build()
            )
            .putMapping(mapping)
            .putAlias(new AliasMetadata(tbl.indexNameOrAlias()))
            .build();
        Metadata metadata = Metadata.builder()
                .put(template)
                .build();
        metadata = metadataUpgradeService.upgradeMetadata(metadata);

        DocTableInfoFactory docTableInfoFactory = new DocTableInfoFactory(nodeCtx);
        DocTableInfo docTableInfo = docTableInfoFactory.create(tbl, metadata);
        assertThat(docTableInfo.versionCreated()).isEqualTo(Version.V_5_4_0);
    }

    /**
     * Tests a regression were the IndexColumn was created with the wrong identifier,
     * the identifier was the column name using the copy_to instead of the index name.
     * The actual upgrade happens inside the {@link MetadataUpgradeService} when the metadata is upgraded,
     * but we want to ensure that the {@link DocTableInfoFactory} correctly parses the index column as well.
     */
    @Test
    public void test_old_index_column_using_copy_to_is_parsed_correctly() {
        String schema = randomSchema();
        RelationName tbl = new RelationName(schema, "tbl");
        String mapping = """
            {
              "default" : {
                "dynamic" : "strict",
                "_meta" : {
                  "indices" : {
                    "text_ft" : { }
                  },
                  "primary_keys" : [ "id" ]
                },
                "properties" : {
                  "col_timestamp" : {
                    "type" : "date",
                    "position" : 2,
                    "format" : "epoch_millis||strict_date_optional_time",
                    "ignore_timezone" : true
                  },
                  "id" : {
                    "type" : "keyword",
                    "position" : 1
                  },
                  "text" : {
                    "type" : "keyword",
                    "position" : 3,
                    "copy_to" : [ "text_ft" ]
                  },
                  "text_ft" : {
                    "type" : "text",
                    "position" : 4,
                    "analyzer" : "standard"
                  }
                }
              }
            }
            """;
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(tbl.indexNameOrAlias())
            .settings(Settings.builder().put("index.version.created", Version.V_5_0_0).build())
            .numberOfReplicas(0)
            .numberOfShards(5)
            .putMapping(mapping);

        Metadata metadata = Metadata.builder()
            .put(indexMetadataBuilder)
            .build();
        metadata = metadataUpgradeService.upgradeMetadata(metadata);

        DocTableInfoFactory docTableInfoFactory = new DocTableInfoFactory(nodeCtx);
        DocTableInfo docTableInfo = docTableInfoFactory.create(tbl, metadata);

        assertThat(docTableInfo.indexColumn(ColumnIdent.of("text_ft")))
            .isNotNull();
    }
}

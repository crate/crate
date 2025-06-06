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

package org.elasticsearch.action.admin.cluster.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.action.admin.cluster.state.TransportClusterState.buildResponse;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.sql.tree.ColumnPolicy;

public class TransportClusterStateTests extends ESTestCase {

    private final Logger logger = LogManager.getLogger(getClass());

    private ClusterState clusterState;


    @Before
    public void setup() throws Throwable {
        var relationName1 = new RelationName("doc", "t1");
        var indexUUID1 = UUIDs.randomBase64UUID();
        var indexUUID2 = UUIDs.randomBase64UUID();
        var relationName2 = new RelationName("doc", "t2");
        clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder()
                .persistentSettings(Settings.builder().put("setting1", "bar").build())
                .setTable(
                    relationName1,
                    List.of(),
                    settings(Version.CURRENT)
                        .put(SETTING_NUMBER_OF_SHARDS, 1)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0)
                        .build(),
                    null,
                    ColumnPolicy.STRICT,
                    null,
                    Map.of(),
                    List.of(),
                    List.of(),
                    IndexMetadata.State.OPEN,
                    List.of(indexUUID1),
                    0
                )
                .setTable(
                    relationName2,
                    List.of(),
                    settings(Version.CURRENT)
                        .put(SETTING_NUMBER_OF_SHARDS, 1)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0)
                        .build(),
                    null,
                    ColumnPolicy.STRICT,
                    null,
                    Map.of(),
                    List.of(),
                    List.of(ColumnIdent.of("parted")),
                    IndexMetadata.State.OPEN,
                    List.of(indexUUID2),
                    0
                )
                .put(IndexMetadata.builder(indexUUID1)
                        .settings(settings(Version.CURRENT)
                            .put(SETTING_INDEX_UUID, indexUUID1))
                        .indexName(relationName1.indexNameOrAlias())
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .build(),
                    true)
                .put(IndexMetadata.builder(indexUUID2)
                        .settings(settings(Version.CURRENT)
                            .put(SETTING_INDEX_UUID, indexUUID2))
                        .indexName(relationName2.indexNameOrAlias())
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .build(),
                    true)
                .put(IndexTemplateMetadata.builder(PartitionName.templateName(relationName2.schema(), relationName2.name()))
                    .patterns(List.of(PartitionName.templatePrefix(relationName2.schema(), relationName2.name())))
                    .putAlias(new AliasMetadata(new Alias(relationName2.indexNameOrAlias()).name()))
                    .settings(settings(Version.CURRENT))
                    .putMapping("{}")
                    .build())
                .build()
            )
            .build();
    }

    @Test
    public void test_response_contains_complete_metadata_if_no_relations_requested() {
        var request = new ClusterStateRequest();
        request.metadata(true);

        var response = buildResponse(request, clusterState, logger);

        List<RelationName> relationNames = List.of(
            new RelationName("doc", "t1"),
            new RelationName("doc", "t2")
        );
        for (var relationName : relationNames) {
            RelationMetadata.Table table = response.getState().metadata().getRelation(relationName);
            assertThat(table).isNotNull();
            for (String indexUUID : table.indexUUIDs()) {
                assertThat(response.getState().metadata().index(indexUUID)).isNotNull();
            }
            if (!table.partitionedBy().isEmpty()) {
                String templateName = PartitionName.templateName(table.name().schema(), table.name().name());
                assertThat(response.getState().metadata().templates().get(templateName)).isNotNull();
            }
        }
        assertThat(response.getState().metadata().persistentSettings().get("setting1")).isEqualTo("bar");
    }

    @Test
    public void test_response_contains_relations_only() {
        var relationName = new RelationName("doc", "t1");
        var request = new ClusterStateRequest();
        request.metadata(true);
        request.relationNames(List.of(relationName));

        var response = buildResponse(request, clusterState, logger);
        assertThat(response.getState().metadata().templates().size()).isZero();
        assertThat(response.getState().metadata().indices().size()).isEqualTo(1);

        RelationMetadata.Table table = response.getState().metadata().getRelation(relationName);
        assertThat(table).isNotNull();
        for (String indexUUID : table.indexUUIDs()) {
            assertThat(response.getState().metadata().index(indexUUID)).isNotNull();
        }
        assertThat(response.getState().metadata().persistentSettings().get("setting1")).isNull();
    }
}

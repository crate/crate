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

package io.crate.integrationtests;

import static io.crate.testing.Asserts.assertThat;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.concurrent.FutureActionListener;

public class GCDanglingArtifactsIntegrationTest extends IntegTestCase {

    @Test
    public void test_remove_dangling_indices() throws Exception {
        List<String> danglingIndexNames = List.of(
            "schema..partitioned.partitioned.ident",
            "schema.ttemp",
            ".blob_blobstemp");

        execute("CREATE TABLE schema.t(a int)");
        execute("INSERT INTO schema.t VALUES(10)");
        execute("CREATE TABLE schema.partitioned(a int, p int) PARTITIONED BY(p)");
        execute("INSERT INTO schema.partitioned(a, p) VALUES(11, 1)");
        execute("INSERT INTO schema.partitioned(a, p) VALUES(22, 2)");
        execute("REFRESH TABLE schema.t, schema.partitioned");
        execute("CREATE BLOB TABLE blobs");

        FutureActionListener<ClusterStateUpdateResponse> future = new FutureActionListener<>();
        cluster().getCurrentMasterNodeInstance(ClusterService.class)
            .submitStateUpdateTask("test-create-dangling-index", new ClusterStateUpdateTask(Priority.HIGH) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                    for (String danglingIndexName : danglingIndexNames) {
                        String indexUUID = UUIDs.randomBase64UUID();
                        IndexMetadata indexMetadata = IndexMetadata.builder(indexUUID)
                            .settings(Settings.builder()
                                .put(SETTING_INDEX_UUID, indexUUID)
                                .put(SETTING_NUMBER_OF_SHARDS, 2)
                                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                                .put(SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
                                .build())
                            .indexName(danglingIndexName)
                            .build();
                        mdBuilder.put(indexMetadata, false);
                    }
                    return ClusterState.builder(currentState).metadata(mdBuilder).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    future.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    future.onResponse(null);
                }
            });
        assertThat(future).succeedsWithin(10, TimeUnit.SECONDS);

        execute("alter cluster gc dangling artifacts");
        assertBusy(() -> {
            for (String danglingIndexName : danglingIndexNames) {
                assertThat(cluster().clusterService().state().metadata().index(danglingIndexName)).isNull();
            }
        });
        execute("SELECT * FROM schema.t");
        assertThat(response).hasRows("10");
        execute("SELECT * FROM schema.partitioned ORDER BY a");
        assertThat(response).hasRows("11| 1", "22| 2");
    }
}

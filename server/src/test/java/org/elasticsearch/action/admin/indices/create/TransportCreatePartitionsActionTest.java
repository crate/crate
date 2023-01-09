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

package org.elasticsearch.action.admin.indices.create;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import io.crate.exceptions.SQLExceptions;

public class TransportCreatePartitionsActionTest extends IntegTestCase {

    TransportCreatePartitionsAction action;

    @Before
    public void prepare() {
        action = internalCluster().getInstance(TransportCreatePartitionsAction.class, internalCluster().getMasterName());
    }

    @Test
    public void testCreateBulkIndicesSimple() throws Exception {
        List<String> indices = Arrays.asList("index1", "index2", "index3", "index4");

        PutIndexTemplateRequest request = new PutIndexTemplateRequest("*")
            .patterns(List.of("*"))
            .mapping(Map.of())
            .settings(Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
            );
        internalCluster().client().execute(PutIndexTemplateAction.INSTANCE, request).get();

        AcknowledgedResponse response = action.execute(
            new CreatePartitionsRequest(indices, UUID.randomUUID())
        ).get();
        assertThat(response.isAcknowledged(), is(true));

        Metadata indexMetadata = internalCluster().clusterService().state().metadata();
        for (String index : indices) {
            assertThat(indexMetadata.hasIndex(index), is(true));
        }
    }

    @Test
    public void testRoutingOfIndicesIsNotOverridden() throws Exception {
        PutIndexTemplateRequest templateRequest = new PutIndexTemplateRequest("*")
            .patterns(List.of("*"))
            .mapping(Map.of())
            .settings(Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
            );
        internalCluster().client().execute(PutIndexTemplateAction.INSTANCE, templateRequest).get();

        cluster().client().admin().indices()
            .create(new CreateIndexRequest("index_0")
                .settings(Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0))
            ).get();
        ensureYellow("index_0");

        CreatePartitionsRequest request = new CreatePartitionsRequest(
            Arrays.asList("index_0", "index_1"),
            UUID.randomUUID());

        CompletableFuture<ClusterStateUpdateResponse> response = new CompletableFuture<>();

        action.createIndices(request, new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                response.complete(clusterStateUpdateResponse);
            }

            @Override
            public void onFailure(Exception e) {
                response.completeExceptionally(e);
            }
        });

        assertThat("Create indices action did not lead to a new cluster state",
            response.get(5, TimeUnit.SECONDS).isAcknowledged(), is(true));

        ClusterState currentState = internalCluster().clusterService().state();
        ImmutableOpenIntMap<IndexShardRoutingTable> newRouting = currentState.routingTable().indicesRouting().get("index_0").getShards();
        assertTrue("[index_0][0] must be started already", newRouting.get(0).primaryShard().started());
    }

    @Test
    public void testCreateBulkIndicesIgnoreExistingSame() throws Exception {
        PutIndexTemplateRequest templateRequest = new PutIndexTemplateRequest("*")
            .patterns(List.of("*"))
            .mapping(Map.of())
            .settings(Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
            );
        internalCluster().client().execute(PutIndexTemplateAction.INSTANCE, templateRequest).get();
        List<String> indices = Arrays.asList("index1", "index2", "index3", "index1");
        AcknowledgedResponse response = action.execute(
            new CreatePartitionsRequest(indices, UUID.randomUUID())
        ).get();
        assertThat(response.isAcknowledged(), is(true));
        Metadata indexMetadata = internalCluster().clusterService().state().metadata();
        for (String index : indices) {
            assertThat(indexMetadata.hasIndex(index), is(true));
        }
        AcknowledgedResponse response2 = action.execute(
            new CreatePartitionsRequest(indices, UUID.randomUUID())
        ).get();
        assertThat(response2.isAcknowledged(), is(true));
    }

    @Test
    public void testEmpty() throws Exception {
        AcknowledgedResponse response = action.execute(
            new CreatePartitionsRequest(List.of(), UUID.randomUUID())).get();
        assertThat(response.isAcknowledged(), is(true));
    }

    @Test
    public void testCreateInvalidName() throws Exception {
        CreatePartitionsRequest createPartitionsRequest = new CreatePartitionsRequest(Arrays.asList("valid", "invalid/#haha"), UUID.randomUUID());
        Assertions.assertThrows(
            InvalidIndexNameException.class,
            () -> {
                try {
                    action.execute(createPartitionsRequest).get();
                } catch (Exception e) {
                    throw SQLExceptions.unwrap(e);
                }
            },
            "Invalid index name [invalid/#haha], must not contain the following characters [ , \", *, \\, <, |, ,, >, /, ?]");
        // if one name is invalid no index is created
        assertThat(internalCluster().clusterService().state().metadata().hasIndex("valid"), is(false));
    }
}

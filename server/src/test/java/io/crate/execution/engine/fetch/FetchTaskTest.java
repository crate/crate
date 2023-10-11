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

package io.crate.execution.engine.fetch;

import static io.crate.testing.TestingHelpers.createReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntIndexedContainer;

import io.crate.execution.dsl.phases.FetchPhase;
import io.crate.execution.jobs.SharedShardContexts;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.Schemas;
import io.crate.planner.fetch.IndexBaseBuilder;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.DataTypes;

public class FetchTaskTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testGetIndexServiceForInvalidReaderId() throws Exception {
        final FetchTask context = new FetchTask(
            UUID.randomUUID(),
            new FetchPhase(
                1,
                null,
                new TreeMap<>(),
                new HashMap<>(),
                List.of()),
            0,
            "dummy",
            new SharedShardContexts(mock(IndicesService.class), UnaryOperator.identity()),
            clusterService.state().metadata(),
            relationName -> null,
            Collections.emptyList());

        assertThatThrownBy(() -> context.indexService(10))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Reader with id 10 not found");
    }

    @Test
    public void testSearcherIsAcquiredForShard() throws Exception {
        IntArrayList shards = IntArrayList.from(1, 2);
        Routing routing = new Routing(Map.of("dummy", Map.of("i1", shards)));
        IndexBaseBuilder ibb = new IndexBaseBuilder();
        ibb.allocate("i1", shards);

        Map<RelationName, Collection<String>> tableIndices = new HashMap<>();
        tableIndices.put(new RelationName(Schemas.DOC_SCHEMA_NAME, "i1"), List.of("i1"));

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("i1")
                     .settings(Settings.builder()
                                   .put(SETTING_NUMBER_OF_SHARDS, 1)
                                   .put(SETTING_NUMBER_OF_REPLICAS, 0)
                                   .put(SETTING_VERSION_CREATED, Version.CURRENT))
                     .build(), true)
            .build();
        IndicesService indicesService = mock(IndicesService.class, RETURNS_MOCKS);
        AtomicBoolean calledRefreshReaders = new AtomicBoolean(false);
        SharedShardContexts sharedShardContexts = new SharedShardContexts(indicesService, UnaryOperator.identity()) {
            @Override
            public CompletableFuture<Void> maybeRefreshReaders(Metadata metadata,
                                                               Map<String, IntIndexedContainer> shardsByIndex,
                                                               Map<String, Integer> bases) {
                calledRefreshReaders.set(true);
                return CompletableFuture.completedFuture(null);
            }
        };
        final FetchTask context = new FetchTask(
            UUID.randomUUID(),
            new FetchPhase(
                1,
                null,
                ibb.build(),
                tableIndices,
                List.of(createReference("i1", new ColumnIdent("x"), DataTypes.STRING))),
            0,
            "dummy",
            sharedShardContexts,
            metadata,
            relationName -> null,
            List.of(routing));

        context.start().get(5, TimeUnit.SECONDS);
        assertThat(calledRefreshReaders.get()).isTrue();

        assertThat(context.searcher(1)).isNotNull();
        assertThat(context.searcher(2)).isNotNull();
    }
}

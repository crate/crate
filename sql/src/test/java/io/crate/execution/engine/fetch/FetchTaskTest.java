/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.fetch;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntIndexedContainer;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.execution.dsl.phases.FetchPhase;
import io.crate.execution.jobs.SharedShardContexts;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.Schemas;
import io.crate.planner.fetch.IndexBaseBuilder;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.UnaryOperator;

import static io.crate.testing.TestingHelpers.createReference;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;

public class FetchTaskTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testGetIndexServiceForInvalidReaderId() throws Exception {
        final FetchTask context = new FetchTask(
            new FetchPhase(
                1,
                null,
                new TreeMap<>(),
                HashMultimap.create(),
                ImmutableList.of()),
            "dummy",
            new SharedShardContexts(mock(IndicesService.class), UnaryOperator.identity()),
            clusterService.state().getMetaData(),
            Collections.emptyList());

        expectedException.expect(IllegalArgumentException.class);
        context.indexService(10);
    }

    @Test
    public void testSearcherIsAcquiredForShard() throws Exception {
        IntArrayList shards = IntArrayList.from(1, 2);
        Routing routing = new Routing(
            TreeMapBuilder.<String, Map<String, IntIndexedContainer>>newMapBuilder().put(
                "dummy",
                TreeMapBuilder.<String, IntIndexedContainer>newMapBuilder().put("i1", shards).map()).map());

        IndexBaseBuilder ibb = new IndexBaseBuilder();
        ibb.allocate("i1", shards);

        HashMultimap<RelationName, String> tableIndices = HashMultimap.create();
        tableIndices.put(new RelationName(Schemas.DOC_SCHEMA_NAME, "i1"), "i1");

        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder("i1")
                .settings(Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(SETTING_VERSION_CREATED, Version.CURRENT))
                .build(), true)
            .build();
        final FetchTask context = new FetchTask(
            new FetchPhase(
                1,
                null,
                ibb.build(),
                tableIndices,
                ImmutableList.of(createReference("i1", new ColumnIdent("x"), DataTypes.STRING))),
            "dummy",
            new SharedShardContexts(mock(IndicesService.class, RETURNS_MOCKS), UnaryOperator.identity()),
            metaData,
            ImmutableList.of(routing));

        context.start();

        assertThat(context.searcher(1), Matchers.notNullValue());
        assertThat(context.searcher(2), Matchers.notNullValue());
    }
}

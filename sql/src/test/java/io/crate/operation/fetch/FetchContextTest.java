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

package io.crate.operation.fetch;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import io.crate.action.job.SharedShardContexts;
import io.crate.analyze.symbol.Reference;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.planner.fetch.IndexBaseVisitor;
import io.crate.planner.node.fetch.FetchPhase;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.indices.IndicesService;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;

public class FetchContextTest extends CrateUnitTest {

    @Test
    public void testGetIndexServiceForInvalidReaderId() throws Exception {


        final FetchContext context = new FetchContext(
                new FetchPhase(
                        null,
                        1,
                        null,
                        new TreeMap<String, Integer>(),
                        HashMultimap.<TableIdent, String>create(),
                        ImmutableList.<Reference>of()),
                "dummy",
                new SharedShardContexts(mock(IndicesService.class)),
                Collections.<Routing>emptyList());

        expectedException.expect(IllegalArgumentException.class);
        context.indexService(10);
    }

    @Test
    public void testSearcherIsAcquiredForShard() throws Exception {
        Routing routing = new Routing(
                TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder().put(
                        "dummy",
                        TreeMapBuilder.<String, List<Integer>>newMapBuilder().put("i1", ImmutableList.of(1, 2)).map()).map());

        IndexBaseVisitor ibv = new IndexBaseVisitor();
        routing.walkLocations(ibv);

        HashMultimap<TableIdent, String> tableIndices = HashMultimap.create();

        tableIndices.put(new TableIdent(null, "i1"), "i1");

        final FetchContext context = new FetchContext(
                new FetchPhase(
                        null,
                        1,
                        null,
                        ibv.build(),
                        tableIndices,
                        ImmutableList.<Reference>of()),
                "dummy",
                new SharedShardContexts(mock(IndicesService.class, RETURNS_MOCKS)),
                ImmutableList.of(routing));

        context.prepare();

        assertThat(context.searcher(1), Matchers.notNullValue());
        assertThat(context.searcher(2), Matchers.notNullValue());
    }
}
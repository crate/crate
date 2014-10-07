/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.task;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.QueryResult;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.Routing;
import io.crate.operation.collect.CollectOperation;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.QueryAndFetchNode;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class LocalCollectTaskTest {

    public final static Routing CLUSTER_ROUTING = new Routing();
    private UUID testJobId = UUID.randomUUID();

    @Test
    public void testCollectTask() throws Exception {

        final QueryAndFetchNode queryAndFetchNode = new QueryAndFetchNode(
                "ei-die",
                CLUSTER_ROUTING,
                ImmutableList.<Symbol>of(Literal.newLiteral(4)),
                ImmutableList.<Symbol>of(Literal.newLiteral(4)),
                null, null, null, null, null, null, null, null, RowGranularity.CLUSTER, null);
        queryAndFetchNode.jobId(testJobId);


        CollectOperation<Object[][]> collectOperation = new CollectOperation<Object[][]>() {
            @Override
            public ListenableFuture<Object[][]> collect(QueryAndFetchNode cn) {
                assertEquals(cn, queryAndFetchNode);
                SettableFuture<Object[][]> result = SettableFuture.create();
                result.set(new Object[][]{{1}});
                return result;
            }
        };
        LocalCollectTask collectTask = new LocalCollectTask(
                queryAndFetchNode,
                mock(ClusterService.class),
                ImmutableSettings.EMPTY,
                mock(TransportActionProvider.class),
                collectOperation,
                mock(ReferenceResolver.class),
                mock(Functions.class),
                new ThreadPool(getClass().getName())
        );
        collectTask.start();
        List<ListenableFuture<QueryResult>> results = collectTask.result();
        assertThat(results.size(), is(1));

        ListenableFuture<QueryResult> result = results.get(0);
        assertThat(result.get().rows(), is(new Object[][]{{1}}));
    }
}

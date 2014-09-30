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
import io.crate.metadata.Routing;
import io.crate.operation.collect.CollectOperation;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class LocalCollectTaskTest {

    public final static Routing CLUSTER_ROUTING = new Routing();
    private UUID testJobId = UUID.randomUUID();

    @Test
    public void testCollectTask() throws Exception {

        final CollectNode collectNode = new CollectNode("ei-die", CLUSTER_ROUTING);
        collectNode.maxRowGranularity(RowGranularity.CLUSTER);
        collectNode.jobId(testJobId);
        collectNode.toCollect(ImmutableList.<Symbol>of(Literal.newLiteral(4)));


        CollectOperation collectOperation = new CollectOperation() {
            @Override
            public ListenableFuture collect(CollectNode cn) {
                assertEquals(cn, collectNode);
                SettableFuture<Object[][]> result = SettableFuture.create();
                result.set(new Object[][]{{1}});
                return result;
            }
        };
        LocalCollectTask collectTask = new LocalCollectTask(UUID.randomUUID(),
                collectOperation,
                collectNode);
        collectTask.start();
        List<ListenableFuture<QueryResult>> results = collectTask.result();
        assertThat(results.size(), is(1));

        ListenableFuture<QueryResult> result = results.get(0);
        assertThat(result.get().rows(), is(new Object[][]{{1}}));
    }
}

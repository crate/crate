/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.core.collections.Bucket;
import io.crate.executor.Job;
import io.crate.executor.TaskResult;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.Planner;
import io.crate.planner.RowGranularity;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.CollectAndMerge;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.Symbols;
import io.crate.testing.TestingHelpers;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.core.Is.is;

public class TransportExecutorJoinTest extends BaseTransportExecutorTest {

    @Before
    public void setUpTestTables() throws Exception {
        execute("create table colors (id int, name string)");
        execute("create table sizes (id int, name string)");
        execute("create table gender (id int, name string)");
        ensureYellow();

        execute("insert into colors (id, name) values (?, ?)", new Object[][]{
                new Object[]{1, "red"},
                new Object[]{2, "blue"},
                new Object[]{3, "green"}
        });
        execute("insert into sizes (id, name) values (?, ?)", new Object[][]{
                new Object[]{1, "small"},
                new Object[]{2, "large"},
        });
        execute("insert into gender (id, name) values (?, ?)", new Object[][]{
                new Object[]{1, "female"},
                new Object[]{2, "male"},
        });
        execute("refresh table colors, sizes, gender");
    }

    @Test
    public void testNonDistributedNestedLoopWithOrderedQAF() throws Exception {
        // select colors.id, colors.name,
        //        sizes.id, sizes.name,
        //        gender.id, gender.name
        //   from colors, sizes, gender
        //   order by colors.id, sizes.id, gender.name;

        DocTableInfo colors = docSchemaInfo.getTableInfo("colors");
        DocTableInfo sizes = docSchemaInfo.getTableInfo("sizes");
        DocTableInfo gender = docSchemaInfo.getTableInfo("gender");

        Reference innerLeftIdRef = new Reference(colors.getReferenceInfo(new ColumnIdent("id")));
        Reference innerLeftNameRef = new Reference(colors.getReferenceInfo(new ColumnIdent("name")));

        Reference innerRightIdRef = new Reference(sizes.getReferenceInfo(new ColumnIdent("id")));
        Reference innerRightNameRef = new Reference(sizes.getReferenceInfo(new ColumnIdent("name")));

        Reference outerRightIdRef = new Reference(gender.getReferenceInfo(new ColumnIdent("id")));
        Reference outerRightNameRef = new Reference(gender.getReferenceInfo(new ColumnIdent("name")));

        List<Symbol> innerLeftCollectSymbols = Lists.<Symbol>newArrayList(innerLeftIdRef, innerLeftNameRef);
        List<Symbol> innerRightCollectSymbols = Lists.<Symbol>newArrayList(innerRightIdRef, innerRightNameRef);
        List<Symbol> outerRightCollectSymbols = Lists.<Symbol>newArrayList(outerRightIdRef, outerRightNameRef);
        List<Symbol> outputSymbols = Lists.newArrayList(innerLeftCollectSymbols);
        outputSymbols.addAll(innerRightCollectSymbols);
        outputSymbols.addAll(outerRightCollectSymbols);

        UUID jobId = UUID.randomUUID();
        Planner.Context plannerContext = new Planner.Context(clusterService(), jobId, null);
        String localNodeId = clusterService.localNode().id();
        Set<String> localExecutionNode = Sets.newHashSet(localNodeId);

        // outer right relation
        OrderBy outerRightOrderBy = new OrderBy(ImmutableList.<Symbol>of(outerRightNameRef),
                new boolean[]{true}, new Boolean[]{true});
        CollectPhase outerRightCollectPhase = new CollectPhase(
                jobId,
                plannerContext.nextExecutionPhaseId(),
                "collect",
                gender.getRouting(WhereClause.MATCH_ALL, null),
                RowGranularity.DOC,
                outerRightCollectSymbols,
                ImmutableList.<Projection>of(),
                WhereClause.MATCH_ALL,
                DistributionInfo.DEFAULT_BROADCAST
        );
        outerRightCollectPhase.orderBy(outerRightOrderBy);

        // inner left relation
        OrderBy innerLeftOrderBy = new OrderBy(ImmutableList.<Symbol>of(innerLeftIdRef),
                new boolean[]{false}, new Boolean[]{false});
        CollectPhase innerLeftCollectPhase = new CollectPhase(
                jobId,
                plannerContext.nextExecutionPhaseId(),
                "collect",
                colors.getRouting(WhereClause.MATCH_ALL, null),
                RowGranularity.DOC,
                innerLeftCollectSymbols,
                ImmutableList.<Projection>of(),
                WhereClause.MATCH_ALL,
                DistributionInfo.DEFAULT_BROADCAST
        );
        innerLeftCollectPhase.orderBy(innerLeftOrderBy);

        // inner right relation
        OrderBy innerRightOrderBy = new OrderBy(ImmutableList.<Symbol>of(innerRightIdRef),
                new boolean[]{false}, new Boolean[]{false});
        CollectPhase innerRightCollectPhase = new CollectPhase(
                jobId,
                plannerContext.nextExecutionPhaseId(),
                "collect",
                sizes.getRouting(WhereClause.MATCH_ALL, null),
                RowGranularity.DOC,
                innerRightCollectSymbols,
                ImmutableList.<Projection>of(),
                WhereClause.MATCH_ALL,
                DistributionInfo.DEFAULT_BROADCAST
        );
        innerRightCollectPhase.orderBy(innerRightOrderBy);

        // inner nested loop node
        MergePhase innerLeftMergePhase = MergePhase.sortedMerge(
                jobId,
                plannerContext.nextExecutionPhaseId(),
                innerLeftOrderBy,
                innerLeftCollectSymbols,
                innerLeftOrderBy.orderBySymbols(),
                ImmutableList.<Projection>of(),
                innerLeftCollectPhase
        );
        MergePhase innerRightMergePhase = MergePhase.sortedMerge(
                jobId,
                plannerContext.nextExecutionPhaseId(),
                innerRightOrderBy,
                innerRightCollectSymbols,
                innerRightOrderBy.orderBySymbols(),
                ImmutableList.<Projection>of(),
                innerRightCollectPhase
        );
        NestedLoopPhase innerNestedLoopPhase = new NestedLoopPhase(
                jobId,
                plannerContext.nextExecutionPhaseId(),
                "nested-loop",
                ImmutableList.<Projection>of(),
                innerLeftMergePhase,
                innerLeftMergePhase.outputTypes(),
                innerRightMergePhase,
                innerRightMergePhase.outputTypes(),
                localExecutionNode
        );

        PlannedAnalyzedRelation innerPlan = new NestedLoop(
                jobId,
                new CollectAndMerge(innerLeftCollectPhase, null, jobId),
                new CollectAndMerge(innerRightCollectPhase, null, jobId),
                innerNestedLoopPhase,
                null,
                false);

        // outer nested loop node
        List<Symbol> outerLeftCollectSymbols = Lists.newArrayList(innerLeftCollectSymbols);
        outerLeftCollectSymbols.addAll(innerRightCollectSymbols);

        MergePhase outerRightMergePhase = MergePhase.sortedMerge(
                jobId,
                plannerContext.nextExecutionPhaseId(),
                outerRightOrderBy,
                outerRightCollectSymbols,
                outerRightOrderBy.orderBySymbols(),
                ImmutableList.<Projection>of(),
                outerRightCollectPhase
        );

        NestedLoopPhase outerNestedLoopPhase = new NestedLoopPhase(
                jobId,
                plannerContext.nextExecutionPhaseId(),
                "nested-loop",
                ImmutableList.<Projection>of(),
                null,
                Symbols.extractTypes(outerLeftCollectSymbols),
                outerRightMergePhase,
                outerRightMergePhase.outputTypes(),
                localExecutionNode
                );

        // nested loop plan
        NestedLoop nestedLoopPlan = new NestedLoop(
                jobId,
                innerPlan,
                new CollectAndMerge(outerRightCollectPhase, null, jobId),
                outerNestedLoopPhase,
                null,
                false
        );

        Job job = executor.newJob(nestedLoopPlan);
        assertThat(job.tasks().size(), is(1));
        List<? extends ListenableFuture<TaskResult>> result = executor.execute(job);
        Bucket rows = result.get(0).get().rows();
        assertThat(rows.size(), is(12));

        assertThat(TestingHelpers.printedTable(rows), is("" +
                "1| red| 1| small| 2| male\n" +
                "1| red| 1| small| 1| female\n" +
                "1| red| 2| large| 2| male\n" +
                "1| red| 2| large| 1| female\n" +
                "2| blue| 1| small| 2| male\n" +
                "2| blue| 1| small| 1| female\n" +
                "2| blue| 2| large| 2| male\n" +
                "2| blue| 2| large| 1| female\n" +
                "3| green| 1| small| 2| male\n" +
                "3| green| 1| small| 1| female\n" +
                "3| green| 2| large| 2| male\n" +
                "3| green| 2| large| 1| female\n"));
    }
}
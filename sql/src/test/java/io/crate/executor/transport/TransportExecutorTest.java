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

package io.crate.executor.transport;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.analyze.WhereClause;
import io.crate.core.collections.Bucket;
import io.crate.executor.Job;
import io.crate.executor.Task;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.task.KillTask;
import io.crate.executor.transport.task.elasticsearch.ESDeleteByQueryTask;
import io.crate.executor.transport.task.elasticsearch.ESGetTask;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.TableIdent;
import io.crate.operation.operator.EqOperator;
import io.crate.planner.IterablePlan;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dml.ESDeleteByQueryNode;
import io.crate.planner.node.dql.ESGetNode;
import io.crate.planner.node.management.KillPlan;
import io.crate.planner.symbol.DynamicReference;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.crate.testing.TestingHelpers.isRow;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;

public class TransportExecutorTest extends BaseTransportExecutorTest {

    @Test
    public void testESGetTask() throws Exception {
        setup.setUpCharacters();

        // create plan
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        Planner.Context ctx = newPlannerContext();
        ESGetNode node = newGetNode("characters", outputs, "2", ctx.nextExecutionPhaseId());
        Plan plan = new IterablePlan(ctx.jobId(), node);
        Job job = executor.newJob(plan);

        // validate tasks
        assertThat(job.tasks().size(), is(1));
        Task task = job.tasks().get(0);
        assertThat(task, instanceOf(ESGetTask.class));

        // execute and validate results
        List<? extends ListenableFuture<TaskResult>> result = executor.execute(job);
        Bucket rows = result.get(0).get().rows();
        assertThat(rows, contains(isRow(2, "Ford")));
    }

    @Test
    public void testESGetTaskWithDynamicReference() throws Exception {
        setup.setUpCharacters();

        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, new DynamicReference(
                new ReferenceIdent(new TableIdent(null, "characters"), "foo"), RowGranularity.DOC));
        Planner.Context ctx = newPlannerContext();
        ESGetNode node = newGetNode("characters", outputs, "2", ctx.nextExecutionPhaseId());
        Plan plan = new IterablePlan(ctx.jobId(), node);
        Job job = executor.newJob(plan);
        List<? extends ListenableFuture<TaskResult>> result = executor.execute(job);
        Bucket rows = result.get(0).get().rows();
        assertThat(rows, contains(isRow(2, null)));
    }

    @Test
    public void testESMultiGet() throws Exception {
        setup.setUpCharacters();
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        Planner.Context ctx = newPlannerContext();
        ESGetNode node = newGetNode("characters", outputs, asList("1", "2"), ctx.nextExecutionPhaseId());
        Plan plan = new IterablePlan(ctx.jobId(), node);
        Job job = executor.newJob(plan);
        List<? extends ListenableFuture<TaskResult>> result = executor.execute(job);
        Bucket objects = result.get(0).get().rows();

        assertThat(objects.size(), is(2));
    }

    @Test
    public void testESDeleteByQueryTask() throws Exception {
        setup.setUpCharacters();

        Function whereClause = new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING)),
                DataTypes.BOOLEAN),
                Arrays.asList(idRef, Literal.newLiteral(2)));

        ESDeleteByQueryNode node = new ESDeleteByQueryNode(
                1,
                ImmutableList.of(new String[]{"characters"}),
                ImmutableList.of(new WhereClause(whereClause)));
        Plan plan = new IterablePlan(UUID.randomUUID(), node);
        Job job = executor.newJob(plan);
        ESDeleteByQueryTask task = (ESDeleteByQueryTask) job.tasks().get(0);

        task.start();
        TaskResult taskResult = task.result().get(0).get(2, TimeUnit.SECONDS);
        Bucket rows = taskResult.rows();
        assertThat(rows, contains(isRow(-1L)));

        // verify deletion
        execute("select * from characters where id = 2");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testKillTask() throws Exception {
        Job job = executor.newJob(new KillPlan(UUID.randomUUID()));
        assertThat(job.tasks(), hasSize(1));
        assertThat(job.tasks().get(0), instanceOf(KillTask.class));

        List<? extends ListenableFuture<TaskResult>> results = executor.execute(job);
        assertThat(results, hasSize(1));
        results.get(0).get();
    }

    protected Planner.Context newPlannerContext() {
        return new Planner.Context(clusterService(), UUID.randomUUID(), null);
    }
}

/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.statement;

import com.google.common.annotations.VisibleForTesting;
import io.crate.analyze.CopyToAnalyzedStatement;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.dsl.projection.MergeCountProjection;
import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.NodeOperationTreeGenerator;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.consumer.FetchMode;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.planner.operators.SubQueryResults;

import java.util.Collections;
import java.util.Set;

public final class CopyStatementPlanner {

    private CopyStatementPlanner() {
    }

    public static Plan planCopyTo(CopyToAnalyzedStatement statement,
                                  LogicalPlanner logicalPlanner,
                                  SubqueryPlanner subqueryPlanner) {
        return new CopyTo(statement, logicalPlanner, subqueryPlanner);
    }

    static class CopyTo implements Plan {

        @VisibleForTesting
        final CopyToAnalyzedStatement copyTo;

        @VisibleForTesting
        final LogicalPlanner logicalPlanner;

        @VisibleForTesting
        final SubqueryPlanner subqueryPlanner;

        CopyTo(CopyToAnalyzedStatement copyTo, LogicalPlanner logicalPlanner, SubqueryPlanner subqueryPlanner) {
            this.copyTo = copyTo;
            this.logicalPlanner = logicalPlanner;
            this.subqueryPlanner = subqueryPlanner;
        }

        @Override
        public StatementType type() {
            return StatementType.COPY;
        }

        @Override
        public void executeOrFail(DependencyCarrier executor,
                                  PlannerContext plannerContext,
                                  RowConsumer consumer,
                                  Row params,
                                  SubQueryResults subQueryResults) {
            ExecutionPlan executionPlan = planCopyToExecution(
                copyTo, plannerContext, logicalPlanner, subqueryPlanner, executor.projectionBuilder(), params);
            NodeOperationTree nodeOpTree = NodeOperationTreeGenerator.fromPlan(executionPlan, executor.localNodeId());
            executor.phasesTaskFactory()
                .create(plannerContext.jobId(), Collections.singletonList(nodeOpTree))
                .execute(consumer, plannerContext.transactionContext());
        }
    }

    @VisibleForTesting
    static ExecutionPlan planCopyToExecution(CopyToAnalyzedStatement statement,
                                             PlannerContext context,
                                             LogicalPlanner logicalPlanner,
                                             SubqueryPlanner subqueryPlanner,
                                             ProjectionBuilder projectionBuilder,
                                             Row params) {
        WriterProjection.OutputFormat outputFormat = statement.outputFormat();
        if (outputFormat == null) {
            outputFormat = statement.columnsDefined() ?
                WriterProjection.OutputFormat.JSON_ARRAY : WriterProjection.OutputFormat.JSON_OBJECT;
        }

        WriterProjection projection = ProjectionBuilder.writerProjection(
            statement.relation().outputs(),
            statement.uri(),
            statement.compressionType(),
            statement.overwrites(),
            statement.outputNames(),
            outputFormat);

        LogicalPlan logicalPlan = logicalPlanner.normalizeAndPlan(
            statement.relation(), context, subqueryPlanner, FetchMode.NEVER_CLEAR, Set.of());
        ExecutionPlan executionPlan = logicalPlan.build(
            context, projectionBuilder, 0, 0, null, null, params, SubQueryResults.EMPTY);
        executionPlan.addProjection(projection);
        return Merge.ensureOnHandler(executionPlan, context, Collections.singletonList(MergeCountProjection.INSTANCE));
    }
}

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

package io.crate.planner.statement;

import static io.crate.analyze.CopyStatementSettings.COMPRESSION_SETTING;
import static io.crate.analyze.CopyStatementSettings.OUTPUT_FORMAT_SETTING;
import static io.crate.analyze.CopyStatementSettings.settingAsEnum;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.elasticsearch.common.settings.Settings;

import io.crate.analyze.AnalyzedCopyTo;
import io.crate.analyze.BoundCopyTo;
import io.crate.analyze.PartitionPropertiesAnalyzer;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.dsl.projection.MergeCountProjection;
import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.JobLauncher;
import io.crate.execution.engine.NodeOperationTreeGenerator;
import io.crate.expression.scalar.cast.CastMode;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.DocReferences;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.planner.optimizer.rule.OptimizeCollectWhereClauseAccess;
import io.crate.sql.tree.Assignment;
import io.crate.statistics.TableStats;
import io.crate.types.DataTypes;

public final class CopyToPlan implements Plan {

    private final AnalyzedCopyTo copyTo;
    private final TableStats tableStats;

    public CopyToPlan(AnalyzedCopyTo copyTo, TableStats tableStats) {
        this.copyTo = copyTo;
        this.tableStats = tableStats;
    }

    @VisibleForTesting
    AnalyzedCopyTo copyTo() {
        return copyTo;
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

        var boundedCopyTo = bind(
            copyTo,
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            params,
            subQueryResults);

        ExecutionPlan executionPlan = planCopyToExecution(
            executor,
            boundedCopyTo,
            plannerContext,
            plannerContext.planStats(),
            executor.projectionBuilder(),
            params,
            subQueryResults
        );

        NodeOperationTree nodeOpTree = NodeOperationTreeGenerator
            .fromPlan(executionPlan, executor.localNodeId());

        JobLauncher jobLauncher = executor.phasesTaskFactory()
            .create(plannerContext.jobId(), List.of(nodeOpTree));

        jobLauncher.execute(
            consumer,
            plannerContext.transactionContext(),
            boundedCopyTo.withClauseOptions().getAsBoolean("wait_for_completion", true));
    }

    @VisibleForTesting
    static ExecutionPlan planCopyToExecution(DependencyCarrier executor,
                                             BoundCopyTo boundedCopyTo,
                                             PlannerContext context,
                                             PlanStats planStats,
                                             ProjectionBuilder projectionBuilder,
                                             Row params,
                                             SubQueryResults subQueryResults) {

        WriterProjection.OutputFormat outputFormat = boundedCopyTo.outputFormat();
        if (outputFormat == null) {
            outputFormat = boundedCopyTo.columnsDefined() ?
                WriterProjection.OutputFormat.JSON_ARRAY : WriterProjection.OutputFormat.JSON_OBJECT;
        }

        WriterProjection projection = ProjectionBuilder.writerProjection(
            boundedCopyTo.outputs(),
            boundedCopyTo.uri(),
            boundedCopyTo.compressionType(),
            boundedCopyTo.overwrites(),
            boundedCopyTo.outputNames(),
            outputFormat,
            boundedCopyTo.withClauseOptions());

        LogicalPlan collect = new Collect(
            new DocTableRelation(boundedCopyTo.table()),
            boundedCopyTo.outputs(),
            boundedCopyTo.whereClause()
        );
        LogicalPlan source = optimizeCollect(context, planStats, collect);
        ExecutionPlan executionPlan = source.build(
            executor, context, Set.of(), projectionBuilder, 0, 0, null, null, params, SubQueryResults.EMPTY);
        executionPlan.addProjection(projection);

        return Merge.ensureOnHandler(
            executionPlan,
            context,
            List.of(MergeCountProjection.INSTANCE));
    }

    private static LogicalPlan optimizeCollect(PlannerContext context, PlanStats planStats, LogicalPlan collect) {
        OptimizeCollectWhereClauseAccess rewriteCollectToGet = new OptimizeCollectWhereClauseAccess();
        Match<Collect> match = rewriteCollectToGet.pattern().accept(collect, Captures.empty());
        if (match.isPresent()) {
            LogicalPlan plan = rewriteCollectToGet.apply(match.value(),
                                                         match.captures(),
                                                         planStats,
                                                         context.transactionContext(),
                                                         context.nodeContext(),
                                                         Function.identity());
            return plan == null ? collect : plan;
        }
        return collect;
    }

    @VisibleForTesting
    public static BoundCopyTo bind(AnalyzedCopyTo copyTo,
                                   CoordinatorTxnCtx txnCtx,
                                   NodeContext nodeCtx,
                                   Row parameters,
                                   SubQueryResults subQueryResults) {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            txnCtx,
            nodeCtx,
            x,
            parameters,
            subQueryResults
        );
        DocTableInfo table = (DocTableInfo) copyTo.tableInfo();

        List<String> partitions = resolvePartitions(
            Lists2.map(copyTo.table().partitionProperties(), x -> x.map(eval)),
            table
        );

        List<Symbol> outputs = new ArrayList<>();
        Map<ColumnIdent, Symbol> overwrites = null;
        boolean columnsDefined = false;
        final List<String> outputNames = new ArrayList<>(copyTo.columns().size());
        if (!copyTo.columns().isEmpty()) {
            // TODO: remove outputNames?
            for (Symbol symbol : copyTo.columns()) {
                assert symbol instanceof Reference : "Only references are expected here";
                RefVisitor.visitRefs(symbol, r -> outputNames.add(r.column().sqlFqn()));
                outputs.add(DocReferences.toSourceLookup(symbol));
            }
            columnsDefined = true;
        } else {
            Symbol toCollect;
            if (table.isPartitioned() && partitions.isEmpty()) {
                // table is partitioned, insert partitioned columns into the output
                overwrites = new HashMap<>();
                for (Reference reference : table.partitionedByColumns()) {
                    if (!(reference instanceof GeneratedReference)) {
                        overwrites.put(reference.column(), reference);
                    }
                }
                if (overwrites.size() > 0) {
                    toCollect = table.getReference(DocSysColumns.DOC);
                } else {
                    var docRef = table.getReference(DocSysColumns.DOC);
                    assert docRef != null : "_doc reference must be resolvable";
                    toCollect = docRef.cast(DataTypes.STRING, CastMode.EXPLICIT);
                }
            } else {
                var docRef = table.getReference(DocSysColumns.DOC);
                assert docRef != null : "_doc reference must be resolvable";
                toCollect = docRef.cast(DataTypes.STRING, CastMode.EXPLICIT);
            }
            outputs = List.of(toCollect);
        }

        Settings settings = Settings.builder().put(copyTo.properties().map(eval)).build();

        WriterProjection.CompressionType compressionType =
            settingAsEnum(WriterProjection.CompressionType.class, COMPRESSION_SETTING.get(settings));
        WriterProjection.OutputFormat outputFormat =
            settingAsEnum(WriterProjection.OutputFormat.class, OUTPUT_FORMAT_SETTING.get(settings));

        if (!columnsDefined && outputFormat == WriterProjection.OutputFormat.JSON_ARRAY) {
            throw new UnsupportedFeatureException("Output format not supported without specifying columns.");
        }

        WhereClause whereClause = new WhereClause(copyTo.whereClause(), partitions, Collections.emptySet());
        return new BoundCopyTo(
            outputs,
            table,
            whereClause,
            Literal.of(DataTypes.STRING.sanitizeValue(eval.apply(copyTo.uri()))),
            compressionType,
            outputFormat,
            outputNames.isEmpty() ? null : outputNames,
            columnsDefined,
            overwrites,
            settings);
    }

    private static List<String> resolvePartitions(List<Assignment<Object>> partitionProperties,
                                                  DocTableInfo table) {
        if (partitionProperties.isEmpty()) {
            return Collections.emptyList();
        }
        var partitionName = PartitionPropertiesAnalyzer.toPartitionName(
            table,
            partitionProperties);
        if (!table.partitions().contains(partitionName)) {
            throw new PartitionUnknownException(partitionName);
        }
        return List.of(partitionName.asIndexName());
    }
}

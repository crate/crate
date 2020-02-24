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
import io.crate.analyze.AnalyzedCopyTo;
import io.crate.analyze.BoundCopyTo;
import io.crate.analyze.PartitionPropertiesAnalyzer;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.dsl.projection.MergeCountProjection;
import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.NodeOperationTreeGenerator;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.format.SymbolPrinter;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Functions;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.Assignment;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.crate.analyze.CopyStatementSettings.COMPRESSION_SETTING;
import static io.crate.analyze.CopyStatementSettings.OUTPUT_FORMAT_SETTING;
import static io.crate.analyze.CopyStatementSettings.OUTPUT_SETTINGS;
import static io.crate.analyze.CopyStatementSettings.settingAsEnum;
import static io.crate.analyze.GenericPropertiesConverter.genericPropertiesToSettings;

public final class CopyToPlan implements Plan {

    private final AnalyzedCopyTo copyTo;
    private final LogicalPlanner logicalPlanner;
    private final SubqueryPlanner subqueryPlanner;

    public CopyToPlan(AnalyzedCopyTo copyTo,
                      LogicalPlanner logicalPlanner,
                      SubqueryPlanner subqueryPlanner) {
        this.copyTo = copyTo;
        this.logicalPlanner = logicalPlanner;
        this.subqueryPlanner = subqueryPlanner;
    }

    @VisibleForTesting
    AnalyzedCopyTo copyTo() {
        return copyTo;
    }

    @VisibleForTesting
    LogicalPlanner logicalPlanner() {
        return logicalPlanner;
    }

    @VisibleForTesting
    SubqueryPlanner subqueryPlanner() {
        return subqueryPlanner;
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
            copyTo,
            plannerContext,
            logicalPlanner,
            subqueryPlanner,
            executor.projectionBuilder(),
            params,
            subQueryResults);

        NodeOperationTree nodeOpTree = NodeOperationTreeGenerator
            .fromPlan(executionPlan, executor.localNodeId());
        executor.phasesTaskFactory()
            .create(plannerContext.jobId(), List.of(nodeOpTree))
            .execute(consumer, plannerContext.transactionContext());
    }

    @VisibleForTesting
    static ExecutionPlan planCopyToExecution(AnalyzedCopyTo copyTo,
                                             PlannerContext context,
                                             LogicalPlanner logicalPlanner,
                                             SubqueryPlanner subqueryPlanner,
                                             ProjectionBuilder projectionBuilder,
                                             Row params,
                                             SubQueryResults subQueryResults) {
        var boundedCopyTo = bind(
            copyTo,
            context.transactionContext(),
            context.functions(),
            params,
            subQueryResults);

        WriterProjection.OutputFormat outputFormat = boundedCopyTo.outputFormat();
        if (outputFormat == null) {
            outputFormat = boundedCopyTo.columnsDefined() ?
                WriterProjection.OutputFormat.JSON_ARRAY : WriterProjection.OutputFormat.JSON_OBJECT;
        }

        WriterProjection projection = ProjectionBuilder.writerProjection(
            boundedCopyTo.relation().outputs(),
            boundedCopyTo.uri(),
            boundedCopyTo.compressionType(),
            boundedCopyTo.overwrites(),
            boundedCopyTo.outputNames(),
            outputFormat);

        LogicalPlan logicalPlan = logicalPlanner.normalizeAndPlan(
            boundedCopyTo.relation(),
            context,
            subqueryPlanner,
            Set.of());

        ExecutionPlan executionPlan = logicalPlan.build(
            context,
            projectionBuilder,
            0, 0, null, null, params, SubQueryResults.EMPTY);
        executionPlan.addProjection(projection);

        return Merge.ensureOnHandler(
            executionPlan,
            context,
            List.of(MergeCountProjection.INSTANCE));
    }

    @VisibleForTesting
    public static BoundCopyTo bind(AnalyzedCopyTo copyTo,
                                   CoordinatorTxnCtx txnCtx,
                                   Functions functions,
                                   Row parameters,
                                   SubQueryResults subQueryResults) {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            txnCtx,
            functions,
            x,
            parameters,
            subQueryResults
        );

        DocTableRelation tableRelation = new DocTableRelation((DocTableInfo) copyTo.tableInfo());

        List<String> partitions = resolvePartitions(
            Lists2.map(copyTo.table().partitionProperties(), x -> x.map(eval)),
            tableRelation.tableInfo());

        List<Symbol> outputs = new ArrayList<>();
        Map<ColumnIdent, Symbol> overwrites = null;
        boolean columnsDefined = false;
        List<String> outputNames = null;
        if (!copyTo.columns().isEmpty()) {
            outputNames = new ArrayList<>(copyTo.columns().size());
            for (Symbol symbol : copyTo.columns()) {
                outputNames.add(SymbolPrinter.printUnqualified(symbol));
                outputs.add(DocReferences.toSourceLookup(symbol));
            }
            columnsDefined = true;
        } else {
            Reference sourceRef;
            if (tableRelation.tableInfo().isPartitioned() && partitions.isEmpty()) {
                // table is partitioned, insert partitioned columns into the output
                overwrites = new HashMap<>();
                for (Reference reference : tableRelation.tableInfo().partitionedByColumns()) {
                    if (!(reference instanceof GeneratedReference)) {
                        overwrites.put(reference.column(), reference);
                    }
                }
                if (overwrites.size() > 0) {
                    sourceRef = tableRelation.tableInfo().getReference(DocSysColumns.DOC);
                } else {
                    sourceRef = tableRelation.tableInfo().getReference(DocSysColumns.RAW);
                }
            } else {
                sourceRef = tableRelation.tableInfo().getReference(DocSysColumns.RAW);
            }
            outputs = List.of(sourceRef);
        }

        Settings settings = genericPropertiesToSettings(
            copyTo.properties().map(eval),
            OUTPUT_SETTINGS);

        WriterProjection.CompressionType compressionType =
            settingAsEnum(WriterProjection.CompressionType.class, COMPRESSION_SETTING.get(settings));
        WriterProjection.OutputFormat outputFormat =
            settingAsEnum(WriterProjection.OutputFormat.class, OUTPUT_FORMAT_SETTING.get(settings));

        if (!columnsDefined && outputFormat == WriterProjection.OutputFormat.JSON_ARRAY) {
            throw new UnsupportedFeatureException("Output format not supported without specifying columns.");
        }

        WhereClause whereClause = new WhereClause(copyTo.whereClause(), partitions, Collections.emptySet());
        QuerySpec querySpec = new QuerySpec(
            outputs,
            whereClause,
            List.of(),
            null,
            null,
            null,
            null
        );
        QueriedSelectRelation<DocTableRelation> subRelation = new QueriedSelectRelation<>(
            false,
            tableRelation,
            outputNames == null
                ? Lists2.map(outputs, Symbols::pathFromSymbol)
                : Lists2.map(outputNames, ColumnIdent::new),
            querySpec
        );

        return new BoundCopyTo(
            subRelation,
            Literal.of(DataTypes.STRING.value(eval.apply(copyTo.uri()))),
            compressionType,
            outputFormat,
            outputNames,
            columnsDefined,
            overwrites);
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

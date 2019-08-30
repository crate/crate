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

package io.crate.planner.operators;

import com.google.common.collect.Sets;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.FetchPhase;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.FetchProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.FetchReference;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.FieldReplacer;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.RefReplacer;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.ReaderAllocations;
import io.crate.planner.consumer.FetchMode;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

import static io.crate.planner.operators.LogicalPlanner.extractColumns;
import static io.crate.planner.operators.OperatorUtils.getUnusedColumns;


/**
 * The FetchOrEval operator is producing the values for all selected expressions.
 *
 * <p>
 * This can be a simple re-arranging of expressions, the evaluation of scalar functions or involve a fetch operation.
 * The first two are rather simple and are handled by a {@link EvalProjection}, the fetch operation is more complicated.
 * </p>
 *
 * <h2>Fetch</h2>:
 *
 * <p>
 * On user tables it's possible to collect a {@link DocSysColumns#FETCHID}, which is like a unique row-id, except that it
 * is only valid within an open index reader. This fetchId can be used to retrieve the values from a row.
 *
 * The idea of fetch is to avoid loading more values than required.
 * This is the case if the data is collected from several nodes and then merged on the coordinator while applying some
 * form of data reduction, like applying a limit.
 * </p>
 *
 * <pre>
 *     select a, b, c from t order by limit 500
 *
 *
 *       N1                 N2
 *        Collect 500        Collect 500
 *             \              /
 *              \            /
 *                Merge
 *              1000 -> 500
 *                  |
 *                Fetch 500
 * </pre>
 */
public class FetchOrEval extends ForwardingLogicalPlan {

    private final FetchMode fetchMode;
    private final boolean doFetch;
    private final List<Symbol> outputs;

    static LogicalPlan.Builder create(LogicalPlan.Builder sourceBuilder,
                                      List<Symbol> outputs,
                                      FetchMode fetchMode,
                                      boolean isLastFetch,
                                      boolean childIsLimited) {
        return (tableStats, hints, usedBeforeNextFetch) -> {
            final LogicalPlan source;

            // This avoids collecting scalars unnecessarily if their source-columns are already collected
            // Ex. cases like: select xx from (select x + x as xx, ... from t1 order by x ..)
            usedBeforeNextFetch = extractColumns(usedBeforeNextFetch);

            boolean doFetch = isLastFetch;
            if (fetchMode == FetchMode.NEVER_CLEAR) {
                source = sourceBuilder.build(tableStats, hints, usedBeforeNextFetch);
            } else if (isLastFetch) {
                source = sourceBuilder.build(tableStats, hints, Collections.emptySet());
            } else {
                /*
                 * In a case like
                 *      select sum(x) from (select x from t limit 10)
                 *
                 * It makes sense to do an intermediate fetch, to reduce the amount of rows.
                 * All columns are used, so a _fetchId propagation wouldn't work.
                 *
                 *
                 * But in a case like
                 *      select x, y from (select x, y from t order by x limit 10) order by x asc limit 5
                 *
                 * A _fetchId propagation makes sense because there are unusedColumns (y), which can be fetched
                 * at the end.
                 */
                List<Symbol> unusedColumns = getUnusedColumns(outputs, usedBeforeNextFetch);
                if (unusedColumns.isEmpty() && childIsLimited) {
                    source = sourceBuilder.build(tableStats, hints, Collections.emptySet());
                    doFetch = true;
                } else {
                    source = sourceBuilder.build(tableStats, hints, usedBeforeNextFetch);
                }
            }
            if (source.outputs().equals(outputs)) {
                return source;
            }
            if (!doFetch && Symbols.containsColumn(source.outputs(), DocSysColumns.FETCHID)) {
                if (usedBeforeNextFetch.isEmpty()) {
                    return new FetchOrEval(source, source.outputs(), fetchMode, false);
                } else {
                    return new FetchOrEval(source, generateOutputs(outputs, source.outputs()), fetchMode, false);
                }
            } else {
                return new FetchOrEval(source, outputs, fetchMode, true);
            }
        };
    }

    public FetchOrEval(LogicalPlan source, List<Symbol> outputs, FetchMode fetchMode, boolean doFetch) {
        super(source);
        this.outputs = outputs;
        this.fetchMode = fetchMode;
        this.doFetch = doFetch;
    }

    /**
     * Returns the source outputs and if there are scalars in the wantedOutputs which can be evaluated
     * using the sourceOutputs these scalars are included as well.
     *
     * <pre>
     *     wantedOutputs: R.x + R.y, R.i
     *      -> wantedColumns: { R.x + R.y: {R.x, R.y} }
     *     sourceOutputs: R._fetchId, R.x, R.y
     *
     *     result: R._fetchId, R.x + R.x
     * </pre>
     */
    private static List<Symbol> generateOutputs(List<Symbol> wantedOutputs, List<Symbol> sourceOutputs) {
        ArrayList<Symbol> result = new ArrayList<>();
        Set<Symbol> sourceColumns = extractColumns(sourceOutputs);
        addFetchIdColumns(sourceOutputs, result);

        HashMap<Symbol, Set<Symbol>> wantedColumnsByScalar = new HashMap<>();
        for (int i = 0; i < wantedOutputs.size(); i++) {
            Symbol wantedOutput = wantedOutputs.get(i);
            if (wantedOutput instanceof Function) {
                wantedColumnsByScalar.put(wantedOutput, extractColumns(wantedOutput));
            } else {
                if (sourceColumns.contains(wantedOutput)) {
                    result.add(wantedOutput);
                }
            }
        }
        addScalarsWithAvailableSources(result, sourceColumns, wantedColumnsByScalar);
        return result;
    }

    private static void addScalarsWithAvailableSources(ArrayList<Symbol> result,
                                                       Set<Symbol> sourceColumns,
                                                       HashMap<Symbol, Set<Symbol>> wantedColumnsByScalar) {
        for (Map.Entry<Symbol, Set<Symbol>> wantedEntry : wantedColumnsByScalar.entrySet()) {
            Symbol wantedOutput = wantedEntry.getKey();
            Set<Symbol> columnsUsedInScalar = wantedEntry.getValue();
            if (Sets.difference(columnsUsedInScalar, sourceColumns).isEmpty()) {
                result.add(wantedOutput);
            }
        }
    }

    private static void addFetchIdColumns(List<Symbol> sourceOutputs, ArrayList<Symbol> result) {
        for (int i = 0; i < sourceOutputs.size(); i++) {
            Symbol sourceOutput = sourceOutputs.get(i);
            if (Symbols.containsColumn(sourceOutput, DocSysColumns.FETCHID)) {
                result.add(sourceOutput);
            }
        }
    }

    @Override
    public ExecutionPlan build(PlannerContext plannerContext,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {

        ExecutionPlan executionPlan = source.build(
            plannerContext, projectionBuilder, limit, offset, null, pageSizeHint, params, subQueryResults);
        List<Symbol> sourceOutputs = source.outputs();
        if (doFetch && Symbols.containsColumn(sourceOutputs, DocSysColumns.FETCHID)) {
            return planWithFetch(plannerContext, executionPlan, sourceOutputs, params, subQueryResults);
        }
        return planWithEvalProjection(plannerContext, executionPlan, sourceOutputs, params, subQueryResults);
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new FetchOrEval(Lists2.getOnlyElement(sources), outputs, fetchMode, doFetch);
    }

    private ExecutionPlan planWithFetch(PlannerContext plannerContext,
                                        ExecutionPlan executionPlan,
                                        List<Symbol> sourceOutputs,
                                        Row params,
                                        SubQueryResults subQueryResults) {
        executionPlan = Merge.ensureOnHandler(executionPlan, plannerContext);
        Map<RelationName, FetchSource> fetchSourceByTableId = new HashMap<>();
        LinkedHashSet<Reference> allFetchRefs = new LinkedHashSet<>();

        Map<FullQualifiedTableRelation, InputColumn> fetchInputColumnsByTable = buildFetchInputColumnsMap(sourceOutputs);
        BiConsumer<FullQualifiedTableRelation, Reference> allocateFetchRef = (rel, ref) -> {
            DocTableInfo tableInfo = rel.docTableRelation.tableInfo();
            RelationName relationName = tableInfo.ident();
            FetchSource fetchSource = fetchSourceByTableId.get(relationName);
            if (fetchSource == null) {
                fetchSource = new FetchSource(tableInfo.partitionedByColumns());
                fetchSourceByTableId.put(relationName, fetchSource);
            }
            if (ref.granularity() == RowGranularity.DOC) {
                allFetchRefs.add(ref);
                fetchSource.addRefToFetch(ref);
            }
            fetchSource.addFetchIdColumn(fetchInputColumnsByTable.get(rel));
        };
        List<Symbol> fetchOutputs = new ArrayList<>(outputs.size());
        for (Symbol output : outputs) {
            fetchOutputs.add(toInputColOrFetchRef(
                output,
                sourceOutputs,
                fetchInputColumnsByTable,
                allocateFetchRef,
                null,
                subQueryResults,
                params)
            );
        }
        if (source.baseTables().size() == 1) {
            // If there are no relation boundaries involved the outputs will have contained no fields but only references
            // and the actions so far had no effect
            Lists2.mutate(
                fetchOutputs,
                s -> transformRefs(
                    s,
                    sourceOutputs,
                    fetchInputColumnsByTable,
                    allocateFetchRef,
                    source.getRelationNames().iterator().next(),
                    source.baseTables().get(0)));
        }
        if (fetchSourceByTableId.isEmpty()) {
            // TODO:
            // this can happen if the Collect operator adds a _fetchId, but it turns out
            // that all required columns are already provided.
            // This should be improved so that this case no longer occurs
            // `testNestedSimpleSelectWithJoin` is an example case
            return planWithEvalProjection(plannerContext, executionPlan, sourceOutputs, params, subQueryResults);
        }

        ReaderAllocations readerAllocations = plannerContext.buildReaderAllocations();
        FetchPhase fetchPhase = new FetchPhase(
            plannerContext.nextExecutionPhaseId(),
            readerAllocations.nodeReaders().keySet(),
            readerAllocations.bases(),
            readerAllocations.tableIndices(),
            allFetchRefs
        );
        FetchProjection fetchProjection = new FetchProjection(
            fetchPhase.phaseId(),
            plannerContext.fetchSize(),
            fetchSourceByTableId,
            fetchOutputs,
            readerAllocations.nodeReaders(),
            readerAllocations.indices(),
            readerAllocations.indicesToIdents()
        );
        executionPlan.addProjection(fetchProjection);
        return new QueryThenFetch(executionPlan, fetchPhase);
    }

    private static Symbol transformRefs(Symbol output,
                                        List<Symbol> sourceOutputs,
                                        Map<FullQualifiedTableRelation, InputColumn> fetchInputColumnsByTable,
                                        BiConsumer<FullQualifiedTableRelation, Reference> allocateFetchRef,
                                        QualifiedName relationName,
                                        AbstractTableRelation baseTable) {
        int idxInSource = sourceOutputs.indexOf(output);
        if (idxInSource > -1) {
            return new InputColumn(idxInSource, sourceOutputs.get(idxInSource).valueType());
        }
        if (!(baseTable instanceof DocTableRelation)) {
            throw new IllegalArgumentException("Output " + output + " must be in the sourceOutput. " +
                                               "It cannot be fetched because no user-tables are involved");
        }
        DocTableRelation docTableRelation = (DocTableRelation) baseTable;
        return RefReplacer.replaceRefs(output, ref -> {
            int idx = sourceOutputs.indexOf(ref);
            if (idx > -1) {
                return new InputColumn(idx, sourceOutputs.get(idx).valueType());
            }
            if (ref.granularity() == RowGranularity.DOC) {
                ref = DocReferences.toSourceLookup(ref);
            }
            FullQualifiedTableRelation fqRel = new FullQualifiedTableRelation(relationName, docTableRelation);
            allocateFetchRef.accept(fqRel, ref);
            return new FetchReference(fetchInputColumnsByTable.get(fqRel), ref);
        });
    }

    private Map<FullQualifiedTableRelation, InputColumn> buildFetchInputColumnsMap(List<Symbol> outputs) {
        HashMap<FullQualifiedTableRelation, InputColumn> m = new HashMap<>();
        for (int i = 0; i < outputs.size(); i++) {
            Symbol output = outputs.get(i);
            if (output instanceof Field &&
                ((Field) output).path().sqlFqn().equals(DocSysColumns.FETCHID.sqlFqn())) {

                DocTableRelation rel = resolveDocTableRelation(output);
                FullQualifiedTableRelation fqRel = new FullQualifiedTableRelation(
                    ((Field) output).relation().getQualifiedName(), rel);
                m.put(fqRel, new InputColumn(i, DataTypes.LONG));
            } else if (output instanceof Reference &&
                       ((Reference) output).column().equals(DocSysColumns.FETCHID)) {
                assert source.baseTables().size() == 1 : "There must only be one table if dealing with References";
                AbstractTableRelation tableRelation = source.baseTables().get(0);
                assert tableRelation instanceof DocTableRelation : "baseTable must be a DocTable if there is a fetchId";

                m.put(new FullQualifiedTableRelation(((DocTableRelation) tableRelation)), new InputColumn(i, DataTypes.LONG));
            }
        }
        return m;
    }

    private DocTableRelation resolveDocTableRelation(Symbol output) {
        while (true) {
            if (output instanceof Field) {
                Field field = (Field) output;
                DocTableRelation docTableRelation = resolveDocTableRelation(field);
                if (docTableRelation != null) {
                    return docTableRelation;
                }
                output = field.pointer();
            } else {
                throw new IllegalStateException("Couldn't retrieve DocTableRelation from " + output);
            }
        }
    }

    private static DocTableRelation resolveDocTableRelation(Field field) {
        AnalyzedRelation relation = field.relation();
        if (relation instanceof DocTableRelation) {
            return (DocTableRelation) relation;
        }
        if (relation instanceof QueriedSelectRelation
            && ((QueriedSelectRelation) relation).subRelation() instanceof DocTableRelation) {

            return ((DocTableRelation) ((QueriedSelectRelation) relation).subRelation());
        }
        return null;
    }

    private static Symbol toInputColOrFetchRef(Symbol output,
                                               List<Symbol> sourceOutputs,
                                               Map<FullQualifiedTableRelation, InputColumn> fetchInputColumnsByTable,
                                               BiConsumer<FullQualifiedTableRelation, Reference> allocateFetchRef,
                                               @Nullable QualifiedName currentQualifiedName,
                                               SubQueryResults subQueryResults,
                                               Row params) {
        int idxInSource = sourceOutputs.indexOf(output);
        if (idxInSource > -1) {
            return new InputColumn(idxInSource, sourceOutputs.get(idxInSource).valueType());
        }

        Symbol boundedOutput = SubQueryAndParamBinder.convert(output, params, subQueryResults);
        idxInSource = sourceOutputs.indexOf(boundedOutput);
        if (idxInSource > -1) {
            return new InputColumn(idxInSource, sourceOutputs.get(idxInSource).valueType());
        }

        return FieldReplacer.replaceFields(boundedOutput, f -> {
            int idx = sourceOutputs.indexOf(f);
            if (idx > -1) {
                return new InputColumn(idx, sourceOutputs.get(idx).valueType());
            }
            AnalyzedRelation relation = f.relation();
            final QualifiedName qualifiedName;
            if (currentQualifiedName != null) {
                qualifiedName = currentQualifiedName;
            } else {
                qualifiedName = relation.getQualifiedName();
            }
            Symbol symbol = f.pointer();
            if (symbol instanceof Reference) {
                Reference ref = (Reference) symbol;
                DocTableRelation docTableRelation = resolveDocTableRelation(f);
                assert docTableRelation != null : "Couldn't retrieve DocTableRelation from " + f;
                if (ref.granularity() == RowGranularity.DOC) {
                    ref = DocReferences.toSourceLookup(ref);
                }
                FullQualifiedTableRelation fqRel = new FullQualifiedTableRelation(qualifiedName, docTableRelation);
                allocateFetchRef.accept(fqRel, ref);
                InputColumn fetchId = fetchInputColumnsByTable.get(fqRel);
                assert fetchId != null : "fetchId InputColumn for " + docTableRelation + " must be present";
                return new FetchReference(fetchId, ref);
            }
            return toInputColOrFetchRef(
                symbol,
                sourceOutputs,
                fetchInputColumnsByTable,
                allocateFetchRef,
                qualifiedName,
                subQueryResults,
                params);
        });
    }

    private ExecutionPlan planWithEvalProjection(PlannerContext plannerContext,
                                                 ExecutionPlan executionPlan,
                                                 List<Symbol> sourceOutputs,
                                                 Row params,
                                                 SubQueryResults subQueryResults) {
        SubQueryAndParamBinder binder = new SubQueryAndParamBinder(params, subQueryResults);
        List<Symbol> boundOutputs = Lists2.map(outputs, binder);
        PositionalOrderBy orderBy = executionPlan.resultDescription().orderBy();
        PositionalOrderBy newOrderBy;
        if (orderBy == null) {
            newOrderBy = null;
        } else {
            newOrderBy = orderBy.tryMapToNewOutputs(sourceOutputs, boundOutputs);
            if (newOrderBy == null) {
                // We've a query like `SELECT x, y FROM t ORDER BY z`
                //
                // The previous operator added `z` to the outputs to be able to do a sorted merge;
                // We couldn't map the PositionalOrderBy to the new outputs (=[x,y]) since they don't contain `z` anymore.
                // We need to merge to handler *before* we remove `z` from the outputs (which is what the eval here would do)
                executionPlan = Merge.ensureOnHandler(executionPlan, plannerContext);
            }
        }
        InputColumns.SourceSymbols ctx = new InputColumns.SourceSymbols(Lists2.map(sourceOutputs, binder));
        executionPlan.addProjection(
            new EvalProjection(InputColumns.create(boundOutputs, ctx)),
            executionPlan.resultDescription().limit(),
            executionPlan.resultDescription().offset(),
            newOrderBy
        );
        return executionPlan;
    }

    @Override
    public String toString() {
        return "FetchOrEval{" +
               "src=" + source +
               ", out=" + outputs +
               '}';
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitFetchOrEval(this, context);
    }

    private static class FullQualifiedTableRelation {
        private final QualifiedName qualifiedName;
        private final DocTableRelation docTableRelation;

        FullQualifiedTableRelation(DocTableRelation docTableRelation) {
            this(docTableRelation.getQualifiedName(), docTableRelation);
        }

        FullQualifiedTableRelation(QualifiedName qualifiedName,
                                   DocTableRelation docTableRelation) {
            this.qualifiedName = qualifiedName;
            this.docTableRelation = docTableRelation;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FullQualifiedTableRelation that = (FullQualifiedTableRelation) o;
            return Objects.equals(qualifiedName, that.qualifiedName) &&
                   Objects.equals(docTableRelation, that.docTableRelation);
        }

        @Override
        public int hashCode() {
            return Objects.hash(qualifiedName, docTableRelation);
        }
    }
}

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

import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.FetchReference;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.FieldReplacer;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.RefReplacer;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.collections.Lists2;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.ReaderAllocations;
import io.crate.planner.consumer.FetchMode;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.fetch.FetchPhase;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.planner.projection.EvalProjection;
import io.crate.planner.projection.FetchProjection;
import io.crate.planner.projection.builder.InputColumns;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

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
class FetchOrEval implements LogicalPlan {

    final LogicalPlan source;
    final List<Symbol> outputs;

    private final FetchMode fetchMode;
    private final boolean isLastFetch;

    static LogicalPlan.Builder create(LogicalPlan.Builder sourceBuilder,
                                      List<Symbol> outputs,
                                      FetchMode fetchMode,
                                      boolean isLastFetch) {
        return usedBeforeNextFetch -> {
            final LogicalPlan source;
            if (fetchMode == FetchMode.NEVER || !isLastFetch) {
                source = sourceBuilder.build(usedBeforeNextFetch);
            } else {
                source = sourceBuilder.build(Collections.emptySet());
            }
            if (source.outputs().equals(outputs)) {
                return source;
            }
            return new FetchOrEval(source, outputs, fetchMode, isLastFetch);
        };
    }

    private FetchOrEval(LogicalPlan source,
                        List<Symbol> outputs,
                        FetchMode fetchMode,
                        boolean isLastFetch) {
        this.source = source;
        this.fetchMode = fetchMode;
        this.isLastFetch = isLastFetch;
        if (isLastFetch) {
            this.outputs = outputs;
        } else {
            if (Symbols.containsColumn(source.outputs(), DocSysColumns.FETCHID)) {
                this.outputs = source.outputs();
            } else {
                this.outputs = outputs;
            }
        }
    }

    @Override
    public Plan build(Planner.Context plannerContext,
                      ProjectionBuilder projectionBuilder,
                      int limit,
                      int offset,
                      @Nullable OrderBy order,
                      @Nullable Integer pageSizeHint) {

        Plan plan = source.build(plannerContext, projectionBuilder, limit, offset, null, pageSizeHint);
        List<Symbol> sourceOutputs = source.outputs();
        if (sourceOutputs.equals(outputs)) {
            return plan;
        }

        if (Symbols.containsColumn(sourceOutputs, DocSysColumns.FETCHID)) {
            return planWithFetch(plannerContext, plan, sourceOutputs);
        }
        return planWithEvalProjection(plannerContext, plan, sourceOutputs);
    }

    private Plan planWithFetch(Planner.Context plannerContext, Plan plan, List<Symbol> sourceOutputs) {
        plan = Merge.ensureOnHandler(plan, plannerContext);
        Map<TableIdent, FetchSource> fetchSourceByTableId = new HashMap<>();
        LinkedHashSet<Reference> allFetchRefs = new LinkedHashSet<>();

        Map<DocTableRelation, InputColumn> fetchInputColumnsByTable = buildFetchInputColumnsMap(sourceOutputs);
        BiConsumer<DocTableRelation, Reference> allocateFetchRef = (rel, ref) -> {
            TableIdent tableIdent = rel.tableInfo().ident();
            FetchSource fetchSource = fetchSourceByTableId.get(tableIdent);
            if (fetchSource == null) {
                fetchSource = new FetchSource(rel.tableInfo().partitionedByColumns());
                fetchSourceByTableId.put(tableIdent, fetchSource);
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
                output, sourceOutputs, fetchInputColumnsByTable, allocateFetchRef, source.expressionMapping()));
        }
        if (source.baseTables().size() == 1) {
            // If there are no relation boundaries involved the outputs will have contained no fields but only references
            // and the actions so far had no effect
            Lists2.replaceItems(
                fetchOutputs,
                s -> transformRefs(
                    s,
                    sourceOutputs,
                    fetchInputColumnsByTable,
                    allocateFetchRef,
                    source.baseTables().get(0)));
        }
        if (fetchSourceByTableId.isEmpty()) {
            // TODO:
            // this can happen if the Collect operator adds a _fetchId, but it turns out
            // that all required columns are already provided.
            // This should be improved so that this case no longer occurs
            // `testNestedSimpleSelectWithJoin` is an example case
            return planWithEvalProjection(plannerContext, plan, sourceOutputs);
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
        plan.addProjection(fetchProjection);
        return new QueryThenFetch(plan, fetchPhase);
    }

    private static Symbol transformRefs(Symbol output,
                                        List<Symbol> sourceOutputs,
                                        Map<DocTableRelation, InputColumn> fetchInputColumnsByTable,
                                        BiConsumer<DocTableRelation, Reference> allocateFetchRef,
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
            allocateFetchRef.accept(docTableRelation, ref);
            return new FetchReference(fetchInputColumnsByTable.get(docTableRelation), ref);
        });
    }

    private Map<DocTableRelation, InputColumn> buildFetchInputColumnsMap(List<Symbol> outputs) {
        HashMap<DocTableRelation, InputColumn> m = new HashMap<>();
        for (int i = 0; i < outputs.size(); i++) {
            Symbol output = outputs.get(i);
            if (output instanceof Field &&
                ((Field) output).path().outputName().equals(DocSysColumns.FETCHID.outputName())) {

                DocTableRelation rel = resolveDocTableRelation(output);
                m.put(rel, new InputColumn(i, DataTypes.LONG));
            } else if (output instanceof Reference &&
                       ((Reference) output).ident().columnIdent().equals(DocSysColumns.FETCHID)) {
                assert source.baseTables().size() == 1 : "There must only be one table if dealing with References";
                AbstractTableRelation tableRelation = source.baseTables().get(0);
                assert tableRelation instanceof DocTableRelation : "baseTable must be a DocTable if there is a fetchId";

                m.put(((DocTableRelation) tableRelation), new InputColumn(i, DataTypes.LONG));
            }
        }
        return m;
    }

    private DocTableRelation resolveDocTableRelation(Symbol output) {
        Symbol mapped = output;
        Symbol old;
        do {
            old = mapped;
            if (old instanceof Field && ((Field) old).relation() instanceof QueriedDocTable) {
                return ((QueriedDocTable) ((Field) old).relation()).tableRelation();
            }
        } while ((mapped = source.expressionMapping().get(old)) != null);
        throw new IllegalStateException("Couldn't retrieve DocTableRelation from " + output);
    }

    private static Symbol toInputColOrFetchRef(Symbol output,
                                               List<Symbol> sourceOutputs,
                                               Map<DocTableRelation, InputColumn> fetchInputColumnsByTable,
                                               BiConsumer<DocTableRelation, Reference> allocateFetchRef,
                                               Map<Symbol, Symbol> expressionMapping) {
        int idxInSource = sourceOutputs.indexOf(output);
        if (idxInSource > -1) {
            return new InputColumn(idxInSource, sourceOutputs.get(idxInSource).valueType());
        }
        return FieldReplacer.replaceFields(output, f -> {
            int idx = sourceOutputs.indexOf(f);
            if (idx > -1) {
                return new InputColumn(idx, sourceOutputs.get(idx).valueType());
            }
            AnalyzedRelation relation = f.relation();
            if (relation instanceof QueriedDocTable) {
                DocTableRelation docTableRelation = ((QueriedDocTable) relation).tableRelation();

                Symbol symbol = expressionMapping.get(f);
                return RefReplacer.replaceRefs(symbol, ref -> {
                    if (ref.granularity() == RowGranularity.DOC) {
                        ref = DocReferences.toSourceLookup(ref);
                    }
                    allocateFetchRef.accept(docTableRelation, ref);
                    InputColumn fetchId = fetchInputColumnsByTable.get(docTableRelation);
                    assert fetchId != null : "fetchId InputColumn for " + docTableRelation + " must be present";
                    return new FetchReference(fetchId, ref);
                });
            }
            Symbol mapped = expressionMapping.get(output);
            assert mapped != null
                : "Field mapping must exists for " + output + " in " + expressionMapping;
            return toInputColOrFetchRef(
                mapped, sourceOutputs, fetchInputColumnsByTable, allocateFetchRef, expressionMapping);
        });
    }

    private Plan planWithEvalProjection(Planner.Context plannerContext, Plan plan, List<Symbol> sourceOutputs) {
        PositionalOrderBy orderBy = plan.resultDescription().orderBy();
        PositionalOrderBy newOrderBy = null;
        if (orderBy != null) {
            newOrderBy = orderBy.tryMapToNewOutputs(sourceOutputs, outputs);
            if (newOrderBy == null) {
                plan = Merge.ensureOnHandler(plan, plannerContext);
            }
        }
        InputColumns.Context ctx = new InputColumns.Context(sourceOutputs);
        plan.addProjection(
            new EvalProjection(InputColumns.create(this.outputs, ctx)),
            plan.resultDescription().limit(),
            plan.resultDescription().offset(),
            newOrderBy
        );
        return plan;
    }

    @Override
    public LogicalPlan tryCollapse() {
        LogicalPlan collapsed = source.tryCollapse();
        if (collapsed == this) {
            return this;
        }
        return new FetchOrEval(collapsed, outputs, fetchMode, isLastFetch);
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public Map<Symbol, Symbol> expressionMapping() {
        return source.expressionMapping();
    }

    @Override
    public List<AbstractTableRelation> baseTables() {
        return source.baseTables();
    }

    @Override
    public String toString() {
        return "FetchOrEval{" +
               "src=" + source +
               ", out=" + outputs +
               '}';
    }
}

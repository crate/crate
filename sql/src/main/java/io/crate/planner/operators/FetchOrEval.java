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
import io.crate.analyze.symbol.FetchReference;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.RefReplacer;
import io.crate.analyze.symbol.RefVisitor;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.collections.Lists2;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.ReaderAllocations;
import io.crate.planner.consumer.FetchMode;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.fetch.FetchPhase;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.planner.projection.EvalProjection;
import io.crate.planner.projection.FetchProjection;
import io.crate.planner.projection.builder.InputColumns;
import io.crate.planner.projection.builder.ProjectionBuilder;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class FetchOrEval implements LogicalPlan {

    final LogicalPlan source;
    final List<Symbol> outputs;
    private final Set<Symbol> usedColumns;

    static LogicalPlan.Builder create(LogicalPlan.Builder source,
                                      List<Symbol> outputs,
                                      FetchMode fetchMode) {

        return usedColumns -> {
            switch (fetchMode) {
                case NEVER:
                    return new FetchOrEval(source.build(usedColumns), usedColumns, outputs);

                case NO_PROPAGATION:
                case WITH_PROPAGATION:
                    return new FetchOrEval(source.build(Collections.emptySet()), usedColumns, outputs);

                default:
                    throw new AssertionError("Invalid fetchMode: " + fetchMode);
            }
        };
    }

    private FetchOrEval(LogicalPlan source, Set<Symbol> usedColumns, List<Symbol> outputs) {
        this.source = source;
        this.usedColumns = usedColumns;
        this.outputs = outputs;
    }

    @Override
    public Plan build(Planner.Context plannerContext,
                      ProjectionBuilder projectionBuilder,
                      int limit,
                      int offset,
                      @Nullable OrderBy order,
                      @Nullable Integer pageSizeHint) {

        Plan plan = source.build(plannerContext, projectionBuilder, limit, offset, order, pageSizeHint);
        List<Symbol> sourceOutputs = source.outputs();
        assert !sourceOutputs.equals(outputs)
            : "FetchOrEval should be removed in tryCollapse if it has nothing to do";

        if (!Symbols.containsColumn(sourceOutputs, DocSysColumns.FETCHID)) {
            return planWithEvalProjection(plannerContext, plan, sourceOutputs);
        }
        return planWithFetch(plannerContext, plan, sourceOutputs);
    }

    private Plan planWithFetch(Planner.Context plannerContext, Plan plan, List<Symbol> sourceOutputs) {
        plan = Merge.ensureOnHandler(plan, plannerContext);
        ReaderAllocations readerAllocations = plannerContext.buildReaderAllocations();
        Map<TableIdent, FetchSource> fetchSourceByTableId = new HashMap<>(source.baseTables().size());
        ArrayList<Reference> allFetchRefs = new ArrayList<>();
        for (TableInfo table : source.baseTables()) {
            if (!(table instanceof DocTableInfo)) {
                continue;
            }
            DocTableInfo docTableInfo = (DocTableInfo) table;
            List<Reference> fetchRefs = fetchRefsFor(outputs, docTableInfo.ident());
            allFetchRefs.addAll(fetchRefs);
            fetchSourceByTableId.put(
                docTableInfo.ident(),
                new FetchSource(
                    docTableInfo.partitionedByColumns(),
                    Collections.singletonList(new InputColumn(idxOfFetchId(sourceOutputs, docTableInfo.ident()))),
                    fetchRefs
                )
            );
        }
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
            Lists2.copyAndReplace(outputs, st -> toFetchRefOrInputColumn(st, sourceOutputs)),
            readerAllocations.nodeReaders(),
            readerAllocations.indices(),
            readerAllocations.indicesToIdents()
        );
        plan.addProjection(fetchProjection);
        return new QueryThenFetch(plan, fetchPhase);
    }

    private Plan planWithEvalProjection(Planner.Context plannerContext, Plan plan, List<Symbol> sourceOutputs) {
        if (plan.resultDescription().orderBy() != null) {
            plan = Merge.ensureOnHandler(plan, plannerContext);
        }
        InputColumns.Context ctx = new InputColumns.Context(sourceOutputs);
        plan.addProjection(new EvalProjection(InputColumns.create(this.outputs, ctx)));
        return plan;
    }

    private static Symbol toFetchRefOrInputColumn(Symbol st, List<Symbol> sourceOutputs) {
        int idx = sourceOutputs.indexOf(st);
        if (idx > -1) {
            return new InputColumn(idx, sourceOutputs.get(idx).valueType());
        }
        return RefReplacer.replaceRefs(st, r -> {
            int i = sourceOutputs.indexOf(r);
            if (i == -1) {
                InputColumn inputColumn = new InputColumn(idxOfFetchId(sourceOutputs, r.ident().tableIdent()));
                final Reference ref = r.granularity() == RowGranularity.DOC
                    ? DocReferences.toSourceLookup(r)
                    : r;
                return new FetchReference(inputColumn, ref);
            }
            return new InputColumn(i, sourceOutputs.get(i).valueType());
        });
    }

    private static List<Reference> fetchRefsFor(List<Symbol> sourceOutputs, TableIdent ident) {
        final ArrayList<Reference> fetchRefs = new ArrayList<>();
        for (Symbol sourceOutput : sourceOutputs) {
            RefVisitor.visitRefs(sourceOutput, r -> {
                if (r.granularity() == RowGranularity.DOC && r.ident().tableIdent().equals(ident)) {
                    fetchRefs.add(DocReferences.toSourceLookup(r));
                }
            });
        }
        return fetchRefs;
    }

    private static int idxOfFetchId(List<Symbol> sourceOutputs, TableIdent ident) {
        for (int i = 0; i < sourceOutputs.size(); i++) {
            Symbol sourceOutput = sourceOutputs.get(i);
            if (sourceOutput instanceof Reference &&
                ((Reference) sourceOutput).ident().tableIdent().equals(ident) &&
                ((Reference) sourceOutput).ident().columnIdent().equals(DocSysColumns.FETCHID)) {

                return i;
            }
        }
        throw new IllegalArgumentException("No fetchId column found in: " + sourceOutputs);
    }

    @Override
    public LogicalPlan tryCollapse() {
        LogicalPlan collapsed = source.tryCollapse();
        if (collapsed.outputs().equals(outputs)) {
            return collapsed;
        }
        if (collapsed == this) {
            return this;
        }
        return new FetchOrEval(collapsed, usedColumns, outputs);
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public List<TableInfo> baseTables() {
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

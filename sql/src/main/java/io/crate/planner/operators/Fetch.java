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
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.FetchPhase;
import io.crate.execution.dsl.projection.FetchProjection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.FetchMarker;
import io.crate.expression.symbol.FetchReference;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.ReaderAllocations;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Fetch extends ForwardingLogicalPlan {

    private final List<Symbol> outputs;

    public Fetch(LogicalPlan source, List<Symbol> outputs) {
        super(source);
        this.outputs = outputs;
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
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
        // TODO: params + subQueryResults binding is missing(?)

        ExecutionPlan sourcePlan = Merge.ensureOnHandler(source.build(
            plannerContext,
            projectionBuilder,
            limit,
            offset,
            order,
            pageSizeHint,
            params,
            subQueryResults
        ), plannerContext);

        Map<QualifiedName, InputColumn> fetchInputColByTableRelation = new HashMap<>();
        Map<RelationName, FetchSource> fetchSourceByTable = new HashMap<>();
        List<Reference> allFetchRefs = new ArrayList<>();
        List<Symbol> sourceOutputs = source.outputs();
        for (int i = 0; i < sourceOutputs.size(); i++) {
            Symbol sourceOutput = sourceOutputs.get(i);
            if (sourceOutput instanceof FetchMarker) {
                FetchMarker fetchMarker = (FetchMarker) sourceOutput;
                DocTableInfo table = fetchMarker.table();
                FetchSource fetchSource = fetchSourceByTable.computeIfAbsent(
                    table.ident(),
                    relName -> new FetchSource(table.partitionedByColumns())
                );
                for (Symbol columnsToFetch : fetchMarker.columnsToFetch()) {
                    RefVisitor.visitRefs(columnsToFetch, ref -> {
                        if (ref.granularity() == RowGranularity.DOC) {
                            allFetchRefs.add(ref);
                            fetchSource.addRefToFetch(ref);
                        }
                    });
                }
                InputColumn fetchIdInput = new InputColumn(i, sourceOutput.valueType());
                fetchSource.addFetchIdColumn(fetchIdInput);
                fetchInputColByTableRelation.put(fetchMarker.qualifiedName(), fetchIdInput);
            }
        }
        InputColumns inputColumns = new InputColumns(source.outputs(), fetchInputColByTableRelation);
        List<Symbol> fetchOutputs = Lists2.map(outputs, s -> s.accept(inputColumns, null));

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
            fetchSourceByTable,
            fetchOutputs,
            readerAllocations.nodeReaders(),
            readerAllocations.indices(),
            readerAllocations.indicesToIdents()
        );
        sourcePlan.addProjection(fetchProjection);
        return new QueryThenFetch(sourcePlan, fetchPhase);
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new Fetch(Lists2.getOnlyElement(sources), outputs);
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitFetch(this, context);
    }

    // TODO: Can we re-use the other InputColumns class somehow?
    private static class InputColumns extends SymbolVisitor<QualifiedName, Symbol> {

        private final List<Symbol> sourceOutputs;
        private final Map<QualifiedName, InputColumn> fetchIdByRelation;

        public InputColumns(List<Symbol> sourceOutputs, Map<QualifiedName, InputColumn> fetchIdByRelation) {
            this.sourceOutputs = sourceOutputs;
            this.fetchIdByRelation = fetchIdByRelation;
        }

        @Override
        public Symbol visitFunction(Function function, QualifiedName relationName) {
            int idx = sourceOutputs.indexOf(function);
            if (idx > -1) {
                return new InputColumn(idx, function.valueType());
            }
            ArrayList<Symbol> newArgs = new ArrayList<>(function.arguments().size());
            for (Symbol argument : function.arguments()) {
                newArgs.add(argument.accept(this, relationName));
            }
            return new Function(function.info(), newArgs, function.filter());
        }

        @Override
        protected Symbol visitSymbol(Symbol symbol, QualifiedName relationName) {
            int idx = sourceOutputs.indexOf(symbol);
            if (idx > -1) {
                return new InputColumn(idx, symbol.valueType());
            }
            throw new IllegalArgumentException("Symbol " + symbol + " not available in sourceOutputs: " + sourceOutputs);
        }

        @Override
        public Symbol visitLiteral(Literal symbol, QualifiedName context) {
            return symbol;
        }

        @Override
        public Symbol visitReference(Reference ref, QualifiedName relationName) {
            int idx = sourceOutputs.indexOf(ref);
            if (idx > -1) {
                return new InputColumn(idx, ref.valueType());
            }
            InputColumn fetchId;
            if (relationName == null) {
                fetchId = fetchIdByRelation.values().iterator().next();
            } else {
                fetchId = fetchIdByRelation.get(relationName);
            }
            if (fetchId == null) {
                throw new IllegalArgumentException("No InputColumn for _fetchId of relation: " + relationName + " found.");
            }
            if (ref.granularity() == RowGranularity.DOC) {
                return new FetchReference(fetchId, ref); // TODO: DocReferences.toSourceLookup(ref));
            } else {
                return new FetchReference(fetchId, ref);
            }
        }


        @Override
        public Symbol visitField(Field field, QualifiedName relationName) {
            int idx = sourceOutputs.indexOf(field);
            if (idx > -1) {
                return new InputColumn(idx, field.valueType());
            }
            return field.pointer().accept(this, field.relation().getQualifiedName());
        }
    }
}

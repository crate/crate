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

package io.crate.planner.operators;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.SequencedCollection;
import java.util.Set;
import java.util.function.Function;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.FieldResolver;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.FetchMarker;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.metadata.RelationName;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;

/**
 * https://en.wikipedia.org/wiki/Relational_algebra#Rename_(%CF%81)
 *
 * This operator can be used as a bridge
 *
 * <pre>
 *     {@code
 *                      outputs: [Reference(x)]
 *                         |
 *          SELECT x FROM tbl as t;
 *                 |
 *                ScopedSymbol(relation=t, x)
 *
 *          Rename does two things:
 *              - Rename the relation (tbl=t)
 *              - Act as bridge for outputs. (in the example above from ScopedSymbol to Reference)
 *     }
 * </pre>
 */
public final class Rename extends ForwardingLogicalPlan implements FieldResolver {

    private final List<Symbol> outputs;
    private final FieldResolver fieldResolver;
    final RelationName name;

    public Rename(List<Symbol> outputs, RelationName name, FieldResolver fieldResolver, LogicalPlan source) {
        super(source);
        this.outputs = outputs;
        this.name = name;
        this.fieldResolver = fieldResolver;
        assert this.outputs.size() == source.outputs().size()
            : "Rename operator must have the same number of outputs as the source. Got " + outputs + " and " + source.outputs();
    }

    @Override
    public boolean preferShardProjections() {
        return source.preferShardProjections();
    }

    public RelationName name() {
        return name;
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public LogicalPlan pruneOutputsExcept(SequencedCollection<Symbol> outputsToKeep) {
        /* In `SELECT * FROM (SELECT t1.*, t2.* FROM tbl AS t1, tbl AS t2) AS tjoin`
         * The `ScopedSymbol`s are ambiguous; To map them correctly this uses a IdentityHashMap
         */
        IdentityHashMap<Symbol, Symbol> parentToChildMap = new IdentityHashMap<>(outputs.size());
        IdentityHashMap<Symbol, Symbol> childToParentMap = new IdentityHashMap<>(outputs.size());
        for (int i = 0; i < outputs.size(); i++) {
            parentToChildMap.put(outputs.get(i), source.outputs().get(i));
            childToParentMap.put(source.outputs().get(i), outputs.get(i));
        }
        ArrayList<Symbol> mappedToKeep = new ArrayList<>();
        for (Symbol outputToKeep : outputsToKeep) {
            SymbolVisitors.intersection(outputToKeep, outputs, s -> {
                Symbol childSymbol = parentToChildMap.get(s);
                assert childSymbol != null : "There must be a mapping available for symbol " + s;
                mappedToKeep.add(childSymbol);
            });
        }
        LogicalPlan newSource = source.pruneOutputsExcept(mappedToKeep);
        if (newSource == source) {
            return this;
        }
        ArrayList<Symbol> newOutputs = new ArrayList<>(newSource.outputs().size());
        for (Symbol sourceOutput : newSource.outputs()) {
            newOutputs.add(childToParentMap.get(sourceOutput));
        }
        return new Rename(
            newOutputs,
            name,
            fieldResolver,
            newSource
        );
    }

    @Nullable
    @Override
    public FetchRewrite rewriteToFetch(Collection<Symbol> usedColumns) {
        IdentityHashMap<Symbol, Symbol> parentToChildMap = new IdentityHashMap<>(outputs.size());
        IdentityHashMap<Symbol, Symbol> childToParentMap = new IdentityHashMap<>(outputs.size());
        for (int i = 0; i < outputs.size(); i++) {
            parentToChildMap.put(outputs.get(i), source.outputs().get(i));
            childToParentMap.put(source.outputs().get(i), outputs.get(i));
        }
        ArrayList<Symbol> mappedUsedColumns = new ArrayList<>();
        for (Symbol usedColumn : usedColumns) {
            SymbolVisitors.intersection(usedColumn, outputs, s -> {
                Symbol childSymbol = parentToChildMap.get(s);
                assert childSymbol != null : "There must be a mapping available for symbol " + s;
                mappedUsedColumns.add(childSymbol);
            });
        }
        FetchRewrite fetchRewrite = source.rewriteToFetch(mappedUsedColumns);
        if (fetchRewrite == null) {
            return null;
        }
        LogicalPlan newSource = fetchRewrite.newPlan();
        ArrayList<Symbol> newOutputs = new ArrayList<>();
        for (Symbol output : newSource.outputs()) {
            if (output instanceof FetchMarker) {
                FetchMarker marker = (FetchMarker) output;
                FetchMarker newMarker = new FetchMarker(name, marker.fetchRefs(), marker.fetchId());
                newOutputs.add(newMarker);
                childToParentMap.put(marker, newMarker);
            } else {
                Symbol mappedOutput = requireNonNull(
                    childToParentMap.get(output),
                    () -> "Mapping must exist for output from source. `" + output + "` is missing in " + childToParentMap
                );
                newOutputs.add(mappedOutput);
            }
        }
        LinkedHashMap<Symbol, Symbol> replacedOutputs = new LinkedHashMap<>();
        Function<Symbol, Symbol> convertChildrenToScopedSymbols = s -> MapBackedSymbolReplacer.convert(s, childToParentMap);
        for (var entry : fetchRewrite.replacedOutputs().entrySet()) {
            Symbol key = entry.getKey();
            Symbol value = entry.getValue();
            Symbol parentSymbolForKey = requireNonNull(
                childToParentMap.get(key),
                () -> "Mapping must exist for output from source. `" + key + "` is missing in " + childToParentMap
            );
            replacedOutputs.put(parentSymbolForKey, convertChildrenToScopedSymbols.apply(value));
        }
        Rename newRename = new Rename(newOutputs, name, fieldResolver, newSource);
        return new FetchRewrite(replacedOutputs, newRename);
    }

    @Override
    public ExecutionPlan build(DependencyCarrier executor,
                               PlannerContext plannerContext,
                               Set<PlanHint> hints,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        return source.build(
            executor, plannerContext, hints, projectionBuilder, limit, offset, order, pageSizeHint, params, subQueryResults);
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new Rename(outputs, name, fieldResolver, Lists.getOnlyElement(sources));
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitRename(this, context);
    }

    @Nullable
    @Override
    public Symbol resolveField(ScopedSymbol field) {
        return fieldResolver.resolveField(field);
    }

    @Override
    public List<RelationName> getRelationNames() {
        return List.of(name);
    }

    @Override
    public void print(PrintContext printContext) {
        printContext
            .text("Rename[")
            .text(Lists.joinOn(", ", outputs, Symbol::toString))
            .text("] AS ")
            .text(name.toString());
        printStats(printContext);
        printContext.nest(source::print);
    }

    @Override
    public String toString() {
        return "Rename{name=" + name + ", outputs=" + outputs + ", src=" + source + "}";
    }
}

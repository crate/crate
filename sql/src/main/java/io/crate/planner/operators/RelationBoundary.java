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
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.FieldsVisitor;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * An Operator that marks the boundary of a relation.
 * In relational algebra terms this is a "no-op" operator - it doesn't apply any modifications on the source relation.
 *
 * It is used to take care of the field mapping,
 * Across relation boundaries parent expression might have to be mapped to source expressions:
 * Example:
 *
 * <pre>
 *     select tt.bb from
 *          (select t.b + t.b as bb from t) tt
 *
 * Mapping:
 *      tt.bb -> t.b + t.b
 *
 * And reverse:
 *      t.b + t.b -> tt.bb
 * </pre>
 */
public class RelationBoundary extends ForwardingLogicalPlan {

    public static LogicalPlan.Builder create(LogicalPlan.Builder sourceBuilder, AnalyzedRelation relation) {
        return (tableStats, hints, usedBeforeNextFetch) -> {
            HashMap<Symbol, Symbol> expressionMapping = new HashMap<>();
            HashMap<Symbol, Symbol> reverseMapping = new HashMap<>();
            List<Field> fields = relation.fields();
            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                Symbol outputAtSamePosition = relation.outputs().get(i);
                expressionMapping.put(field, outputAtSamePosition);
                reverseMapping.put(outputAtSamePosition, field);
            }
            Function<Symbol, Symbol> mapper = OperatorUtils.getMapper(expressionMapping);
            HashSet<Symbol> mappedUsedColumns = new LinkedHashSet<>();
            for (Symbol beforeNextFetch : usedBeforeNextFetch) {
                mappedUsedColumns.add(mapper.apply(beforeNextFetch));
            }
            LogicalPlan source = sourceBuilder.build(tableStats, hints, mappedUsedColumns);
            for (Symbol symbol : source.outputs()) {
                RefVisitor.visitRefs(symbol, r -> {
                    Field field = new Field(relation, r.column(), r);
                    if (reverseMapping.putIfAbsent(r, field) == null) {
                        expressionMapping.put(field, r);
                    }
                });
                FieldsVisitor.visitFields(symbol, f -> {
                    Field field = new Field(relation, f.path(), f);
                    if (reverseMapping.putIfAbsent(f, field) == null) {
                        expressionMapping.put(field, f);
                    }
                });
            }
            List<Symbol> outputs = OperatorUtils.mappedSymbols(source.outputs(), reverseMapping);
            return new RelationBoundary(source, relation, outputs, reverseMapping);
        };
    }

    private final List<Symbol> outputs;

    private final AnalyzedRelation relation;
    private final Map<Symbol, Symbol> reverseMapping;

    public RelationBoundary(LogicalPlan source,
                            AnalyzedRelation relation,
                            List<Symbol> outputs,
                            Map<Symbol, Symbol> reverseMapping) {
        super(source);
        this.outputs = outputs;
        this.relation = relation;
        this.reverseMapping = reverseMapping;
    }

    @Override
    public Set<QualifiedName> getRelationNames() {
        return Set.of(relation.getQualifiedName());
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
        return source.build(
            plannerContext, projectionBuilder, limit, offset, order, pageSizeHint, params, subQueryResults);
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        LogicalPlan newSource = Lists2.getOnlyElement(sources);
        return new RelationBoundary(
            newSource,
            relation,
            OperatorUtils.mappedSymbols(newSource.outputs(), reverseMapping),
            reverseMapping
        );
    }

    @Override
    public String toString() {
        return "Boundary{" + source + '}';
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitRelationBoundary(this, context);
    }
}

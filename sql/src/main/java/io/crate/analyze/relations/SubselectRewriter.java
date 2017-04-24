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

package io.crate.analyze.relations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.crate.analyze.*;
import io.crate.analyze.symbol.Aggregations;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.ReplaceMode;
import io.crate.metadata.ReplacingSymbolVisitor;
import io.crate.operation.operator.AndOperator;
import io.crate.planner.Limits;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

final class SubselectRewriter {

    private final static Visitor INSTANCE = new Visitor();

    private SubselectRewriter() {
    }

    public static AnalyzedRelation rewrite(AnalyzedRelation relation) {
        return INSTANCE.process(relation, new Context(relation.fields()));
    }

    private static class Context {

        private final List<Field> fields;
        private QuerySpec currentParentQSpec;

        public Context(List<Field> fields) {
            this.fields = fields;
        }

        public List<Field> fields() {
            return fields;
        }
    }

    private final static class Visitor extends AnalyzedRelationVisitor<Context, AnalyzedRelation> {

        @Override
        protected AnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, Context context) {
            return relation;
        }

        @Override
        public AnalyzedRelation visitQueriedSelectRelation(QueriedSelectRelation relation, Context context) {
            QuerySpec querySpec = relation.querySpec();
            // Try to merge with parent query spec
            if (context.currentParentQSpec != null) {
                context.currentParentQSpec.replace(new FieldReplacer(querySpec.outputs()));
                if (canBeMerged(querySpec, context.currentParentQSpec)) {
                    querySpec = mergeQuerySpec(querySpec, context.currentParentQSpec);
                }
            }

            // Try to push down to the child
            context.currentParentQSpec = querySpec.copyAndReplace(Function.identity());
            AnalyzedRelation processedChildRelation = process(relation.subRelation(), context);

            // If cannot be pushed down replace qSpec with possibly merged qSpec from context
            if (processedChildRelation == null) {
                relation.querySpec(querySpec);
                return relation;
            } else { // If can be pushed down eliminate relation by return the processed child
                return processedChildRelation;
            }
        }

        @Override
        public AnalyzedRelation visitQueriedTable(QueriedTable table, Context context) {
            if (context.currentParentQSpec == null) {
                return table;
            }
            QuerySpec querySpec = table.querySpec();
            context.currentParentQSpec.replace(new FieldReplacer(querySpec.outputs()));
            if (!canBeMerged(querySpec, context.currentParentQSpec)) {
                return null;
            }

            querySpec = mergeQuerySpec(querySpec, context.currentParentQSpec);
            return new QueriedTable(table.tableRelation(), context.fields(), querySpec);
        }

        @Override
        public AnalyzedRelation visitQueriedDocTable(QueriedDocTable table, Context context) {
            if (context.currentParentQSpec == null) {
                return table;
            }
            QuerySpec querySpec = table.querySpec();
            context.currentParentQSpec.replace(new FieldReplacer(table.querySpec().outputs()));
            if (!canBeMerged(querySpec, context.currentParentQSpec)) {
                return null;
            }

            querySpec = mergeQuerySpec(querySpec, context.currentParentQSpec);
            return new QueriedDocTable(table.tableRelation(), context.fields(), querySpec);
        }

        @Override
        public AnalyzedRelation visitMultiSourceSelect(MultiSourceSelect multiSourceSelect, Context context) {
            if (context.currentParentQSpec == null) {
                return multiSourceSelect;
            }
            context.currentParentQSpec.replace(new FieldReplacer(multiSourceSelect.querySpec().outputs()));
            QuerySpec querySpec = multiSourceSelect.querySpec();
            if (!canBeMerged(querySpec, context.currentParentQSpec)) {
                return null;
            }

            querySpec = mergeQuerySpec(querySpec, context.currentParentQSpec);
            // must create a new MultiSourceSelect because paths and query spec changed
            return new MultiSourceSelect(
                Maps.transformValues(multiSourceSelect.sources(), RelationSource::relation),
                context.fields(),
                querySpec,
                multiSourceSelect.joinPairs());
        }

    }

    private static QuerySpec mergeQuerySpec(QuerySpec childQSpec, QuerySpec parentQSpec) {
        // merge everything: validation that merge is possible has already been done.
        OrderBy newOrderBy;
        if (parentQSpec.hasAggregates() || parentQSpec.groupBy().isPresent()) {
            // select avg(x) from (select x from t order by x)
            // -> can't keep order, but it doesn't matter for aggregations anyway so
            //    only keep the one from the parent Qspec
            newOrderBy = parentQSpec.orderBy().orElse(null);
        } else {
            newOrderBy = tryReplace(childQSpec.orderBy(), parentQSpec.orderBy());
        }
        return new QuerySpec()
            .outputs(parentQSpec.outputs())
            .where(mergeWhere(childQSpec.where(), parentQSpec.where()))
            .orderBy(newOrderBy)
            .offset(Limits.mergeAdd(childQSpec.offset(), parentQSpec.offset()))
            .limit(Limits.mergeMin(childQSpec.limit(), parentQSpec.limit()))
            .groupBy(pushGroupBy(childQSpec.groupBy(), parentQSpec.groupBy()))
            .having(pushHaving(childQSpec.having(), parentQSpec.having()))
            .hasAggregates(childQSpec.hasAggregates() || parentQSpec.hasAggregates());
    }


    private static WhereClause mergeWhere(WhereClause where1, WhereClause where2) {
        if (!where1.hasQuery() || where1 == WhereClause.MATCH_ALL) {
            return where2;
        } else if (!where2.hasQuery() || where2 == WhereClause.MATCH_ALL) {
            return where1;
        }

        return new WhereClause(AndOperator.join(ImmutableList.of(where2.query(), where1.query())));
    }

    /**
     * "Merge" OrderBy of child & parent relations.
     * <p/>
     * examples:
     * <pre>
     *      childOrderBy: col1, col2
     *      parentOrderBy: col2, col3, col4
     *
     *      merged OrderBy returned: col2, col3, col4
     * </pre>
     * <p/>
     * <pre>
     *      childOrderBy: col1, col2
     *      parentOrderBy:
     *
     *      merged OrderBy returned: col1, col2
     * </pre>
     *
     * @param childOrderBy  The OrderBy of the relation being processed
     * @param parentOrderBy The OrderBy of the parent relation (outer select,  union, etc.)
     * @return The merged orderBy
     */
    @Nullable
    private static OrderBy tryReplace(Optional<OrderBy> childOrderBy, Optional<OrderBy> parentOrderBy) {
        if (parentOrderBy.isPresent()) {
            return parentOrderBy.get();
        }
        return childOrderBy.orElse(null);
    }


    @Nullable
    private static List<Symbol> pushGroupBy(Optional<List<Symbol>> childGroupBy, Optional<List<Symbol>> parentGroupBy) {
        assert !(childGroupBy.isPresent() && parentGroupBy.isPresent()) :
            "Cannot merge 'group by' if exists in both parent and child relations";
        return childGroupBy.map(Optional::of).orElse(parentGroupBy).orElse(null);
    }

    @Nullable
    private static HavingClause pushHaving(Optional<HavingClause> childHaving, Optional<HavingClause> parentHaving) {
        assert !(childHaving.isPresent() && parentHaving.isPresent()) :
            "Cannot merge 'having' if exists in both parent and child relations";
        return childHaving.map(Optional::of).orElse(parentHaving).orElse(null);
    }

    private static boolean canBeMerged(QuerySpec childQuerySpec, QuerySpec parentQuerySpec) {
        WhereClause parentWhere = parentQuerySpec.where();
        boolean parentHasWhere = !parentWhere.equals(WhereClause.MATCH_ALL);
        boolean childHasLimit = childQuerySpec.limit().isPresent();
        if (parentHasWhere && childHasLimit) {
            return false;
        }

        boolean parentHasAggregations = parentQuerySpec.hasAggregates() || parentQuerySpec.groupBy().isPresent();
        boolean childHasAggregations = childQuerySpec.hasAggregates() || childQuerySpec.groupBy().isPresent();
        if (parentHasAggregations && (childHasLimit || childHasAggregations)) {
            return false;
        }

        Optional<OrderBy> childOrderBy = childQuerySpec.orderBy();
        Optional<OrderBy> parentOrderBy = parentQuerySpec.orderBy();
        if (childHasLimit && childOrderBy.isPresent() && parentOrderBy.isPresent() &&
            !childOrderBy.equals(parentOrderBy)) {
            return false;
        }
        if (parentHasWhere && parentWhere.hasQuery() && Aggregations.containsAggregation(parentWhere.query())) {
            return false;
        }
        return true;
    }

    private final static class FieldReplacer extends ReplacingSymbolVisitor<Void> implements Function<Symbol, Symbol> {

        private final List<Symbol> outputs;

        FieldReplacer(List<Symbol> outputs) {
            super(ReplaceMode.COPY);
            this.outputs = outputs;
        }

        @Override
        public Symbol visitField(Field field, Void context) {
            return outputs.get(field.index());
        }

        @Override
        public Symbol apply(Symbol symbol) {
            return process(symbol, null);
        }
    }
}

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

import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.crate.analyze.*;
import io.crate.analyze.symbol.Aggregations;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;
import io.crate.metadata.*;
import io.crate.operation.operator.AndOperator;
import io.crate.planner.Limits;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The RelationNormalizer tries to merge the tree of relations in a QueriedSelectRelation into a single QueriedRelation.
 * The merge occurs from the top level to the deepest one. For each level, it verifies if the query is mergeable with
 * the next relation and proceed with the merge if positive. When it is not, the partially merged tree is returned.
 */
final class RelationNormalizer {

    private RelationNormalizer() {
    }

    public static AnalyzedRelation normalize(AnalyzedRelation relation,
                                             Functions functions,
                                             TransactionContext transactionContext) {
        Context context = new Context(functions, relation.fields(), transactionContext);
        return NormalizerVisitor.normalize(SubselectRewriter.rewrite(relation, context), context);
    }

    private static Map<QualifiedName, AnalyzedRelation> mapSourceRelations(MultiSourceSelect multiSourceSelect) {
        return Maps.transformValues(multiSourceSelect.sources(), new com.google.common.base.Function<RelationSource, AnalyzedRelation>() {
            @Override
            public AnalyzedRelation apply(RelationSource input) {
                return input.relation();
            }
        });
    }

    private static QuerySpec mergeQuerySpec(QuerySpec childQSpec, @Nullable QuerySpec parentQSpec) {
        if (parentQSpec == null) {
            return childQSpec;
        }
        // merge everything: validation that merge is possible has already been done.
        OrderBy newOrderBy;
        if (parentQSpec.hasAggregates() || parentQSpec.groupBy().isPresent()) {
            // select avg(x) from (select x from t order by x)
            // -> can't keep order, but it doesn't matter for aggregations anyway so remove
            newOrderBy = null;
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
        return childOrderBy.orNull();
    }



    @Nullable
    private static List<Symbol> pushGroupBy(Optional<List<Symbol>> childGroupBy, Optional<List<Symbol>> parentGroupBy) {
        assert !(childGroupBy.isPresent() && parentGroupBy.isPresent()) :
            "Cannot merge 'group by' if exists in both parent and child relations";
        return childGroupBy.or(parentGroupBy).orNull();
    }

    @Nullable
    private static HavingClause pushHaving(Optional<HavingClause> childHaving, Optional<HavingClause> parentHaving) {
        assert !(childHaving.isPresent() && parentHaving.isPresent()) :
            "Cannot merge 'having' if exists in both parent and child relations";
        return childHaving.or(parentHaving).orNull();
    }

    private static boolean canBeMerged(QuerySpec childQuerySpec, QuerySpec parentQuerySpec) {
        if (parentQuerySpec == null) {
            return true;
        }
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
        if (childHasLimit && childOrderBy.isPresent() && parentOrderBy.isPresent() && !childOrderBy.equals(parentOrderBy)) {
            return false;
        }
        if (parentHasWhere && parentWhere.hasQuery() && Aggregations.containsAggregation(parentWhere.query())) {
            return false;
        }
        return true;
    }

    private static class Context {

        private final List<Field> fields;
        private final TransactionContext transactionContext;
        private final EvaluatingNormalizer normalizer;
        private final Functions functions;

        private QuerySpec currentParentQSpec;

        private Context(Functions functions,
                       List<Field> fields,
                       TransactionContext transactionContext) {
            this.functions = functions;
            this.normalizer = EvaluatingNormalizer.functionOnlyNormalizer(functions, ReplaceMode.COPY);
            this.fields = fields;
            this.transactionContext = transactionContext;
        }

        private Collection<? extends Path> paths() {
            return Collections2.transform(fields, Field::path);
        }
    }

    /**
     * Function to replace Fields with the Reference from the output of the relation the Field is pointing to.
     * E.g.
     *
     * <pre>
     * select t.x from (select x from t1) t
     *         |         |
     *       Field       \                  ____ Reference that is used as replacement.
     *          relation: t                /
     *                    +-- QS.outputs: [x]
     *                                     ^
     *                                     |
     *          index: 0 ------------------+
     *
     * </pre>
     */
    private static class FieldReferenceResolver extends ReplacingSymbolVisitor<Void>
        implements com.google.common.base.Function<Symbol, Symbol>{

        public static final FieldReferenceResolver INSTANCE = new FieldReferenceResolver(ReplaceMode.MUTATE);
        private static final FieldRelationVisitor<Symbol> FIELD_RELATION_VISITOR = new FieldRelationVisitor<>(INSTANCE);

        private FieldReferenceResolver(ReplaceMode mode) {
            super(mode);
        }

        @Override
        public Symbol visitField(Field field, Void context) {
            Symbol output = FIELD_RELATION_VISITOR.process(field.relation(), field);
            return output != null ? output : field;
        }

        @Nullable
        @Override
        public Symbol apply(@Nullable Symbol input) {
            if (input == null) {
                return null;
            }
            return process(input, null);
        }
    }

    /**
     * Visits an output symbol in a queried relation using the provided field index.
     */
    private static class FieldRelationVisitor<R> extends AnalyzedRelationVisitor<Field, R> {

        private final SymbolVisitor<?, R> symbolVisitor;

        FieldRelationVisitor(SymbolVisitor<?, R> symbolVisitor) {
            this.symbolVisitor = symbolVisitor;
        }

        @Override
        protected R visitAnalyzedRelation(AnalyzedRelation relation, Field context) {
            return null;
        }

        @Override
        public R visitQueriedTable(QueriedTable relation, Field field) {
            return visitQueriedRelation(relation, field);
        }

        @Override
        public R visitQueriedDocTable(QueriedDocTable relation, Field field) {
            return visitQueriedRelation(relation, field);
        }

        @Override
        public R visitMultiSourceSelect(MultiSourceSelect relation, Field field) {
            return visitQueriedRelation(relation, field);
        }

        @Override
        public R visitQueriedSelectRelation(QueriedSelectRelation relation, Field field) {
            return visitQueriedRelation(relation, field);
        }

        private R visitQueriedRelation(QueriedRelation relation, Field field) {
            Symbol output = relation.querySpec().outputs().get(field.index());
            return symbolVisitor.process(output, null);
        }
    }

    private static class SubselectRewriter extends AnalyzedRelationVisitor<RelationNormalizer.Context, AnalyzedRelation> {

        private static final SubselectRewriter SUBSELECT_REWRITER = new SubselectRewriter();

        private SubselectRewriter() {
        }

        public static AnalyzedRelation rewrite(AnalyzedRelation relation, Context context) {
            return SUBSELECT_REWRITER.process(relation, context);
        }

        @Override
        protected AnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, Context context) {
            return relation;
        }

        @Override
        public AnalyzedRelation visitQueriedSelectRelation(QueriedSelectRelation relation, Context context) {
            QuerySpec querySpec = relation.querySpec();
            // Try to merge with parent query spec
            if (canBeMerged(querySpec, context.currentParentQSpec)) {
                querySpec = mergeQuerySpec(querySpec, context.currentParentQSpec);
            }

            // Try to push down to the child
            context.currentParentQSpec = querySpec.copyAndReplace(i -> i);
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
            context.currentParentQSpec.replace(FieldReferenceResolver.INSTANCE);
            if (!canBeMerged(querySpec, context.currentParentQSpec)) {
                return null;
            }

            querySpec = mergeQuerySpec(querySpec, context.currentParentQSpec);
            return new QueriedTable(table.relationId(), table.tableRelation(), context.paths(), querySpec);
        }

        @Override
        public AnalyzedRelation visitQueriedDocTable(QueriedDocTable table, Context context) {
            if (context.currentParentQSpec == null) {
                return table;
            }
            QuerySpec querySpec = table.querySpec();
            context.currentParentQSpec.replace(FieldReferenceResolver.INSTANCE);
            if (!canBeMerged(querySpec, context.currentParentQSpec)) {
                return null;
            }

            querySpec = mergeQuerySpec(querySpec, context.currentParentQSpec);
            return new QueriedDocTable(table.relationId(), table.tableRelation(), context.paths(), querySpec);
        }

        @Override
        public AnalyzedRelation visitMultiSourceSelect(MultiSourceSelect multiSourceSelect, Context context) {
            if (context.currentParentQSpec == null) {
                return multiSourceSelect;
            }
            context.currentParentQSpec.replace(FieldReferenceResolver.INSTANCE);
            QuerySpec querySpec = multiSourceSelect.querySpec();
            if (!canBeMerged(querySpec, context.currentParentQSpec)) {
                return null;
            }

            querySpec = mergeQuerySpec(querySpec, context.currentParentQSpec);
            // must create a new MultiSourceSelect because paths and query spec changed
            return new MultiSourceSelect(
                multiSourceSelect.relationId(),
                mapSourceRelations(multiSourceSelect),
                multiSourceSelect.outputSymbols(),
                context.paths(),
                querySpec,
                multiSourceSelect.joinPairs());
        }
    }

    private static class NormalizerVisitor extends AnalyzedRelationVisitor<RelationNormalizer.Context, AnalyzedRelation> {

        private static final NormalizerVisitor NORMALIZER = new NormalizerVisitor();

        private NormalizerVisitor() {
        }

        public static AnalyzedRelation normalize(AnalyzedRelation relation, Context context) {
            return NORMALIZER.process(relation, context);
        }

        @Override
        protected AnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, Context context) {
            return relation;
        }

        @Override
        public AnalyzedRelation visitQueriedSelectRelation(QueriedSelectRelation relation, Context context) {
            relation.subRelation((QueriedRelation) process(relation.subRelation(), context));
            return relation;
        }

        @Override
        public AnalyzedRelation visitQueriedTable(QueriedTable table, Context context) {
            table.normalize(context.functions, context.transactionContext);
            return table;
        }

        @Override
        public AnalyzedRelation visitQueriedDocTable(QueriedDocTable table, Context context) {
            table.normalize(context.functions, context.transactionContext);
            table.analyzeWhereClause(context.functions, context.transactionContext);
            return table;
        }

        @Override
        public AnalyzedRelation visitMultiSourceSelect(MultiSourceSelect multiSourceSelect, Context context) {
            QuerySpec querySpec = multiSourceSelect.querySpec();
            querySpec.normalize(context.normalizer, context.transactionContext);
            // must create a new MultiSourceSelect because paths and query spec changed
            multiSourceSelect = new MultiSourceSelect(
                multiSourceSelect.relationId(),
                mapSourceRelations(multiSourceSelect),
                multiSourceSelect.outputSymbols(),
                context.paths(),
                querySpec,
                multiSourceSelect.joinPairs());
            multiSourceSelect.pushDownQuerySpecs();
            return multiSourceSelect;
        }
    }
}

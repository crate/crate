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
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;
import io.crate.metadata.*;
import io.crate.operation.operator.AndOperator;
import io.crate.operation.scalar.conditional.LeastFunction;
import io.crate.planner.Limits;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The RelationNormalizer tries to merge the tree of relations in a QueriedSelectRelation into a single QueriedRelation.
 * The merge occurs from the top level to the deepest one. For each level, it verifies if the query is mergeable with
 * the next relation and proceed with the merge if positive. When it is not, the partially merged tree is returned.
 */
final class RelationNormalizer extends AnalyzedRelationVisitor<RelationNormalizer.Context, AnalyzedRelation> {

    private static final RelationNormalizer INSTANCE = new RelationNormalizer();

    private RelationNormalizer() {
    }

    public static AnalyzedRelation normalize(AnalyzedRelation relation,
                                             Functions functions,
                                             TransactionContext transactionContext) {
        return INSTANCE.process(relation, new Context(functions, relation.fields(), transactionContext));
    }

    @Override
    protected AnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, Context context) {
        return relation;
    }

    @Override
    public AnalyzedRelation visitQueriedSelectRelation(QueriedSelectRelation relation, Context context) {
        if (hasNestedAggregations(relation)) {
            return relation;
        }

        context.querySpec = mergeQuerySpec(context.querySpec, relation.querySpec());
        return process(relation.relation(), context);
    }

    @Override
    public AnalyzedRelation visitQueriedTable(QueriedTable table, Context context) {
        if (context.querySpec == null) {
            return table;
        }

        QuerySpec querySpec = mergeAndReplaceFields(table, context.querySpec);
        QueriedTable relation = new QueriedTable(table.tableRelation(), context.paths(), querySpec);
        relation.normalize(context.functions, context.transactionContext);
        return relation;
    }

    @Override
    public AnalyzedRelation visitQueriedDocTable(QueriedDocTable table, Context context) {
        QueriedDocTable relation = table;
        if (context.querySpec != null) {
            QuerySpec querySpec = mergeAndReplaceFields(table, context.querySpec);
            relation = new QueriedDocTable(table.tableRelation(), context.paths(), querySpec);
            relation.normalize(context.functions, context.transactionContext);
        }
        relation.analyzeWhereClause(context.functions, context.transactionContext);
        return relation;
    }

    @Override
    public AnalyzedRelation visitMultiSourceSelect(MultiSourceSelect multiSourceSelect, Context context) {
        MultiSourceSelect relation = multiSourceSelect;
        multiSourceSelect.querySpec().normalize(context.normalizer, context.transactionContext);
        if (context.querySpec != null) {
            QuerySpec querySpec = mergeAndReplaceFields(multiSourceSelect, context.querySpec);
            // must create a new MultiSourceSelect because paths and query spec changed
            relation = new MultiSourceSelect(mapSourceRelations(multiSourceSelect),
                relation.outputSymbols(), context.paths(), querySpec,
                multiSourceSelect.joinPairs());
        }
        relation.pushDownQuerySpecs();
        return relation;
    }

    private Map<QualifiedName, AnalyzedRelation> mapSourceRelations(MultiSourceSelect multiSourceSelect) {
        return Maps.transformValues(multiSourceSelect.sources(), new com.google.common.base.Function<MultiSourceSelect.Source, AnalyzedRelation>() {
            @Override
            public AnalyzedRelation apply(MultiSourceSelect.Source input) {
                return input.relation();
            }
        });
    }

    private QuerySpec mergeAndReplaceFields(QueriedRelation table, QuerySpec querySpec) {
        QuerySpec mergedQuerySpec = mergeQuerySpec(querySpec, table.querySpec());
        replaceFieldReferences(mergedQuerySpec);
        return mergedQuerySpec;
    }

    private static QuerySpec mergeQuerySpec(@Nullable QuerySpec querySpec1, QuerySpec querySpec2) {
        if (querySpec1 == null) {
            return querySpec2;
        }

        return new QuerySpec()
            .outputs(querySpec1.outputs())
            .where(mergeWhere(querySpec1.where(), querySpec2.where()))
            .orderBy(mergeOrderBy(querySpec1.orderBy(), querySpec2.orderBy()))
            .offset(Limits.add(querySpec1.offset(), querySpec2.offset()))
            .limit(mergeLimit(querySpec1.limit(), querySpec2.limit()))
            .groupBy(pushGroupBy(querySpec1.groupBy(), querySpec2.groupBy()))
            .having(pushHaving(querySpec1.having(), querySpec2.having()))
            .hasAggregates(querySpec1.hasAggregates() || querySpec2.hasAggregates());
    }

    private static WhereClause mergeWhere(WhereClause where1, WhereClause where2) {
        if (!where1.hasQuery() || where1 == WhereClause.MATCH_ALL) {
            return where2;
        } else if (!where2.hasQuery() || where2 == WhereClause.MATCH_ALL) {
            return where1;
        }

        return new WhereClause(AndOperator.join(ImmutableList.of(where2.query(), where1.query())));
    }

    @Nullable
    private static OrderBy mergeOrderBy(Optional<OrderBy> o1, Optional<OrderBy> o2) {
        if (!o1.isPresent() || !o2.isPresent()) {
            return o1.or(o2).orNull();
        }
        return o1.get().merge(o2.get());
    }

    private static Optional<Symbol> mergeLimit(Optional<Symbol> limit1, Optional<Symbol> limit2) {
        if (limit1.isPresent()) {
            if (limit2.isPresent()) {
                return Optional.of((Symbol) new Function(LeastFunction.TWO_INT_INFO, Arrays.asList(limit1.get(), limit2.get())));
            }
            return limit1;
        }
        return limit2;
    }

    @Nullable
    private static List<Symbol> pushGroupBy(Optional<List<Symbol>> groupBy1, Optional<List<Symbol>> groupBy2) {
        return groupBy1.or(groupBy2).orNull();
    }

    @Nullable
    private static HavingClause pushHaving(Optional<HavingClause> having1, Optional<HavingClause> having2) {
        return having1.or(having2).orNull();
    }

    private static void replaceFieldReferences(QuerySpec querySpec) {
        querySpec.outputs(FieldReferenceResolver.INSTANCE.process(querySpec.outputs(), null));

        if (querySpec.where().hasQuery() && !querySpec.where().noMatch()) {
            Symbol query = FieldReferenceResolver.INSTANCE.process(querySpec.where().query(), null);
            querySpec.where(new WhereClause(query));
        }

        if (querySpec.orderBy().isPresent()) {
            OrderBy orderBy = querySpec.orderBy().get();
            List<Symbol> orderBySymbols = FieldReferenceResolver.INSTANCE.process(orderBy.orderBySymbols(), null);
            querySpec.orderBy(new OrderBy(orderBySymbols, orderBy.reverseFlags(), orderBy.nullsFirst()));
        }

        if (querySpec.groupBy().isPresent()) {
            List<Symbol> groupBy = FieldReferenceResolver.INSTANCE.process(querySpec.groupBy().get(), null);
            querySpec.groupBy(groupBy);
        }

        if (querySpec.having().isPresent() && !querySpec.having().get().noMatch()) {
            Symbol query = FieldReferenceResolver.INSTANCE.process(querySpec.having().get().query(), null);
            querySpec.having(new HavingClause(query));
        }
    }

    private boolean hasNestedAggregations(QueriedSelectRelation relation) {
        QuerySpec querySpec1 = relation.querySpec();
        QuerySpec querySpec2 = relation.relation().querySpec();

        boolean hasAggregations = (querySpec1.hasAggregates() || querySpec1.groupBy().isPresent()) &&
                                  (querySpec2.hasAggregates() || querySpec2.groupBy().isPresent() ||
                                   querySpec2.orderBy().isPresent());

        return hasAggregations || querySpec1.where().hasQuery() && querySpec1.where() != WhereClause.MATCH_ALL &&
                                  AggregateFunctionReferenceFinder.any(querySpec1.where().query());

    }

    static class Context {
        private final List<Field> fields;
        private final TransactionContext transactionContext;
        private final EvaluatingNormalizer normalizer;
        private final Functions functions;

        private QuerySpec querySpec;

        public Context(Functions functions,
                       List<Field> fields,
                       TransactionContext transactionContext) {
            this.functions = functions;
            this.normalizer = EvaluatingNormalizer.functionOnlyNormalizer(functions, ReplaceMode.COPY);
            this.fields = fields;
            this.transactionContext = transactionContext;
        }

        public Collection<? extends Path> paths() {
            return Collections2.transform(fields, Field.TO_PATH);
        }
    }

    /**
     * Used after merging to replace fields with the symbol referenced in their relation.
     */
    private static class FieldReferenceResolver extends ReplacingSymbolVisitor<Void> {

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
    }

    /**
     * Checks if a field references an aggregate function on its relation.
     */
    private static class AggregateFunctionReferenceFinder extends SymbolVisitor<Void, Boolean> {

        private static final AggregateFunctionReferenceFinder INSTANCE = new AggregateFunctionReferenceFinder();
        private static final FieldRelationVisitor<Boolean> FIELD_RELATION_VISITOR = new FieldRelationVisitor<>(INSTANCE);

        public static Boolean any(Symbol symbol) {
            return INSTANCE.process(symbol, null);
        }

        @Override
        protected Boolean visitSymbol(Symbol symbol, Void context) {
            return false;
        }

        @Override
        public Boolean visitFunction(Function symbol, Void context) {
            if (FunctionInfo.Type.AGGREGATE.equals(symbol.info().type())) {
                return true;
            }
            for (Symbol arg : symbol.arguments()) {
                if (process(arg, context)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visitField(Field field, Void context) {
            Boolean result = FIELD_RELATION_VISITOR.process(field.relation(), field);
            return (result != null) && result;
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
}

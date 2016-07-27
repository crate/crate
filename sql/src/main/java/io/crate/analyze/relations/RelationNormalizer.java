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

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Booleans;
import io.crate.analyze.*;
import io.crate.analyze.symbol.*;
import io.crate.exceptions.AmbiguousOrderByException;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Path;
import io.crate.metadata.ReplacingSymbolVisitor;
import io.crate.operation.operator.AndOperator;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

class RelationNormalizer extends AnalyzedRelationVisitor<RelationNormalizer.Context, QueriedRelation> {

    private static final RelationNormalizer INSTANCE = new RelationNormalizer();

    private static final Predicate<Symbol> IS_AGGREGATE_FUNCTION = new Predicate<Symbol>() {
        @Override
        public boolean apply(@Nullable Symbol input) {
            return (input instanceof Function) &&
                   FunctionInfo.Type.AGGREGATE.equals(((Function) input).info().type());
        }
    };

    public static QueriedRelation normalize(AnalyzedRelation relation, AnalysisMetaData analysisMetaData) {
        return INSTANCE.process(relation, new Context(analysisMetaData, relation.fields()));
    }

    @Override
    public QueriedRelation visitQueriedSelectRelation(QueriedSelectRelation relation, Context context) {
        if (hasNestedAggregations(relation)) {
            return relation;
        }

        context.querySpec = mergeQuerySpec(context.querySpec, relation.querySpec());
        return process(relation.relation(), context);
    }

    @Override
    public QueriedRelation visitQueriedTable(QueriedTable table, Context context) {
        mergeTableRelation(table, context);

        QueriedTable relation = new QueriedTable(table.tableRelation(), context.paths(), context.querySpec);
        relation.normalize(context.analysisMetaData);
        return relation;
    }

    @Override
    public QueriedRelation visitQueriedDocTable(QueriedDocTable table, Context context) {
        mergeTableRelation(table, context);

        QueriedDocTable relation = new QueriedDocTable(table.tableRelation(), context.paths(), context.querySpec);
        relation.normalize(context.analysisMetaData);
        relation.analyzeWhereClause(context.analysisMetaData);
        return relation;
    }

    private void mergeTableRelation(QueriedTableRelation table, Context context) {
        context.querySpec = mergeQuerySpec(context.querySpec, table.querySpec());
        replaceFieldReferences(context.querySpec);
    }

    private static QuerySpec mergeQuerySpec(@Nullable QuerySpec querySpec1, QuerySpec querySpec2) {
        if (querySpec1 == null) {
            return querySpec2;
        }

        QuerySpec querySpec = new QuerySpec()
            .outputs(querySpec1.outputs())
            .where(mergeWhere(querySpec1, querySpec2))
            .orderBy(mergeOrderBy(querySpec1, querySpec2))
            .offset(mergeOffset(querySpec1, querySpec2))
            .limit(mergeLimit(querySpec1, querySpec2))
            .groupBy(pushGroupBy(querySpec1, querySpec2))
            .having(pushHaving(querySpec1, querySpec2))
            .hasAggregates(querySpec1.hasAggregates() || querySpec2.hasAggregates());

        return querySpec;
    }

    private static WhereClause mergeWhere(QuerySpec querySpec1, QuerySpec querySpec2) {
        WhereClause where1 = querySpec1.where();
        WhereClause where2 = querySpec2.where();

        if (!where1.hasQuery() || where1 == WhereClause.MATCH_ALL) {
            return where2;
        } else if (!where2.hasQuery() || where2 == WhereClause.MATCH_ALL) {
            return where1;
        }

        return new WhereClause(AndOperator.join(ImmutableList.of(where2.query(), where1.query())));
    }

    @Nullable
    private static OrderBy mergeOrderBy(QuerySpec querySpec1, QuerySpec querySpec2) {
        if (!querySpec1.orderBy().isPresent()) {
            return querySpec2.orderBy().orNull();
        } else if (!querySpec2.orderBy().isPresent()) {
            return querySpec1.orderBy().orNull();
        }

        OrderBy orderBy1 = querySpec1.orderBy().get();
        OrderBy orderBy2 = querySpec2.orderBy().get();

        List<Symbol> orderBySymbols = orderBy2.orderBySymbols();
        List<Boolean> reverseFlags = new ArrayList<>(Booleans.asList(orderBy2.reverseFlags()));
        List<Boolean> nullsFirst = new ArrayList<>(Arrays.asList(orderBy2.nullsFirst()));

        for (int i = 0; i < orderBy1.orderBySymbols().size(); i++) {
            Symbol orderBySymbol = orderBy1.orderBySymbols().get(i);
            int idx = orderBySymbols.indexOf(orderBySymbol);
            if (idx == -1) {
                orderBySymbols.add(orderBySymbol);
                reverseFlags.add(orderBy1.reverseFlags()[i]);
                nullsFirst.add(orderBy1.nullsFirst()[i]);
            } else {
                if (reverseFlags.get(idx) != orderBy1.reverseFlags()[i]) {
                    throw new AmbiguousOrderByException(orderBySymbol);
                }
                if (nullsFirst.get(idx) != orderBy1.nullsFirst()[i]) {
                    throw new AmbiguousOrderByException(orderBySymbol);
                }
            }
        }

        return new OrderBy(orderBySymbols, Booleans.toArray(reverseFlags), nullsFirst.toArray(new Boolean[0]));
    }

    private static int mergeOffset(QuerySpec querySpec1, QuerySpec querySpec2) {
        return querySpec1.offset() + querySpec2.offset();
    }

    @Nullable
    private static Integer mergeLimit(QuerySpec querySpec1, QuerySpec querySpec2) {
        if (!querySpec1.limit().isPresent()) {
            return querySpec2.limit().orNull();
        } else if (!querySpec2.limit().isPresent()) {
            return querySpec1.limit().orNull();
        }

        Integer limit1 = querySpec1.limit().or(0);
        Integer limit2 = querySpec2.limit().or(0);

        return Math.min(limit1, limit2);
    }

    @Nullable
    private static List<Symbol> pushGroupBy(QuerySpec querySpec1, QuerySpec querySpec2) {
        return querySpec1.groupBy().or(querySpec2.groupBy()).orNull();
    }

    @Nullable
    private static HavingClause pushHaving(QuerySpec querySpec1, QuerySpec querySpec2) {
        return querySpec1.having().or(querySpec2.having()).orNull();
    }

    private static void replaceFieldReferences(QuerySpec querySpec) {
        querySpec.outputs(FieldReplacingVisitor.INSTANCE.process(querySpec.outputs(), null));

        if (querySpec.where().hasQuery() && !querySpec.where().noMatch()) {
            Symbol query = FieldReplacingVisitor.INSTANCE.process(querySpec.where().query(), null);
            querySpec.where(new WhereClause(query));
        }

        if (querySpec.orderBy().isPresent()) {
            OrderBy orderBy = querySpec.orderBy().get();
            List<Symbol> orderBySymbols = FieldReplacingVisitor.INSTANCE.process(orderBy.orderBySymbols(), null);
            querySpec.orderBy(new OrderBy(orderBySymbols, orderBy.reverseFlags(), orderBy.nullsFirst()));
        }

        if (querySpec.groupBy().isPresent()) {
            List<Symbol> groupBy = FieldReplacingVisitor.INSTANCE.process(querySpec.groupBy().get(), null);
            querySpec.groupBy(groupBy);
        }

        if (querySpec.having().isPresent() && !querySpec.having().get().noMatch()) {
            Symbol query = FieldReplacingVisitor.INSTANCE.process(querySpec.having().get().query(), null);
            querySpec.having(new HavingClause(query));
        }
    }

    private boolean hasNestedAggregations(QueriedSelectRelation relation) {
        QuerySpec querySpec1 = relation.querySpec();
        QuerySpec querySpec2 = relation.relation().querySpec();

        if ((querySpec1.hasAggregates() || querySpec1.groupBy().isPresent()) &&
            (querySpec2.hasAggregates() || querySpec2.groupBy().isPresent() || querySpec2.orderBy().isPresent())) {
            return true;
        }

        return querySpec1.where().hasQuery() && querySpec1.where() != WhereClause.MATCH_ALL &&
               AggregateFunctionReferenceVisitor.any(querySpec1.where().query());
    }

    static class Context {
        private final AnalysisMetaData analysisMetaData;
        private final List<Field> fields;

        private QuerySpec querySpec;

        public Context(AnalysisMetaData analysisMetaData, List<Field> fields) {
            this.analysisMetaData = analysisMetaData;
            this.fields = fields;
        }

        public Collection<? extends Path> paths() {
            return Collections2.transform(fields, new com.google.common.base.Function<Field, Path>() {
                @Override
                public Path apply(Field input) {
                    return input.path();
                }
            });
        }
    }

    private static class FieldReplacingVisitor extends ReplacingSymbolVisitor<Void> {

        public static final FieldReplacingVisitor INSTANCE = new FieldReplacingVisitor(true);
        public static final FieldRelationVisitor<Symbol> FIELD_RELATION_VISITOR = new FieldRelationVisitor(INSTANCE);

        private FieldReplacingVisitor(boolean inPlace) {
            super(inPlace);
        }

        @Override
        public Symbol visitField(Field field, Void context) {
            return FIELD_RELATION_VISITOR.process(field.relation(), field, field);
        }
    }

    private static class AggregateFunctionReferenceVisitor extends SymbolVisitor<Void, Boolean> {

        public static final AggregateFunctionReferenceVisitor INSTANCE = new AggregateFunctionReferenceVisitor();
        public static final FieldRelationVisitor<Boolean> FIELD_RELATION_VISITOR = new FieldRelationVisitor(INSTANCE);

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
            return FIELD_RELATION_VISITOR.process(field.relation(), field, false);
        }
    }

    /**
     * Visits an output symbol in a queried relation using the provided field index.
     */
    private static class FieldRelationVisitor<R> extends AnalyzedRelationVisitor<FieldRelationVisitorContext<R>, R> {

        private final SymbolVisitor<Void, R> symbolVisitor;

        public FieldRelationVisitor(SymbolVisitor<Void, R> symbolVisitor) {
            this.symbolVisitor = symbolVisitor;
        }

        public R process(AnalyzedRelation relation, Field field, R defaultReturnValue) {
            return process(relation, new FieldRelationVisitorContext<>(field.index(), defaultReturnValue));
        }

        @Override
        protected R visitAnalyzedRelation(AnalyzedRelation relation, FieldRelationVisitorContext<R> context) {
            return context.defaultReturnValue;
        }

        @Override
        public R visitQueriedTable(QueriedTable relation, FieldRelationVisitorContext<R> context) {
            return visitQueriedRelation(relation, context);
        }

        @Override
        public R visitQueriedDocTable(QueriedDocTable relation, FieldRelationVisitorContext<R> context) {
            return visitQueriedRelation(relation, context);
        }

        @Override
        public R visitMultiSourceSelect(MultiSourceSelect relation, FieldRelationVisitorContext<R> context) {
            return visitQueriedRelation(relation, context);
        }

        @Override
        public R visitQueriedSelectRelation(QueriedSelectRelation relation, FieldRelationVisitorContext<R> context) {
            return visitQueriedRelation(relation, context);
        }

        private R visitQueriedRelation(QueriedRelation relation, FieldRelationVisitorContext<R> context) {
            Symbol output = relation.querySpec().outputs().get(context.fieldIndex);
            return symbolVisitor.process(output, null);
        }
    }

    private static class FieldRelationVisitorContext<R> {
        private final int fieldIndex;
        private final R defaultReturnValue;

        public FieldRelationVisitorContext(int fieldIndex, R defaultReturnValue) {
            this.fieldIndex = fieldIndex;
            this.defaultReturnValue = defaultReturnValue;
        }
    }
}

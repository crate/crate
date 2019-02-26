/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.lucene;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.crate.data.Input;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersionInvalidException;
import io.crate.execution.engine.collect.DocInputFactory;
import io.crate.execution.engine.collect.collectors.CollectorFieldsVisitor;
import io.crate.expression.InputFactory;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.GtOperator;
import io.crate.expression.operator.GteOperator;
import io.crate.expression.operator.LikeOperator;
import io.crate.expression.operator.LtOperator;
import io.crate.expression.operator.LteOperator;
import io.crate.expression.operator.OrOperator;
import io.crate.expression.operator.RegexpMatchCaseInsensitiveOperator;
import io.crate.expression.operator.RegexpMatchOperator;
import io.crate.expression.operator.any.AnyLikeOperator;
import io.crate.expression.operator.any.AnyOperators;
import io.crate.expression.predicate.IsNullPredicate;
import io.crate.expression.predicate.MatchPredicate;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.scalar.ArrayUpperFunction;
import io.crate.expression.scalar.Ignore3vlFunction;
import io.crate.expression.scalar.SubscriptFunction;
import io.crate.expression.scalar.conditional.CoalesceFunction;
import io.crate.expression.scalar.geo.DistanceFunction;
import io.crate.expression.scalar.geo.WithinFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.format.SymbolFormatter;
import io.crate.expression.symbol.format.SymbolPrinter;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.DocReferences;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.crate.expression.eval.NullEliminator.eliminateNullsIfPossible;
import static io.crate.metadata.DocReferences.inverseSourceLookup;


@Singleton
public class LuceneQueryBuilder {

    private static final Logger LOGGER = LogManager.getLogger(LuceneQueryBuilder.class);
    private static final Visitor VISITOR = new Visitor();
    private final Functions functions;
    private final EvaluatingNormalizer normalizer;

    @Inject
    public LuceneQueryBuilder(Functions functions) {
        this.functions = functions;
        normalizer = EvaluatingNormalizer.functionOnlyNormalizer(functions);
    }

    public Context convert(Symbol query,
                           TransactionContext txnCtx,
                           MapperService mapperService,
                           QueryShardContext queryShardContext,
                           IndexCache indexCache) throws UnsupportedFeatureException {
        Context ctx = new Context(txnCtx, functions, mapperService, indexCache, queryShardContext);
        CoordinatorTxnCtx coordinatorTxnCtx = CoordinatorTxnCtx.systemTransactionContext();
        ctx.query = VISITOR.process(
            eliminateNullsIfPossible(inverseSourceLookup(query), s -> normalizer.normalize(s, coordinatorTxnCtx)),
            ctx
        );
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("WHERE CLAUSE [{}] -> LUCENE QUERY [{}] ", SymbolPrinter.INSTANCE.printUnqualified(query), ctx.query);
        }
        return ctx;
    }

    static Query termsQuery(@Nullable MappedFieldType fieldType, List values, QueryShardContext context) {
        if (fieldType == null) {
            return Queries.newMatchNoDocsQuery("column does not exist in this index");
        }
        return fieldType.termsQuery(values, context);
    }

    static List asList(Literal literal) {
        Object val = literal.value();
        if (val instanceof Object[]) {
            return Stream.of((Object[]) val).filter(Objects::nonNull).collect(Collectors.toList());
        }
        return (List) val;
    }

    public static class Context {
        Query query;

        final Map<String, Object> filteredFieldValues = new HashMap<>();

        final DocInputFactory docInputFactory;
        final MapperService mapperService;
        final IndexCache indexCache;
        private final TransactionContext txnCtx;
        final QueryShardContext queryShardContext;

        Context(TransactionContext txnCtx,
                Functions functions,
                MapperService mapperService,
                IndexCache indexCache,
                QueryShardContext queryShardContext) {
            this.txnCtx = txnCtx;
            this.queryShardContext = queryShardContext;
            FieldTypeLookup typeLookup = mapperService::fullName;
            this.docInputFactory = new DocInputFactory(
                functions,
                typeLookup,
                new LuceneReferenceResolver(typeLookup, mapperService.getIndexSettings()));
            this.mapperService = mapperService;
            this.indexCache = indexCache;
        }

        public Query query() {
            return this.query;
        }

        @Nullable
        public Float minScore() {
            Object score = filteredFieldValues.get("_score");
            if (score == null) {
                return null;
            }
            return ((Number) score).floatValue();
        }

        @Nullable
        public String unsupportedMessage(String field) {
            return UNSUPPORTED_FIELDS.get(field);
        }

        /**
         * These fields are ignored in the whereClause.
         * If a filtered field is encountered the value of the literal is written into filteredFieldValues
         * (only applies to Function with 2 arguments and if left == reference and right == literal)
         */
        static final Set<String> FILTERED_FIELDS = ImmutableSet.of("_score");

        /**
         * key = columnName
         * value = error message
         * <p/>
         * (in the _version case if the primary key is present a GetPlan is built from the planner and
         * the LuceneQueryBuilder is never used)
         */
        static final Map<String, String> UNSUPPORTED_FIELDS = ImmutableMap.<String, String>builder()
            .put("_version", VersionInvalidException.ERROR_MSG)
            .build();

        @Nullable
        MappedFieldType getFieldTypeOrNull(String fqColumnName) {
            return mapperService.fullName(fqColumnName);
        }

        public QueryShardContext queryShardContext() {
            return queryShardContext;
        }
    }


    static class Visitor extends SymbolVisitor<Context, Query> {

        class Ignore3vlQuery implements FunctionToQuery {

            @Nullable
            @Override
            public Query apply(Function input, Context context) {
                List<Symbol> args = input.arguments();
                assert args.size() == 1 : "ignore3vl expects exactly 1 argument, got: " + args.size();
                return process(args.get(0), context);
            }
        }

        class NotQuery implements FunctionToQuery {

            private class SymbolToNotNullContext {
                private final HashSet<Reference> references = new HashSet<>();
                boolean hasStrictThreeValuedLogicFunction = false;

                boolean add(Reference symbol) {
                    return references.add(symbol);
                }

                Set<Reference> references() {
                    return references;
                }
            }

            private class SymbolToNotNullRangeQueryArgs extends SymbolVisitor<SymbolToNotNullContext, Void> {

                /**
                 * Three valued logic systems are defined in logic as systems in which there are 3 truth values: true,
                 * false and an indeterminate third value (in our case null is the third value).
                 * <p>
                 * This is a set of functions for which inputs evaluating to null needs to be explicitly included or
                 * excluded (in the case of 'not ...') in the boolean queries
                 */
                private final Set<String> STRICT_3VL_FUNCTIONS =
                    ImmutableSet.of(
                        AnyOperators.Names.EQ,
                        AnyOperators.Names.NEQ,
                        AnyOperators.Names.GTE,
                        AnyOperators.Names.GT,
                        AnyOperators.Names.LTE,
                        AnyOperators.Names.LT,
                        AnyLikeOperator.LIKE,
                        AnyLikeOperator.NOT_LIKE,
                        CoalesceFunction.NAME);

                private boolean isStrictThreeValuedLogicFunction(Function symbol) {
                    return STRICT_3VL_FUNCTIONS.contains(symbol.info().ident().name());
                }

                private boolean isIgnoreThreeValuedLogicFunction(Function symbol) {
                    return Ignore3vlFunction.NAME.equals(symbol.info().ident().name());
                }

                @Override
                public Void visitReference(Reference symbol, SymbolToNotNullContext context) {
                    context.add(symbol);
                    return null;
                }

                @Override
                public Void visitFunction(Function symbol, SymbolToNotNullContext context) {
                    if (isIgnoreThreeValuedLogicFunction(symbol)) {
                        return null;
                    }

                    if (!isStrictThreeValuedLogicFunction(symbol)) {
                        for (Symbol arg : symbol.arguments()) {
                            process(arg, context);
                        }
                    } else {
                        context.hasStrictThreeValuedLogicFunction = true;
                    }
                    return null;
                }
            }

            private final SymbolToNotNullRangeQueryArgs INNER_VISITOR = new SymbolToNotNullRangeQueryArgs();

            @Override
            public Query apply(Function input, Context context) {
                assert input != null : "function must not be null";
                assert input.arguments().size() == 1 : "function's number of arguments must be 1";
                /**
                 * not null -> null     -> no match
                 * not true -> false    -> no match
                 * not false -> true    -> match
                 */

                // handles not true / not false
                Symbol arg = input.arguments().get(0);
                Query innerQuery = process(arg, context);
                Query notX = Queries.not(innerQuery);

                // not x =  not x & x is not null
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                builder.add(notX, BooleanClause.Occur.MUST);

                SymbolToNotNullContext ctx = new SymbolToNotNullContext();
                INNER_VISITOR.process(arg, ctx);
                for (Reference reference : ctx.references()) {
                    String columnName = reference.column().fqn();
                    MappedFieldType fieldType = context.getFieldTypeOrNull(columnName);
                    if (fieldType == null) {
                        // probably an object column, fallback to genericFunctionFilter
                        return null;
                    }
                    if (reference.isNullable()) {
                        builder.add(ExistsQueryBuilder.newFilter(context.queryShardContext, columnName), BooleanClause.Occur.MUST);
                    }
                }
                if (ctx.hasStrictThreeValuedLogicFunction) {
                    FunctionInfo isNullInfo = IsNullPredicate.generateInfo(Collections.singletonList(arg.valueType()));
                    Function isNullFunction = new Function(isNullInfo, Collections.singletonList(arg));
                    builder.add(
                        Queries.not(genericFunctionFilter(isNullFunction, context)),
                        BooleanClause.Occur.MUST
                    );
                }
                return builder.build();
            }
        }

        class AndQuery implements FunctionToQuery {
            @Override
            public Query apply(Function input, Context context) {
                assert input != null : "input must not be null";
                BooleanQuery.Builder query = new BooleanQuery.Builder();
                for (Symbol symbol : input.arguments()) {
                    query.add(process(symbol, context), BooleanClause.Occur.MUST);
                }
                return query.build();
            }
        }

        class OrQuery implements FunctionToQuery {
            @Override
            public Query apply(Function input, Context context) {
                assert input != null : "input must not be null";
                BooleanQuery.Builder query = new BooleanQuery.Builder();
                query.setMinimumNumberShouldMatch(1);
                for (Symbol symbol : input.arguments()) {
                    query.add(process(symbol, context), BooleanClause.Occur.SHOULD);
                }
                return query.build();
            }
        }


        private static final EqQuery eqQuery = new EqQuery();
        private static final RangeQuery ltQuery = new RangeQuery("lt");
        private static final RangeQuery lteQuery = new RangeQuery("lte");
        private static final RangeQuery gtQuery = new RangeQuery("gt");
        private static final RangeQuery gteQuery = new RangeQuery("gte");
        private static final WithinQuery withinQuery = new WithinQuery();
        private final ImmutableMap<String, FunctionToQuery> functions =
            ImmutableMap.<String, FunctionToQuery>builder()
                .put(WithinFunction.NAME, withinQuery)
                .put(AndOperator.NAME, new AndQuery())
                .put(OrOperator.NAME, new OrQuery())
                .put(EqOperator.NAME, eqQuery)
                .put(LtOperator.NAME, ltQuery)
                .put(LteOperator.NAME, lteQuery)
                .put(GteOperator.NAME, gteQuery)
                .put(GtOperator.NAME, gtQuery)
                .put(LikeOperator.NAME, new LikeQuery())
                .put(NotPredicate.NAME, new NotQuery())
                .put(Ignore3vlFunction.NAME, new Ignore3vlQuery())
                .put(IsNullPredicate.NAME, new IsNullQuery())
                .put(MatchPredicate.NAME, new ToMatchQuery())
                .put(AnyOperators.Names.EQ, new AnyEqQuery())
                .put(AnyOperators.Names.NEQ, new AnyNeqQuery())
                .put(AnyOperators.Names.LT, new AnyRangeQuery("gt", "lt"))
                .put(AnyOperators.Names.LTE, new AnyRangeQuery("gte", "lte"))
                .put(AnyOperators.Names.GTE, new AnyRangeQuery("lte", "gte"))
                .put(AnyOperators.Names.GT, new AnyRangeQuery("lt", "gt"))
                .put(AnyLikeOperator.LIKE, new AnyLikeQuery())
                .put(AnyLikeOperator.NOT_LIKE, new AnyNotLikeQuery())
                .put(RegexpMatchOperator.NAME, new RegexpMatchQuery())
                .put(RegexpMatchCaseInsensitiveOperator.NAME, new RegexMatchQueryCaseInsensitive())
                .build();

        private final ImmutableMap<String, InnerFunctionToQuery> innerFunctions =
            ImmutableMap.<String, InnerFunctionToQuery>builder()
                .put(DistanceFunction.NAME, new DistanceQuery())
                .put(WithinFunction.NAME, withinQuery)
                .put(SubscriptFunction.NAME, new SubscriptQuery())
                .put(ArrayUpperFunction.ARRAY_LENGTH, new ArrayLengthQuery())
                .put(ArrayUpperFunction.ARRAY_UPPER, new ArrayLengthQuery())
                .build();

        @Override
        public Query visitFunction(Function function, Context context) {
            assert function != null : "function must not be null";
            if (fieldIgnored(function, context)) {
                return Queries.newMatchAllQuery();
            }
            function = rewriteAndValidateFields(function, context);

            FunctionToQuery toQuery = functions.get(function.info().ident().name());
            if (toQuery == null) {
                return genericFunctionFilter(function, context);
            }

            Query query;
            try {
                query = toQuery.apply(function, context);
            } catch (IOException e) {
                throw ExceptionsHelper.convertToRuntime(e);
            } catch (UnsupportedOperationException e) {
                return genericFunctionFilter(function, context);
            }
            if (query == null) {
                query = queryFromInnerFunction(function, context);
                if (query == null) {
                    return genericFunctionFilter(function, context);
                }
            }
            return query;
        }

        private Query queryFromInnerFunction(Function function, Context context) {
            for (Symbol symbol : function.arguments()) {
                if (symbol.symbolType() == SymbolType.FUNCTION) {
                    String functionName = ((Function) symbol).info().ident().name();
                    InnerFunctionToQuery functionToQuery = innerFunctions.get(functionName);
                    if (functionToQuery != null) {
                        try {
                            Query query = functionToQuery.apply(function, (Function) symbol, context);
                            if (query != null) {
                                return query;
                            }
                        } catch (IOException e) {
                            throw ExceptionsHelper.convertToRuntime(e);
                        }
                    }
                }
            }
            return null;
        }

        private static boolean fieldIgnored(Function function, Context context) {
            if (function.arguments().size() != 2) {
                return false;
            }
            Symbol left = function.arguments().get(0);
            Symbol right = function.arguments().get(1);
            if (left.symbolType() == SymbolType.REFERENCE && right.symbolType().isValueSymbol()) {
                String columnName = ((Reference) left).column().name();
                if (Context.FILTERED_FIELDS.contains(columnName)) {
                    context.filteredFieldValues.put(columnName, ((Input) right).value());
                    return true;
                }
                String unsupportedMessage = Context.UNSUPPORTED_FIELDS.get(columnName);
                if (unsupportedMessage != null) {
                    throw new UnsupportedFeatureException(unsupportedMessage);
                }
            }
            return false;
        }

        private static Function rewriteAndValidateFields(Function function, Context context) {
            List<Symbol> arguments = function.arguments();
            if (arguments.size() == 2) {
                Symbol left = arguments.get(0);
                Symbol right = arguments.get(1);
                if (left.symbolType() == SymbolType.REFERENCE && right.symbolType().isValueSymbol()) {
                    Reference ref = (Reference) left;
                    if (ref.column().equals(DocSysColumns.UID)) {
                        return new Function(
                            function.info(),
                            ImmutableList.of(DocSysColumns.forTable(ref.ident().tableIdent(), DocSysColumns.ID), right)
                        );
                    } else {
                        String unsupportedMessage = context.unsupportedMessage(ref.column().name());
                        if (unsupportedMessage != null) {
                            throw new UnsupportedFeatureException(unsupportedMessage);
                        }
                    }
                }
            }
            return function;
        }


        @Override
        public Query visitReference(Reference symbol, Context context) {
            // called for queries like: where boolColumn
            if (symbol.valueType() == DataTypes.BOOLEAN) {
                MappedFieldType fieldType = context.getFieldTypeOrNull(symbol.column().fqn());
                if (fieldType == null) {
                    return Queries.newMatchNoDocsQuery("column does not exist in this index");
                }
                return fieldType.termQuery(true, context.queryShardContext());
            }
            return super.visitReference(symbol, context);
        }

        @Override
        public Query visitLiteral(Literal literal, Context context) {
            Object value = literal.value();
            if (value == null) {
                return Queries.newMatchNoDocsQuery("WHERE null -> no match");
            }
            try {
                return (boolean) value
                    ? Queries.newMatchAllQuery()
                    : Queries.newMatchNoDocsQuery("WHERE false -> no match");
            } catch (ClassCastException e) {
                // Throw a nice error if the top-level literal doesn't have a boolean type
                // (This is currently caught earlier, so this code is just a safe-guard)
                return visitSymbol(literal, context);
            }
        }

        @Override
        protected Query visitSymbol(Symbol symbol, Context context) {
            throw new UnsupportedOperationException(
                SymbolFormatter.format("Can't build query from symbol %s", symbol));
        }
    }

    static Query genericFunctionFilter(Function function, Context context) {
        if (function.valueType() != DataTypes.BOOLEAN) {
            raiseUnsupported(function);
        }
        // rewrite references to source lookup instead of using the docValues column store if:
        // - no docValues are available for the related column, currently only on objects defined as `ignored`
        // - docValues value differs from source, currently happening on GeoPoint types as lucene's internal format
        //   results in precision changes (e.g. longitude 11.0 will be 10.999999966)
        function = (Function) DocReferences.toSourceLookup(function,
            r -> r.columnPolicy() == ColumnPolicy.IGNORED
                 || r.valueType() == DataTypes.GEO_POINT);

        final InputFactory.Context<? extends LuceneCollectorExpression<?>> ctx = context.docInputFactory.getCtx(context.txnCtx);
        @SuppressWarnings("unchecked")
        final Input<Boolean> condition = (Input<Boolean>) ctx.add(function);
        @SuppressWarnings("unchecked")
        final Collection<? extends LuceneCollectorExpression<?>> expressions = ctx.expressions();
        final CollectorContext collectorContext = new CollectorContext(
            context.queryShardContext::getForField,
            new CollectorFieldsVisitor(expressions.size())
        );

        for (LuceneCollectorExpression expression : expressions) {
            expression.startCollect(collectorContext);
        }
        return new GenericFunctionQuery(function, expressions, collectorContext, condition);
    }

    private static void raiseUnsupported(Function function) {
        throw new UnsupportedOperationException(
            SymbolFormatter.format("Cannot convert function %s into a query", function));
    }
}

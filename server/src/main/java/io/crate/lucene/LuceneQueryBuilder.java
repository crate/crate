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
import io.crate.exceptions.VersioninigValidationException;
import io.crate.execution.engine.collect.DocInputFactory;
import io.crate.expression.InputFactory;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.operator.CIDROperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.GtOperator;
import io.crate.expression.operator.GteOperator;
import io.crate.expression.operator.LikeOperators;
import io.crate.expression.operator.LtOperator;
import io.crate.expression.operator.LteOperator;
import io.crate.expression.operator.OrOperator;
import io.crate.expression.operator.RegexpMatchCaseInsensitiveOperator;
import io.crate.expression.operator.RegexpMatchOperator;
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
import io.crate.expression.scalar.geo.DistanceFunction;
import io.crate.expression.scalar.geo.WithinFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.ColumnPolicy;
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
import org.elasticsearch.index.query.QueryShardContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static io.crate.expression.eval.NullEliminator.eliminateNullsIfPossible;
import static io.crate.metadata.DocReferences.inverseSourceLookup;


@Singleton
public class LuceneQueryBuilder {

    private static final Logger LOGGER = LogManager.getLogger(LuceneQueryBuilder.class);
    private static final Visitor VISITOR = new Visitor();
    private final Functions functions;

    @Inject
    public LuceneQueryBuilder(Functions functions) {
        this.functions = functions;
    }

    public Context convert(Symbol query,
                           TransactionContext txnCtx,
                           MapperService mapperService,
                           String indexName,
                           QueryShardContext queryShardContext,
                           DocTableInfo table,
                           IndexCache indexCache) throws UnsupportedFeatureException {
        var refResolver = new LuceneReferenceResolver(
            indexName,
            mapperService::fullName,
            table.partitionedByColumns()
        );
        var normalizer = new EvaluatingNormalizer(functions, RowGranularity.PARTITION, refResolver, null);
        Context ctx = new Context(
            txnCtx,
            functions,
            mapperService,
            indexCache,
            queryShardContext,
            indexName,
            table.partitionedByColumns()
        );
        CoordinatorTxnCtx coordinatorTxnCtx = CoordinatorTxnCtx.systemTransactionContext();
        ctx.query = eliminateNullsIfPossible(
            inverseSourceLookup(normalizer.normalize(query, coordinatorTxnCtx)),
            s -> normalizer.normalize(s, coordinatorTxnCtx)
        ).accept(VISITOR, ctx);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("WHERE CLAUSE [{}] -> LUCENE QUERY [{}] ", query.toString(Style.UNQUALIFIED), ctx.query);
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
        return (List) ((List) val).stream().filter(Objects::nonNull).collect(Collectors.toList());
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
                QueryShardContext queryShardContext,
                String indexName,
                List<Reference> partitionColumns) {
            this.txnCtx = txnCtx;
            this.queryShardContext = queryShardContext;
            FieldTypeLookup typeLookup = mapperService::fullName;
            this.docInputFactory = new DocInputFactory(
                functions,
                typeLookup,
                new LuceneReferenceResolver(
                    indexName,
                    typeLookup,
                    partitionColumns
                )
            );
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
            .put("_version", VersioninigValidationException.VERSION_COLUMN_USAGE_MSG)
            .put("_seq_no", VersioninigValidationException.SEQ_NO_AND_PRIMARY_TERM_USAGE_MSG)
            .put("_primary_term", VersioninigValidationException.SEQ_NO_AND_PRIMARY_TERM_USAGE_MSG)
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
                return args.get(0).accept(Visitor.this, context);
            }
        }

        class AndQuery implements FunctionToQuery {
            @Override
            public Query apply(Function input, Context context) {
                assert input != null : "input must not be null";
                BooleanQuery.Builder query = new BooleanQuery.Builder();
                for (Symbol symbol : input.arguments()) {
                    query.add(symbol.accept(Visitor.this, context), BooleanClause.Occur.MUST);
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
                    query.add(symbol.accept(Visitor.this, context), BooleanClause.Occur.SHOULD);
                }
                return query.build();
            }
        }


        private static final EqQuery EQ_QUERY = new EqQuery();
        private static final RangeQuery LT_QUERY = new RangeQuery("lt");
        private static final RangeQuery LTE_QUERY = new RangeQuery("lte");
        private static final RangeQuery GT_QUERY = new RangeQuery("gt");
        private static final RangeQuery GTE_QUERY = new RangeQuery("gte");
        private static final CIDRRangeQuery LLT_QUERY = new CIDRRangeQuery();
        private static final WithinQuery WITHIN_QUERY = new WithinQuery();
        private final ImmutableMap<String, FunctionToQuery> functions =
            ImmutableMap.<String, FunctionToQuery>builder()
                .put(WithinFunction.NAME, WITHIN_QUERY)
                .put(AndOperator.NAME, new AndQuery())
                .put(OrOperator.NAME, new OrQuery())
                .put(EqOperator.NAME, EQ_QUERY)
                .put(LtOperator.NAME, LT_QUERY)
                .put(LteOperator.NAME, LTE_QUERY)
                .put(GteOperator.NAME, GTE_QUERY)
                .put(GtOperator.NAME, GT_QUERY)
                .put(CIDROperator.CONTAINED_WITHIN, LLT_QUERY)
                .put(LikeOperators.OP_LIKE, new LikeQuery(false))
                .put(LikeOperators.OP_ILIKE, new LikeQuery(true))
                .put(NotPredicate.NAME, new NotQuery(this))
                .put(Ignore3vlFunction.NAME, new Ignore3vlQuery())
                .put(IsNullPredicate.NAME, new IsNullQuery())
                .put(MatchPredicate.NAME, new ToMatchQuery())
                .put(AnyOperators.Names.EQ, new AnyEqQuery())
                .put(AnyOperators.Names.NEQ, new AnyNeqQuery())
                .put(AnyOperators.Names.LT, new AnyRangeQuery("gt", "lt"))
                .put(AnyOperators.Names.LTE, new AnyRangeQuery("gte", "lte"))
                .put(AnyOperators.Names.GTE, new AnyRangeQuery("lte", "gte"))
                .put(AnyOperators.Names.GT, new AnyRangeQuery("lt", "gt"))
                .put(LikeOperators.ANY_LIKE, new AnyLikeQuery(false))
                .put(LikeOperators.ANY_NOT_LIKE, new AnyNotLikeQuery(false))
                .put(LikeOperators.ANY_ILIKE, new AnyLikeQuery(true))
                .put(LikeOperators.ANY_NOT_ILIKE, new AnyNotLikeQuery(true))
                .put(RegexpMatchOperator.NAME, new RegexpMatchQuery())
                .put(RegexpMatchCaseInsensitiveOperator.NAME, new RegexMatchQueryCaseInsensitive())
                .build();

        private final ImmutableMap<String, InnerFunctionToQuery> innerFunctions =
            ImmutableMap.<String, InnerFunctionToQuery>builder()
                .put(DistanceFunction.NAME, new DistanceQuery())
                .put(WithinFunction.NAME, WITHIN_QUERY)
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
                            function.signature(),
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
                    Symbols.format("Can't build query from symbol %s", symbol));
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
        final Collection<? extends LuceneCollectorExpression<?>> expressions = ctx.expressions();
        final CollectorContext collectorContext = new CollectorContext(context.queryShardContext::getForField);
        for (LuceneCollectorExpression<?> expression : expressions) {
            expression.startCollect(collectorContext);
        }
        return new GenericFunctionQuery(function, expressions, condition);
    }

    private static void raiseUnsupported(Function function) {
        throw new UnsupportedOperationException(
                Symbols.format("Cannot convert function %s into a query", function));
    }

}

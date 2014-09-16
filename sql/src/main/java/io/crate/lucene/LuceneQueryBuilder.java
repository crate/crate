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

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.WhereClause;
import io.crate.executor.transport.task.elasticsearch.ESQueryBuilder;
import io.crate.metadata.Functions;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectInputSymbolVisitor;
import io.crate.operation.operator.*;
import io.crate.operation.operator.any.*;
import io.crate.operation.predicate.IsNullPredicate;
import io.crate.operation.predicate.MatchPredicate;
import io.crate.operation.predicate.NotPredicate;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.operation.reference.doc.lucene.LuceneDocLevelReferenceResolver;
import io.crate.planner.symbol.*;
import io.crate.types.DataTypes;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.docset.MatchDocIdSet;
import org.elasticsearch.common.lucene.search.NotFilter;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.query.RegexpFlag;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

public class LuceneQueryBuilder {

    private final Visitor visitor;
    private SearchContext searchContext;

    public LuceneQueryBuilder(Functions functions) {
        CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor =
                new CollectInputSymbolVisitor<>(functions, LuceneDocLevelReferenceResolver.INSTANCE);
        visitor = new Visitor(inputSymbolVisitor);
    }

    public void searchContext(SearchContext searchContext) {
        visitor.searchContext = searchContext;
    }

    public Context convert(WhereClause whereClause) {
        Context ctx = new Context();
        if (whereClause.noMatch()) {
            ctx.query = Queries.newMatchNoDocsQuery();
        } else if (!whereClause.hasQuery()) {
            ctx.query = Queries.newMatchAllQuery();
        } else {
            ctx.query = visitor.process(whereClause.query(), ctx);
        }
        return ctx;
    }

    public static class Context {
        Query query;
        final Map<String, Object> filteredFieldValues = new HashMap<>();

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

        /**
         * These fields are ignored in the whereClause.
         * If a filtered field is encountered the value of the literal is written into filteredFieldValues
         * (only applies to Function with 2 arguments and if left == reference and right == literal)
         */
        final static Set<String> FILTERED_FIELDS = new HashSet<String>(){{ add("_score"); }};

        /**
         * key = columnName
         * value = error message
         * <p/>
         * (in the _version case if the primary key is present a GetPlan is built from the planner and
         * the LuceneQueryBuilder is never used)
         */
        final static Map<String, String> UNSUPPORTED_FIELDS = ImmutableMap.<String, String>builder()
                .put("_version", "\"_version\" column is only valid in the WHERE clause if the primary key column is also present")
                .build();
    }

    static class Visitor extends SymbolVisitor<Context, Query> {

        private SearchContext searchContext;
        private final CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor;

        public Visitor(CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor) {
            this.inputSymbolVisitor = inputSymbolVisitor;
        }


        abstract class FunctionToQuery {
            public abstract Query apply (Function input, Context context) throws IOException;
        }

        abstract class CmpQuery extends FunctionToQuery {

            protected Tuple<Reference, Literal> prepare(Function input) {
                assert input != null;
                assert input.arguments().size() == 2;

                Symbol left = input.arguments().get(0);
                Symbol right = input.arguments().get(1);

                if (left.symbolType() == SymbolType.FUNCTION || right.symbolType() == SymbolType.FUNCTION) {
                    raiseUnsupported(input);
                }

                assert left instanceof Reference;
                assert right.symbolType() == SymbolType.LITERAL;

                return new Tuple<>((Reference)left, (Literal)right);
            }
        }

        /**
         * 1 != any ( col ) -->  gt 1 or lt 1
         */
        class AnyNeqQuery extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> tuple = prepare(input);
                Reference reference = tuple.v1();
                Object value = tuple.v2().value();

                String columnName = reference.info().ident().columnIdent().fqn();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(reference.valueType());
                BooleanQuery query = new BooleanQuery();
                query.setMinimumNumberShouldMatch(1);
                query.add(
                        builder.rangeQuery(columnName, value, null, false, false),
                        BooleanClause.Occur.SHOULD
                );
                query.add(
                        builder.rangeQuery(columnName, null, value, false, false),
                        BooleanClause.Occur.SHOULD
                );
                return query;
            }
        }

        class AnyNotLikeQuery extends CmpQuery {

            private String negateWildcard(String wildCard) {
                return String.format("~(%s)", wildCard);
            }

            @Override
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> prepare = prepare(input);
                String notLike = negateWildcard(
                        ESQueryBuilder.convertWildcardToRegex(BytesRefs.toString(prepare.v2().value())));

                return new RegexpQuery(new Term(
                        prepare.v1().info().ident().columnIdent().fqn(),
                        notLike),
                        RegexpFlag.COMPLEMENT.value()
                );
            }
        }
        class LikeQuery extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> tuple = prepare(input);
                String columnName = tuple.v1().info().ident().columnIdent().fqn();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(tuple.v1().valueType());
                return builder.like(columnName, tuple.v2().value());
            }
        }

        class NotQuery extends FunctionToQuery {

            @Override
            public Query apply(Function input, Context context) {
                assert input != null;
                assert input.arguments().size() == 1;
                BooleanQuery query = new BooleanQuery();

                query.add(process(input.arguments().get(0), context), BooleanClause.Occur.MUST_NOT);
                query.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);

                return query;
            }
        }

        class IsNullQuery extends FunctionToQuery {

            @Override
            public Query apply(Function input, Context context) {
                assert input != null;
                assert input.arguments().size() == 1;
                Symbol arg = input.arguments().get(0);
                checkArgument(arg.symbolType() == SymbolType.REFERENCE);

                Reference reference = (Reference)arg;

                String columnName = reference.info().ident().columnIdent().fqn();
                QueryBuilderHelper builderHelper = QueryBuilderHelper.forType(reference.valueType());
                return new FilteredQuery(
                        new MatchAllDocsQuery(),
                        new NotFilter(builderHelper.rangeFilter(columnName, null, null, true, true)));
            }
        }

        class EqQuery extends CmpQuery {
            @Override
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> tuple = super.prepare(input);

                String columnName = tuple.v1().info().ident().columnIdent().fqn();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(tuple.v1().valueType());
                return builder.eq(columnName, tuple.v2().value());
            }
        }

        class AndQuery extends FunctionToQuery {
            @Override
            public Query apply(Function input, Context context) {
                assert input != null;
                BooleanQuery query = new BooleanQuery();
                for (Symbol symbol : input.arguments()) {
                    query.add(process(symbol, context), BooleanClause.Occur.MUST);
                }
                return query;
            }
        }

        class OrQuery extends FunctionToQuery {
            @Override
            public Query apply(Function input, Context context) {
                assert input != null;
                BooleanQuery query = new BooleanQuery();
                query.setMinimumNumberShouldMatch(1);
                for (Symbol symbol : input.arguments()) {
                    query.add(process(symbol, context), BooleanClause.Occur.SHOULD);
                }
                return query;
            }
        }

        class LtQuery extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> tuple = super.prepare(input);

                String columnName = tuple.v1().info().ident().columnIdent().fqn();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(tuple.v1().valueType());
                return builder.rangeQuery(columnName, null, tuple.v2().value(), false, false);
            }
        }

        class LteQuery extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> tuple = super.prepare(input);

                String columnName = tuple.v1().info().ident().columnIdent().fqn();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(tuple.v1().valueType());
                return builder.rangeQuery(columnName, null, tuple.v2().value(), false, true);
            }
        }

        class GtQuery extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> tuple = super.prepare(input);

                String columnName = tuple.v1().info().ident().columnIdent().fqn();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(tuple.v1().valueType());
                return builder.rangeQuery(columnName, tuple.v2().value(), null, false, false);
            }
        }

        class GteQuery extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> tuple = super.prepare(input);

                String columnName = tuple.v1().info().ident().columnIdent().fqn();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(tuple.v1().valueType());
                return builder.rangeQuery(columnName, tuple.v2().value(), null, true, false);
            }
        }

        class ToMatchQuery extends FunctionToQuery {

            @Override
            public Query apply(Function input, Context context) throws IOException {
                List<Symbol> arguments = input.arguments();
                assert arguments.size() == 4 : "invalid number of arguments";
                assert Symbol.isLiteral(arguments.get(0), DataTypes.OBJECT);
                assert Symbol.isLiteral(arguments.get(1), DataTypes.STRING);
                assert Symbol.isLiteral(arguments.get(2), DataTypes.STRING);
                assert Symbol.isLiteral(arguments.get(3), DataTypes.OBJECT);

                @SuppressWarnings("unchecked")
                Map<String, Object> fields = (Map) ((Literal) arguments.get(0)).value();
                BytesRef queryString = (BytesRef) ((Literal) arguments.get(1)).value();
                BytesRef matchType = (BytesRef) ((Literal) arguments.get(2)).value();
                Map options = (Map) ((Literal) arguments.get(3)).value();

                checkArgument(queryString != null, "cannot use NULL as query term in match predicate");

                io.crate.lucene.MatchQueryBuilder queryBuilder = new io.crate.lucene.MatchQueryBuilder(
                        searchContext, fields, queryString, matchType, options);
                return queryBuilder.query();
            }
        }

        private final EqQuery eqQuery = new EqQuery();
        private final LtQuery ltQuery = new LtQuery();
        private final LteQuery lteQuery = new LteQuery();
        private final GtQuery gtQuery = new GtQuery();
        private final GteQuery gteQuery = new GteQuery();
        private final LikeQuery likeQuery = new LikeQuery();
        private final ImmutableMap<String, FunctionToQuery> functions =
                ImmutableMap.<String, FunctionToQuery>builder()
                    .put(AndOperator.NAME, new AndQuery())
                    .put(OrOperator.NAME, new OrQuery())
                    .put(EqOperator.NAME, eqQuery)
                    .put(LtOperator.NAME, ltQuery)
                    .put(LteOperator.NAME, lteQuery)
                    .put(GteOperator.NAME, gteQuery)
                    .put(GtOperator.NAME, gtQuery)
                    .put(LikeOperator.NAME, likeQuery)
                    .put(NotPredicate.NAME, new NotQuery())
                    .put(IsNullPredicate.NAME, new IsNullQuery())
                    .put(MatchPredicate.NAME, new ToMatchQuery())
                    .put(AnyEqOperator.NAME, eqQuery)
                    .put(AnyNeqOperator.NAME, new AnyNeqQuery())
                    .put(AnyLtOperator.NAME, ltQuery)
                    .put(AnyLteOperator.NAME, lteQuery)
                    .put(AnyGteOperator.NAME, gteQuery)
                    .put(AnyGtOperator.NAME, gtQuery)
                    .put(AnyLikeOperator.NAME, likeQuery)
                    .put(AnyNotLikeOperator.NAME, new AnyNotLikeQuery())
                .build();

        @Override
        public Query visitFunction(Function function, Context context) {
            assert function != null;

            if (fieldIgnored(function, context)) {
                return Queries.newMatchAllQuery();
            }

            FunctionToQuery toQuery = functions.get(function.info().ident().name());

            if (toQuery == null) {
                return genericFunctionQuery(function);
            }
            try {
                return toQuery.apply(function, context);
            } catch (IOException e) {
                throw ExceptionsHelper.convertToRuntime(e);
            } catch (UnsupportedOperationException e) { // TODO: don't use try/catch for fallback but check before-hand?
                return genericFunctionQuery(function);
            }
        }

        private boolean fieldIgnored(Function function, Context context) {
            if (function.arguments().size() != 2) {
                return false;
            }

            Symbol left = function.arguments().get(0);
            Symbol right = function.arguments().get(1);
            if (left.symbolType() == SymbolType.REFERENCE && right.symbolType().isValueSymbol()) {
                String columnName = ((Reference) left).info().ident().columnIdent().name();
                if (Context.FILTERED_FIELDS.contains(columnName)) {
                    context.filteredFieldValues.put(columnName, ((Input) right).value());
                    return true;
                }
                String unsupportedMessage = Context.UNSUPPORTED_FIELDS.get(columnName);
                if (unsupportedMessage != null) {
                    throw new UnsupportedOperationException(unsupportedMessage);
                }
            }
            return false;
        }

        private Query genericFunctionQuery(Function function) {
            if (function.valueType() != DataTypes.BOOLEAN) {
                raiseUnsupported(function);
            }

            final CollectInputSymbolVisitor.Context ctx = inputSymbolVisitor.process(function);
            assert ctx.topLevelInputs().size() == 1;
            @SuppressWarnings("unchecked")
            final Input<Boolean> condition = (Input<Boolean>) ctx.topLevelInputs().get(0);
            @SuppressWarnings("unchecked")
            final List<LuceneCollectorExpression> expressions = ctx.docLevelExpressions();
            CollectorContext collectorContext = new CollectorContext();
            collectorContext.searchContext(searchContext);
            for (LuceneCollectorExpression expression : expressions) {
                expression.startCollect(collectorContext);
            }
            return new FilteredQuery(new MatchAllDocsQuery(), new Filter() {
                @Override
                public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
                    for (LuceneCollectorExpression expression : expressions) {
                        expression.setNextReader(context);
                    }
                    return BitsFilteredDocIdSet.wrap(
                            new FunctionDocSet(
                                    condition,
                                    expressions,
                                    context.reader().maxDoc(),
                                    acceptDocs
                            ),
                            acceptDocs);
                }
            });
        }

        static class FunctionDocSet extends MatchDocIdSet {

            private final Input<Boolean> condition;
            private final List<LuceneCollectorExpression> expressions;

            protected FunctionDocSet(Input<Boolean> condition,
                                     List<LuceneCollectorExpression> expressions,
                                     int maxDoc,
                                     @Nullable Bits acceptDocs) {
                super(maxDoc, acceptDocs);
                this.condition = condition;
                this.expressions = expressions;
            }

            @Override
            protected boolean matchDoc(int doc) {
                for (LuceneCollectorExpression expression : expressions) {
                    expression.setNextDocId(doc);
                }
                Boolean value = condition.value();
                if (value == null) {
                    return false;
                }
                return value;
            }
        }

        private Query raiseUnsupported(Function function) {
            throw new UnsupportedOperationException(
                    SymbolFormatter.format("Cannot convert function %s into a query", function));
        }

        @Override
        public Query visitReference(Reference symbol, Context context) {
            // called for queries like: where boolColumn
            if (symbol.valueType() == DataTypes.BOOLEAN) {
                return QueryBuilderHelper.forType(DataTypes.BOOLEAN).eq(symbol.info().ident().columnIdent().fqn(), true);
            }
            return super.visitReference(symbol, context);
        }

        @Override
        protected Query visitSymbol(Symbol symbol, Context context) {
            throw new UnsupportedOperationException(
                    SymbolFormatter.format("Can't build query from symbol %s", symbol));
        }
    }
}

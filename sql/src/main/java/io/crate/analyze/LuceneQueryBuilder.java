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

package io.crate.analyze;

import com.google.common.base.*;
import com.google.common.collect.ImmutableMap;
import io.crate.lucene.QueryBuilderHelper;
import io.crate.operator.operator.*;
import io.crate.planner.symbol.*;
import io.crate.planner.symbol.Function;
import org.apache.lucene.search.*;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;

import javax.annotation.Nullable;

public class LuceneQueryBuilder {

    private final static Visitor visitor = new Visitor();

    public Query convert(@Nullable Symbol whereClause) {
        return convert(Optional.fromNullable(whereClause));
    }

    public Query convert(Optional<? extends Symbol> whereClause) {
        if (!whereClause.isPresent()) {
            return new MatchAllDocsQuery();
        }

        return visitor.process(whereClause.get(), null);
    }

    static class Visitor extends SymbolVisitor<Void, Query> {

        abstract class FunctionToQuery {
            public abstract Query apply (Function input);
        }

        abstract class CmpQuery extends FunctionToQuery {

            protected Tuple<Reference, Literal> prepare(Function input) {
                Preconditions.checkNotNull(input);
                Preconditions.checkArgument(input.arguments().size() == 2);

                Symbol left = input.arguments().get(0);
                Symbol right = input.arguments().get(1);

                if (left.symbolType() == SymbolType.FUNCTION || right.symbolType() == SymbolType.FUNCTION) {
                    raiseUnsupported(input);
                }

                assert left.symbolType() == SymbolType.REFERENCE;
                assert right.symbolType().isLiteral();

                return new Tuple<>((Reference)left, (Literal)right);
            }
        }

        class EqQuery extends CmpQuery {
            @Override
            public Query apply(Function input) {
                Tuple<Reference, Literal> tuple = super.prepare(input);

                String columnName = tuple.v1().info().ident().fqDottedColumnName();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(tuple.v1().valueType());
                return builder.eq(columnName, tuple.v2().value());
            }
        }

        class NotEqQuery extends CmpQuery {

            @Override
            public Query apply(Function input) {
                BooleanQuery query = new BooleanQuery();
                query.add(functions.get(EqOperator.NAME).apply(input), BooleanClause.Occur.MUST_NOT);
                query.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);

                return query;
            }
        }

        class AndQuery extends FunctionToQuery {
            @Override
            public Query apply(Function input) {
                Preconditions.checkNotNull(input);
                BooleanQuery query = new BooleanQuery();
                for (Symbol symbol : input.arguments()) {
                    query.add(process(symbol, null), BooleanClause.Occur.MUST);
                }
                return query;
            }
        }

        class OrQuery extends FunctionToQuery {
            @Override
            public Query apply(Function input) {
                Preconditions.checkNotNull(input);
                BooleanQuery query = new BooleanQuery();
                query.setMinimumNumberShouldMatch(1);
                for (Symbol symbol : input.arguments()) {
                    query.add(process(symbol, null), BooleanClause.Occur.SHOULD);
                }
                return query;
            }
        }

        class LtQuery extends CmpQuery {

            @Override
            public Query apply(Function input) {
                Tuple<Reference, Literal> tuple = super.prepare(input);

                String columnName = tuple.v1().info().ident().fqDottedColumnName();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(tuple.v1().valueType());
                return builder.lt(columnName, tuple.v2().value());
            }
        }

        class LteQuery extends CmpQuery {

            @Override
            public Query apply(Function input) {
                Tuple<Reference, Literal> tuple = super.prepare(input);

                String columnName = tuple.v1().info().ident().fqDottedColumnName();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(tuple.v1().valueType());
                return builder.lte(columnName, tuple.v2().value());
            }
        }
        class GtQuery extends CmpQuery {

            @Override
            public Query apply(Function input) {
                Tuple<Reference, Literal> tuple = super.prepare(input);

                String columnName = tuple.v1().info().ident().fqDottedColumnName();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(tuple.v1().valueType());
                return builder.gt(columnName, tuple.v2().value());
            }
        }
        class GteQuery extends CmpQuery {

            @Override
            public Query apply(Function input) {
                Tuple<Reference, Literal> tuple = super.prepare(input);

                String columnName = tuple.v1().info().ident().fqDottedColumnName();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(tuple.v1().valueType());
                return builder.gte(columnName, tuple.v2().value());
            }
        }

        private ImmutableMap<String, FunctionToQuery> functions =
                ImmutableMap.<String, FunctionToQuery>builder()
                    .put(AndOperator.NAME, new AndQuery())
                    .put(OrOperator.NAME, new OrQuery())
                    .put(EqOperator.NAME, new EqQuery())
                    .put(NotEqOperator.NAME, new NotEqQuery())
                    .put(LtOperator.NAME, new LtQuery())
                    .put(LteOperator.NAME, new LteQuery())
                    .put(GteOperator.NAME, new GteQuery())
                    .put(GtOperator.NAME, new GtQuery())
                .build();

        @Override
        public Query visitFunction(Function function, Void context) {
            Preconditions.checkNotNull(function);
            FunctionToQuery toQuery = functions.get(function.info().ident().name());
            if (toQuery == null) {
                return raiseUnsupported(function);
            }
            return toQuery.apply(function);
        }

        private Query raiseUnsupported(Function function) {
            throw new UnsupportedOperationException(
                    String.format("Cannot convert function <%s> into a query", function));
        }


        /**
         * might be called in the case of
         *      where null
         * @return MatchNoDocs
         */
        @Override
        public Query visitNullLiteral(Null symbol, Void context) {
            return new MatchNoDocsQuery();
        }

        /**
         * might be called in the case of
         *      where x = 'a' and true
         * or
         *      where true
         * @return MatchAll/MatchNo Docs
         */
        @Override
        public Query visitBooleanLiteral(BooleanLiteral symbol, Void context) {
            Preconditions.checkNotNull(symbol);
            if (symbol.value()) {
                return new MatchAllDocsQuery();
            }

            return new MatchNoDocsQuery();
        }

        @Override
        protected Query visitSymbol(Symbol symbol, Void context) {
            throw new UnsupportedOperationException("Can't build query from symbol " + symbol);
        }
    }
}

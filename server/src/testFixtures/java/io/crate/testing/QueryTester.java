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

package io.crate.testing;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.analyze.relations.DocTableRelation;
import io.crate.common.collections.Iterables;
import io.crate.common.collections.Lists;
import io.crate.data.Input;
import io.crate.execution.dml.IndexItem;
import io.crate.execution.dml.Indexer;
import io.crate.execution.engine.collect.collectors.LuceneBatchIterator;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.symbol.FunctionCopyVisitor;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.PlannerContext;
import io.crate.planner.optimizer.symbol.Optimizer;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.CreateTable;

public final class QueryTester implements AutoCloseable {

    private final BiFunction<ColumnIdent, Query, LuceneBatchIterator> getIterator;
    private final BiFunction<String, Object[], Symbol> expressionToSymbol;
    private final Function<Symbol, Query> symbolToQuery;
    private final IndexEnv indexEnv;

    public static class Builder {

        private final DocTableInfo table;
        private final SqlExpressions expressions;
        private final PlannerContext plannerContext;
        private final IndexEnv indexEnv;
        private final LuceneQueryBuilder queryBuilder;

        public Builder(ThreadPool threadPool,
                       ClusterService clusterService,
                       Version indexVersion,
                       String createTableStmt) throws IOException {
            this(
                threadPool,
                clusterService,
                indexVersion,
                createTableStmt,
                // Disable OID generation for columns/references in order to be able to compare the query outcome with
                // expected ones.
                () -> COLUMN_OID_UNASSIGNED);
        }

        public Builder(ThreadPool threadPool,
                       ClusterService clusterService,
                       Version indexVersion,
                       String createTableStmt,
                       LongSupplier columnOidSupplier) throws IOException {
            var sqlExecutor = SQLExecutor
                .of(clusterService)
                .setColumnOidSupplier(columnOidSupplier)
                .addTable(createTableStmt);
            plannerContext = sqlExecutor.getPlannerContext();

            var createTable = (CreateTable<?>) SqlParser.createStatement(createTableStmt);
            String tableName = Iterables.getLast(createTable.name().getName().getParts());
            table = sqlExecutor.resolveTableInfo(tableName);

            indexEnv = new IndexEnv(
                threadPool,
                table,
                clusterService.state(),
                indexVersion
            );
            queryBuilder = new LuceneQueryBuilder(plannerContext.nodeContext());
            var docTableRelation = new DocTableRelation(table);
            expressions = new SqlExpressions(
                Collections.singletonMap(table.ident(), docTableRelation),
                docTableRelation
            );
        }

        public Builder indexValues(String column, Object ... values) throws IOException {
            for (Object value : values) {
                indexValue(column, value);
            }
            return this;
        }

        public Builder indexValue(String column, Object value) throws IOException {
            Indexer indexer = new Indexer(
                table.concreteIndices(plannerContext.clusterState().metadata())[0],
                table,
                plannerContext.transactionContext(),
                plannerContext.nodeContext(),
                List.of(table.getReference(ColumnIdent.fromPath(column))),
                null
            );
            var item = new IndexItem.StaticItem("dummy-id", List.of(), new Object[] { value }, -1L, -1L);
            ParsedDocument parsedDocument = indexer.index(item);
            indexEnv.writer().addDocument(parsedDocument.doc());
            return this;
        }

        public Builder indexValues(List<String> columns, Object ... values) throws IOException {
            Indexer indexer = new Indexer(
                table.concreteIndices(plannerContext.clusterState().metadata())[0],
                table,
                plannerContext.transactionContext(),
                plannerContext.nodeContext(),
                Lists.map(columns, c -> table.getReference(ColumnIdent.fromPath(c))),
                null
            );
            var item = new IndexItem.StaticItem("dummy-id", List.of(), values, -1L, -1L);
            ParsedDocument parsedDocument = indexer.index(item);
            indexEnv.writer().addDocument(parsedDocument.doc());
            return this;
        }

        private LuceneBatchIterator getIterator(ColumnIdent column, Query query) {
            InputFactory inputFactory = new InputFactory(plannerContext.nodeContext());
            InputFactory.Context<LuceneCollectorExpression<?>> ctx = inputFactory.ctxForRefs(
                CoordinatorTxnCtx.systemTransactionContext(), indexEnv.luceneReferenceResolver());
            Input<?> input = ctx.add(requireNonNull(table.getReference(column),
                "column must exist in created table: " + column));
            IndexSearcher indexSearcher;
            try {
                indexSearcher = new IndexSearcher(DirectoryReader.open(indexEnv.writer()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new LuceneBatchIterator(
                indexSearcher,
                query,
                null,
                false,
                new CollectorContext(table.droppedColumns(), table.lookupNameBySourceKey()),
                Collections.singletonList(input),
                ctx.expressions()
            );
        }

        public QueryTester build() throws IOException {
            indexEnv.writer().commit();
            CoordinatorTxnCtx systemTxnCtx = CoordinatorTxnCtx.systemTransactionContext();
            return new QueryTester(
                this::getIterator,
                (expr, params) -> {
                    Symbol symbol = expressions.asSymbol(expr);
                    Symbol boundSymbol = symbol.accept(new FunctionCopyVisitor<>() {
                        @Override
                        public Symbol visitParameterSymbol(ParameterSymbol parameterSymbol, Object context) {
                            Object param = params[parameterSymbol.index()];
                            return Literal.ofUnchecked(
                                parameterSymbol.valueType(),
                                parameterSymbol.valueType().sanitizeValue(param)
                            );
                        }
                    }, null);
                    return Optimizer.optimizeCasts(expressions.normalize(boundSymbol), plannerContext);
                },
                symbol -> queryBuilder.convert(
                    Optimizer.optimizeCasts(symbol,plannerContext),
                    systemTxnCtx,
                    indexEnv.mapperService(),
                    indexEnv.indexService().index().getName(),
                    indexEnv.queryShardContext(),
                    table,
                    indexEnv.queryCache()
                ).query(),
                indexEnv
            );
        }
    }

    private QueryTester(BiFunction<ColumnIdent, Query, LuceneBatchIterator> getIterator,
                        BiFunction<String, Object[], Symbol> expressionToSymbol,
                        Function<Symbol, Query> symbolToQuery,
                        IndexEnv indexEnv) {
        this.getIterator = getIterator;
        this.expressionToSymbol = expressionToSymbol;
        this.symbolToQuery = symbolToQuery;
        this.indexEnv = indexEnv;
    }

    public IndexSearcher searcher() throws IOException {
        return new IndexSearcher(DirectoryReader.open(indexEnv.writer()));
    }

    public Query toQuery(String expression, Object ... params) {
        return symbolToQuery.apply(expressionToSymbol.apply(expression, params));
    }

    public Query toQuery(Symbol expression) {
        return symbolToQuery.apply(expression);
    }

    public List<Object> runQuery(String resultColumn, String expression, Object ... params) throws Exception {
        Query query = toQuery(expression, params);
        LuceneBatchIterator batchIterator = getIterator.apply(ColumnIdent.fromPath(resultColumn), query);
        return batchIterator
            .map(row -> row.get(0))
            .toList()
            .get(5, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws Exception {
        indexEnv.close();
    }
}

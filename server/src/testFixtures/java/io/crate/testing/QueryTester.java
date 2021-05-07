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

import io.crate.analyze.relations.DocTableRelation;
import io.crate.data.BatchIterators;
import io.crate.data.Input;
import io.crate.execution.dml.upsert.GeneratedColumns;
import io.crate.execution.dml.upsert.InsertSourceGen;
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
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.planner.PlannerContext;
import io.crate.planner.optimizer.symbol.Optimizer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public final class QueryTester implements AutoCloseable {

    private final BiFunction<ColumnIdent, Query, LuceneBatchIterator> getIterator;
    private final BiFunction<String, Object[], Symbol> expressionToSymbol;
    private final Function<Symbol, Query> symbolToQuery;
    private final IndexEnv indexEnv;

    public static class Builder {

        private final DocTableInfo table;
        private final SQLExecutor sqlExecutor;
        private final SqlExpressions expressions;
        private final PlannerContext plannerContext;
        private final DocTableRelation docTableRelation;
        private final IndexEnv indexEnv;
        private final LuceneQueryBuilder queryBuilder;

        public Builder(Path tempDir,
                       ThreadPool threadPool,
                       ClusterService clusterService,
                       Version indexVersion,
                       String createTableStmt,
                       AbstractModule... additionalModules) throws IOException {
            sqlExecutor = SQLExecutor
                .builder(clusterService, additionalModules)
                .addTable(createTableStmt)
                .build();
            plannerContext = sqlExecutor.getPlannerContext(clusterService.state());

            DocSchemaInfo docSchema = findDocSchema(sqlExecutor.schemas());
            table = (DocTableInfo) docSchema.getTables().iterator().next();

            indexEnv = new IndexEnv(
                threadPool,
                table,
                clusterService.state(),
                indexVersion,
                tempDir
            );
            queryBuilder = new LuceneQueryBuilder(plannerContext.nodeContext());
            docTableRelation = new DocTableRelation(table);
            expressions = new SqlExpressions(
                Collections.singletonMap(table.ident(), docTableRelation),
                docTableRelation
            );
        }

        private DocSchemaInfo findDocSchema(Schemas schemas) {
            for (SchemaInfo schema : schemas) {
                if (schema instanceof DocSchemaInfo) {
                    return (DocSchemaInfo) schema;
                }
            }
            throw new IllegalArgumentException("Create table statement must result in the creation of a user table");
        }

        public Builder indexValues(String column, Object ... values) throws IOException {
            for (Object value : values) {
                indexValue(column, value);
            }
            return this;
        }

        void indexValue(String column, Object value) throws IOException {
            DocumentMapper mapper = indexEnv.mapperService().documentMapper();
            InsertSourceGen sourceGen = InsertSourceGen.of(
                CoordinatorTxnCtx.systemTransactionContext(),
                plannerContext.nodeContext(),
                table,
                table.concreteIndices()[0],
                GeneratedColumns.Validation.NONE,
                Collections.singletonList(table.getReference(ColumnIdent.fromPath(column)))
            );
            BytesReference source = sourceGen.generateSourceAndCheckConstraintsAsBytesReference(new Object[]{value});
            SourceToParse sourceToParse = new SourceToParse(
                table.concreteIndices()[0],
                UUIDs.randomBase64UUID(),
                source,
                XContentType.JSON
            );
            ParsedDocument parsedDocument = mapper.parse(sourceToParse);
            indexEnv.writer().addDocuments(parsedDocument.docs());
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
                new CollectorContext(),
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
                    symbol,
                    systemTxnCtx,
                    indexEnv.mapperService(),
                    indexEnv.indexService().index().getName(),
                    indexEnv.queryShardContext(),
                    table,
                    indexEnv.indexCache()
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

    public Query toQuery(String expression) {
        return symbolToQuery.apply(expressionToSymbol.apply(expression, null));
    }

    public Query toQuery(String expression, Object ... params) {
        return symbolToQuery.apply(expressionToSymbol.apply(expression, params));
    }

    public Query toQuery(Symbol expression) {
        return symbolToQuery.apply(expression);
    }



    public List<Object> runQuery(String resultColumn, String expression) throws Exception {
        Query query = toQuery(expression);
        LuceneBatchIterator batchIterator = getIterator.apply(ColumnIdent.fromPath(resultColumn), query);
        return BatchIterators.collect(
            batchIterator,
            Collectors.mapping(row -> row.get(0), Collectors.toList())
        ).get(5, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws Exception {
        indexEnv.close();
    }
}

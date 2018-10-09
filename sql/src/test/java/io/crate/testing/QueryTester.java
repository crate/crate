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

package io.crate.testing;

import io.crate.analyze.relations.DocTableRelation;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.BatchIterators;
import io.crate.data.Input;
import io.crate.execution.dml.upsert.GeneratedColumns;
import io.crate.execution.dml.upsert.InsertSourceGen;
import io.crate.execution.engine.collect.collectors.CollectorFieldsVisitor;
import io.crate.execution.engine.collect.collectors.LuceneBatchIterator;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.sql.tree.QualifiedName;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.cache.query.DisabledQueryCache;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.ArrayMapper;
import org.elasticsearch.index.mapper.ArrayTypeParser;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.mock.orig.Mockito.mock;

public final class QueryTester implements AutoCloseable {

    private final BiFunction<ColumnIdent, Query, LuceneBatchIterator> getIterator;
    private final Function<String, Query> expressionToQuery;
    private final AutoCloseable onClose;

    public static class Builder {

        private final LuceneQueryBuilder queryBuilder;
        private final DocTableInfo table;
        private final MapperService mapperService;
        private final AtomicReference<QueryShardContext> queryShardContext = new AtomicReference<>();
        private final IndexCache indexCache;
        private final SQLExecutor sqlExecutor;
        private final IndexWriter writer;
        private final IndexFieldDataService indexFieldDataService;
        private final SqlExpressions expressions;
        private final LuceneReferenceResolver luceneReferenceResolver;
        private final NodeEnvironment nodeEnvironment;

        public Builder(Path tempDir,
                       ThreadPool threadPool,
                       ClusterService clusterService,
                       String createTableStmt) throws IOException {
            sqlExecutor = SQLExecutor
                .builder(clusterService)
                .addTable(createTableStmt)
                .build();

            DocSchemaInfo docSchema = findDocSchema(sqlExecutor.schemas());
            table = (DocTableInfo) docSchema.getTables().iterator().next();
            Index index = new Index(table.ident().indexName(), UUIDs.randomBase64UUID());
            queryBuilder = new LuceneQueryBuilder(sqlExecutor.functions());
            Settings nodeSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put("path.home", tempDir.toAbsolutePath())
                .build();
            Environment env = new Environment(nodeSettings, tempDir.resolve("config"));
            IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(index, nodeSettings);
            AnalysisRegistry analysisRegistry = new AnalysisModule(env, Collections.emptyList()).getAnalysisRegistry();
            IndexAnalyzers indexAnalyzers = analysisRegistry.build(idxSettings);
            ScriptService scriptService = mock(ScriptService.class);
            SimilarityService similarityService = new SimilarityService(
                idxSettings, scriptService, Collections.emptyMap());
            MapperRegistry mapperRegistry = new IndicesModule(Collections.singletonList(new MapperPlugin() {
                @Override
                public Map<String, Mapper.TypeParser> getMappers() {
                    return Collections.singletonMap(ArrayMapper.CONTENT_TYPE, new ArrayTypeParser());
                }
            })).getMapperRegistry();
            mapperService = new MapperService(
                idxSettings,
                indexAnalyzers,
                NamedXContentRegistry.EMPTY,
                similarityService,
                mapperRegistry,
                queryShardContext::get
            );
            IndexMetaData indexMetaData = clusterService.state().getMetaData().getIndices().get(table.concreteIndices()[0]);
            mapperService.merge(
                "default",
                indexMetaData.mappingOrDefault("default").source(),
                MapperService.MergeReason.MAPPING_UPDATE,
                true
            );
            BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(
                idxSettings,
                mock(BitsetFilterCache.Listener.class)
            );
            DisabledQueryCache queryCache = new DisabledQueryCache(idxSettings);
            indexCache = new IndexCache(
                idxSettings,
                queryCache,
                bitsetFilterCache
            );
            IndexModule indexModule = new IndexModule(idxSettings, analysisRegistry);
            Client client = mock(Client.class);
            NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
            nodeEnvironment = new NodeEnvironment(Settings.EMPTY, env);
            luceneReferenceResolver = new LuceneReferenceResolver(
                mapperService::fullName,
                idxSettings
            );
            IndexService indexService = indexModule.newIndexService(
                nodeEnvironment,
                NamedXContentRegistry.EMPTY,
                new IndexService.ShardStoreDeleter() {
                    @Override
                    public void deleteShardStore(String reason, ShardLock lock, IndexSettings indexSettings) throws IOException {

                    }

                    @Override
                    public void addPendingDelete(ShardId shardId, IndexSettings indexSettings) {

                    }
                },
                new NoneCircuitBreakerService(),
                BigArrays.NON_RECYCLING_INSTANCE,
                threadPool,
                scriptService,
                client,
                new IndicesQueryCache(Settings.EMPTY),
                mapperRegistry,
                new IndicesFieldDataCache(Settings.EMPTY, mock(IndexFieldDataCache.Listener.class)),
                namedWriteableRegistry
            );
            indexFieldDataService = indexService.fieldData();
            IndexWriterConfig conf = new IndexWriterConfig(new StandardAnalyzer());
            writer = new IndexWriter(new RAMDirectory(), conf);
            QualifiedName tableName = new QualifiedName(table.ident().name());
            DocTableRelation docTableRelation = new DocTableRelation(table);
            expressions = new SqlExpressions(
                Collections.singletonMap(tableName, docTableRelation),
                docTableRelation
            );
            DirectoryReader reader = DirectoryReader.open(writer, true, true);
            queryShardContext.set(new QueryShardContext(
                0,
                idxSettings,
                bitsetFilterCache,
                indexFieldDataService::getForField,
                Builder.this.mapperService,
                similarityService,
                scriptService,
                NamedXContentRegistry.EMPTY,
                namedWriteableRegistry,
                client,
                reader,
                System::currentTimeMillis,
                "dummyClusterAlias"
            ));
        }

        private DocSchemaInfo findDocSchema(Schemas schemas) {
            for (SchemaInfo schema : schemas) {
                if (schema instanceof DocSchemaInfo) {
                    return (DocSchemaInfo) schema;
                }
            }
            throw new IllegalArgumentException("Create table statement must result in the creation of a user table");
        }

        public Builder addDoc(String column, Object value) throws IOException {
            DocumentMapper mapper = mapperService.documentMapperWithAutoCreate("default").getDocumentMapper();
            InsertSourceGen sourceGen = InsertSourceGen.of(
                sqlExecutor.functions(),
                table,
                GeneratedColumns.Validation.NONE,
                Collections.singletonList(table.getReference(ColumnIdent.fromPath(column)))
            );
            BytesReference source = sourceGen.generateSource(new Object[]{value});
            SourceToParse sourceToParse = SourceToParse.source(
                table.concreteIndices()[0],
                "default",
                UUIDs.randomBase64UUID(),
                source,
                XContentType.JSON
            );
            ParsedDocument parsedDocument = mapper.parse(sourceToParse);
            writer.addDocuments(parsedDocument.docs());
            writer.commit();
            return this;
        }

        private LuceneBatchIterator getIterator(ColumnIdent column, Query query) {
            InputFactory inputFactory = new InputFactory(sqlExecutor.functions());
            InputFactory.Context<LuceneCollectorExpression<?>> ctx = inputFactory.ctxForRefs(luceneReferenceResolver);
            Input<?> input = ctx.add(requireNonNull(table.getReference(column),
                "column must exist in created table: " + column));
            IndexSearcher indexSearcher;
            try {
                indexSearcher = new IndexSearcher(DirectoryReader.open(writer));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new LuceneBatchIterator(
                indexSearcher,
                query,
                null,
                false,
                new CollectorContext(queryShardContext.get()::getForField, new CollectorFieldsVisitor(1)),
                new RamAccountingContext("dummy", new NoopCircuitBreaker("dummy")),
                Collections.singletonList(input),
                ctx.expressions()
            );
        }

        public QueryTester build() {
            return new QueryTester(
                this::getIterator,
                expr -> queryBuilder.convert(
                    expressions.normalize(expressions.asSymbol(expr)),
                    mapperService,
                    queryShardContext.get(),
                    indexCache
                ).query(),
                () -> {
                    writer.close();
                    nodeEnvironment.close();
                }
            );
        }
    }

    private QueryTester(BiFunction<ColumnIdent, Query, LuceneBatchIterator> getIterator,
                        Function<String, Query> expressionToQuery,
                        AutoCloseable onClose) {
        this.getIterator = getIterator;
        this.expressionToQuery = expressionToQuery;
        this.onClose = onClose;
    }

    public List<Object> runQuery(String resultColumn, String expression) throws Exception {
        Query query = expressionToQuery.apply(expression);
        LuceneBatchIterator batchIterator = getIterator.apply(ColumnIdent.fromPath(resultColumn), query);
        return BatchIterators.collect(
            batchIterator,
            Collectors.mapping(row -> row.get(0), Collectors.toList())
        ).get(5, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws Exception {
        onClose.close();
    }
}

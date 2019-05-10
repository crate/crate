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

import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.metadata.RelationName;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
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
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.ArrayMapper;
import org.elasticsearch.index.mapper.ArrayTypeParser;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

public final class IndexEnv implements AutoCloseable {

    private final AtomicReference<QueryShardContext> queryShardContext = new AtomicReference<>();
    private final MapperService mapperService;
    private final LuceneReferenceResolver luceneReferenceResolver;
    private final NodeEnvironment nodeEnvironment;
    private final IndexCache indexCache;
    private final IndexService indexService;
    private final IndexFieldDataService indexFieldDataService;
    private final IndexWriter writer;

    public IndexEnv(ThreadPool threadPool,
                    RelationName relation,
                    ClusterState clusterState,
                    Version indexVersion,
                    Path tempDir) throws IOException  {
        String indexName = relation.indexNameOrAlias();
        assert clusterState.metaData().hasIndex(indexName) : "ClusterState must contain the index: " + indexName;

        Index index = new Index(indexName, UUIDs.randomBase64UUID());
        Settings nodeSettings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, indexVersion)
            .put("path.home", tempDir.toAbsolutePath())
            .build();
        Environment env = new Environment(nodeSettings, tempDir.resolve("config"));
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(index, nodeSettings);
        AnalysisRegistry analysisRegistry = new AnalysisModule(env, Collections.emptyList()).getAnalysisRegistry();
        IndexAnalyzers indexAnalyzers = analysisRegistry.build(idxSettings);
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
            mapperRegistry,
            queryShardContext::get
        );
        IndexMetaData indexMetaData = clusterState.getMetaData().index(indexName);
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
        IndexModule indexModule = new IndexModule(idxSettings, analysisRegistry, new InternalEngineFactory(), Collections.emptyMap());
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        nodeEnvironment = new NodeEnvironment(Settings.EMPTY, env);
        luceneReferenceResolver = new LuceneReferenceResolver(mapperService::fullName);
        indexService = indexModule.newIndexService(
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
            new IndicesQueryCache(Settings.EMPTY),
            mapperRegistry,
            new IndicesFieldDataCache(Settings.EMPTY, mock(IndexFieldDataCache.Listener.class)),
            namedWriteableRegistry
        );
        indexFieldDataService = indexService.fieldData();
        IndexWriterConfig conf = new IndexWriterConfig(new StandardAnalyzer());
        writer = new IndexWriter(new ByteBuffersDirectory(), conf);
        queryShardContext.set(new QueryShardContext(
            idxSettings,
            indexFieldDataService::getForField,
            mapperService,
            System::currentTimeMillis
        ));
    }

    @Override
    public void close() throws Exception {
        indexService.close("stopping", true);
        writer.close();
        nodeEnvironment.close();
    }

    public MapperService mapperService() {
        return mapperService;
    }

    public QueryShardContext queryShardContext() {
        return queryShardContext.get();
    }

    public IndexCache indexCache() {
        return indexCache;
    }

    public IndexWriter writer() {
        return writer;
    }

    public LuceneReferenceResolver luceneReferenceResolver() {
        return luceneReferenceResolver;
    }

    public IndexService indexService() {
        return indexService;
    }
}

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
import org.apache.lucene.store.RAMDirectory;
import io.crate.es.Version;
import io.crate.es.cluster.ClusterModule;
import io.crate.es.cluster.ClusterState;
import io.crate.es.cluster.metadata.IndexMetaData;
import io.crate.es.common.UUIDs;
import io.crate.es.common.io.stream.NamedWriteableRegistry;
import io.crate.es.common.settings.Settings;
import io.crate.es.common.util.BigArrays;
import io.crate.es.common.xcontent.NamedXContentRegistry;
import io.crate.es.env.Environment;
import io.crate.es.env.NodeEnvironment;
import io.crate.es.env.ShardLock;
import io.crate.es.index.Index;
import io.crate.es.index.IndexModule;
import io.crate.es.index.IndexService;
import io.crate.es.index.IndexSettings;
import io.crate.es.index.analysis.AnalysisRegistry;
import io.crate.es.index.analysis.IndexAnalyzers;
import io.crate.es.index.cache.IndexCache;
import io.crate.es.index.cache.bitset.BitsetFilterCache;
import io.crate.es.index.cache.query.DisabledQueryCache;
import io.crate.es.index.engine.InternalEngineFactory;
import io.crate.es.index.fielddata.IndexFieldDataCache;
import io.crate.es.index.fielddata.IndexFieldDataService;
import io.crate.es.index.mapper.ArrayMapper;
import io.crate.es.index.mapper.ArrayTypeParser;
import io.crate.es.index.mapper.Mapper;
import io.crate.es.index.mapper.MapperService;
import io.crate.es.index.query.QueryShardContext;
import io.crate.es.index.shard.ShardId;
import io.crate.es.indices.IndicesModule;
import io.crate.es.indices.IndicesQueryCache;
import io.crate.es.indices.analysis.AnalysisModule;
import io.crate.es.indices.breaker.NoneCircuitBreakerService;
import io.crate.es.indices.fielddata.cache.IndicesFieldDataCache;
import io.crate.es.indices.mapper.MapperRegistry;
import io.crate.es.plugins.MapperPlugin;
import io.crate.es.test.IndexSettingsModule;
import io.crate.es.threadpool.ThreadPool;

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
        nodeEnvironment = new NodeEnvironment(Settings.EMPTY, env, nodeId -> {});
        luceneReferenceResolver = new LuceneReferenceResolver(
            mapperService::fullName,
            idxSettings
        );
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
        writer = new IndexWriter(new RAMDirectory(), conf);
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

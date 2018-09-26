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

package io.crate.lucene;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.IndexVersionCreated;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.ArrayMapper;
import org.elasticsearch.index.mapper.ArrayTypeParser;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.IndexSettingsModule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mockito.Answers;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class LuceneQueryBuilderTest extends CrateDummyClusterServiceUnitTest {

    private LuceneQueryBuilder builder;
    private IndexCache indexCache;
    private QueryShardContext queryShardContext;
    private MapperService mapperService;

    SqlExpressions expressions;
    Map<QualifiedName, AnalyzedRelation> sources;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Rule
    public TestName testName = new TestName();

    @Before
    public void prepare() throws Exception {
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService)
            .addTable("create table users (" +
                      " name string," +
                      " tags string index using fulltext not null," +
                      " x integer not null," +
                      " d double," +
                      " obj object as (" +
                      "     x integer," +
                      "     y integer" +
                      " )," +
                      " d_array array(double)," +
                      " y_array array(long)," +
                      " o_array array(object as (xs array(integer)))," +
                      " ts_array array(timestamp)," +
                      " shape geo_shape," +
                      " point geo_point," +
                      " ts timestamp," +
                      " addr ip" +
                      ")")
            .build();
        TableInfo users = sqlExecutor.schemas().getTableInfo(new RelationName("doc", "users"));
        TableRelation usersTr = new TableRelation(users);
        sources = ImmutableMap.of(new QualifiedName("users"), usersTr);
        expressions = new SqlExpressions(sources, usersTr);
        builder = new LuceneQueryBuilder(sqlExecutor.functions());
        indexCache = mock(IndexCache.class, Answers.RETURNS_MOCKS.get());

        Index index = new Index(users.ident().indexName(), UUIDs.randomBase64UUID());
        File homeFolder = temporaryFolder.newFolder();
        Settings nodeSettings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, indexVersion())
            .put("path.home", homeFolder.getAbsolutePath())
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(index, nodeSettings);
        when(indexCache.getIndexSettings()).thenReturn(idxSettings);
        IndexAnalyzers indexAnalyzers = createIndexAnalyzers(idxSettings, nodeSettings, homeFolder.toPath().resolve("config"));
        ScriptService scriptService = mock(ScriptService.class);
        mapperService = createMapperService(idxSettings, indexAnalyzers, scriptService);

        IndexMetaData usersIndex = clusterService.state().getMetaData().getIndices().get("users");
        CompressedXContent mappingSource = usersIndex.mappingOrDefault("default").source();

        mapperService.merge("default", mappingSource, MapperService.MergeReason.MAPPING_UPDATE, true);

        IndexFieldDataService indexFieldDataService = mock(IndexFieldDataService.class);
        IndexFieldData geoFieldData = mock(IndexGeoPointFieldData.class);

        when(geoFieldData.getFieldName()).thenReturn("point");
        when(indexFieldDataService.getForField(mapperService.fullName("point"))).thenReturn(geoFieldData);

        queryShardContext = new QueryShardContext(
            0,
            idxSettings,
            new BitsetFilterCache(idxSettings, mock(BitsetFilterCache.Listener.class)),
            indexFieldDataService::getForField,
            mapperService,
            new SimilarityService(idxSettings, scriptService, Collections.emptyMap()),
            scriptService,
            xContentRegistry(),
            writableRegistry(),
            mock(Client.class),
            mock(IndexReader.class),
            System::currentTimeMillis,
            "dummyClusterAlias"
        );
    }

    private MapperService createMapperService(IndexSettings indexSettings,
                                              IndexAnalyzers indexAnalyzers,
                                              ScriptService scriptService) {
        IndicesModule indicesModule = new IndicesModule(singletonList(
            new MapperPlugin() {
                @Override
                public Map<String, Mapper.TypeParser> getMappers() {
                    return Collections.singletonMap(ArrayMapper.CONTENT_TYPE, new ArrayTypeParser());
                }
            }));
        return new MapperService(
            indexSettings,
            indexAnalyzers,
            xContentRegistry(),
            new SimilarityService(indexSettings, scriptService, Collections.emptyMap()),
            indicesModule.getMapperRegistry(),
            () -> null
        );
    }

    private IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings, Settings nodeSettings, Path configPath) throws IOException {
        Environment env = new Environment(nodeSettings, configPath);
        AnalysisModule analysisModule = new AnalysisModule(env, Collections.emptyList());
        return analysisModule.getAnalysisRegistry().build(indexSettings);
    }

    protected Query convert(Symbol query) {
        return builder.convert(query, mapperService, queryShardContext, indexCache).query;
    }

    protected Query convert(String expression) {
        return convert(expressions.normalize(expressions.asSymbol(expression)));
    }

    private Version indexVersion() {
        try {
            Class<?> clazz = this.getClass();
            Method method = clazz.getMethod(testName.getMethodName());
            IndexVersionCreated annotation = method.getAnnotation(IndexVersionCreated.class);
            if (annotation == null) {
                annotation = clazz.getAnnotation(IndexVersionCreated.class);
                if (annotation == null) {
                    return Version.CURRENT;
                }
            }
            int value = annotation.value();
            if (value == -1) {
                return Version.CURRENT;
            }
            return Version.fromId(value);
        } catch (NoSuchMethodException ignored) {
            return Version.CURRENT;
        }
    }
}

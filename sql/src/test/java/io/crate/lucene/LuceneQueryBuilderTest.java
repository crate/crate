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
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.IndexVersionCreated;
import io.crate.testing.SqlExpressions;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
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

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class LuceneQueryBuilderTest extends CrateUnitTest {

    private LuceneQueryBuilder builder;
    private IndexCache indexCache;
    private IndexFieldDataService indexFieldDataService;
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
        DocTableInfo users = TestingTableInfo.builder(new TableIdent(null, "users"), null)
            .add("name", DataTypes.STRING)
            .add("x", DataTypes.INTEGER, null, ColumnPolicy.DYNAMIC, Reference.IndexType.NOT_ANALYZED, false, false)
            .add("d", DataTypes.DOUBLE)
            .add("d_array", new ArrayType(DataTypes.DOUBLE))
            .add("y_array", new ArrayType(DataTypes.LONG))
            .add("o_array", new ArrayType(DataTypes.OBJECT))
            .add("shape", DataTypes.GEO_SHAPE)
            .add("point", DataTypes.GEO_POINT)
            .build();
        TableRelation usersTr = new TableRelation(users);
        sources = ImmutableMap.of(new QualifiedName("users"), usersTr);

        expressions = new SqlExpressions(sources, usersTr);
        builder = new LuceneQueryBuilder(expressions.getInstance(Functions.class));
        indexCache = mock(IndexCache.class, Answers.RETURNS_MOCKS.get());

        Index index = new Index(users.ident().indexName(), UUIDs.randomBase64UUID());
        Settings nodeSettings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, indexVersion())
            .put("path.home", temporaryFolder.newFolder())
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(index, nodeSettings);
        when(indexCache.getIndexSettings()).thenReturn(idxSettings);
        IndexAnalyzers indexAnalyzers = createIndexAnalyzers(idxSettings, nodeSettings);
        mapperService = createMapperService(idxSettings, indexAnalyzers);

        // @formatter:off
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("default")
                .startObject("properties")
                    .startObject("name").field("type", "keyword").endObject()
                    .startObject("x").field("type", "integer").endObject()
                    .startObject("d").field("type", "double").endObject()
                    .startObject("point").field("type", "geo_point").endObject()
                    .startObject("shape").field("type", "geo_shape").endObject()
                    .startObject("d_array")
                        .field("type", "array")
                        .startObject("inner")
                            .field("type", "double")
                        .endObject()
                    .endObject()
                    .startObject("y_array")
                        .field("type", "array")
                        .startObject("inner")
                            .field("type", "integer")
                        .endObject()
                    .endObject()
                    .startObject("o_array")
                        .field("type", "array")
                        .startObject("inner")
                            .field("type", "object")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
            .endObject();
        // @formatter:on
        mapperService.merge("default",
            new CompressedXContent(xContentBuilder.bytes()), MapperService.MergeReason.MAPPING_UPDATE, true);

        indexFieldDataService = mock(IndexFieldDataService.class);
        IndexFieldData geoFieldData = mock(IndexGeoPointFieldData.class);

        when(geoFieldData.getFieldName()).thenReturn("point");
        when(indexFieldDataService.getForField(mapperService.fullName("point"))).thenReturn(geoFieldData);

        queryShardContext = new QueryShardContext(
            0,
            idxSettings,
            new BitsetFilterCache(idxSettings, mock(BitsetFilterCache.Listener.class)),
            indexFieldDataService,
            mapperService,
            new SimilarityService(idxSettings, Collections.emptyMap()),
            mock(ScriptService.class),
            xContentRegistry(),
            mock(Client.class),
            mock(IndexReader.class),
            System::currentTimeMillis
        );
    }

    private MapperService createMapperService(IndexSettings indexSettings, IndexAnalyzers indexAnalyzers) {
        IndicesModule indicesModule = new IndicesModule(Collections.singletonList(
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
            new SimilarityService(indexSettings, Collections.emptyMap()),
            indicesModule.getMapperRegistry(),
            () -> null
        );
    }

    private IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings, Settings nodeSettings) throws IOException {
        Environment env = new Environment(nodeSettings);
        AnalysisModule analysisModule = new AnalysisModule(env, Collections.emptyList());
        return analysisModule.getAnalysisRegistry().build(indexSettings);
    }

    private WhereClause asWhereClause(String expression) {
        return new WhereClause(expressions.normalize(expressions.asSymbol(expression)));
    }

    protected Query convert(WhereClause clause) {
        return builder.convert(clause, mapperService, queryShardContext, indexFieldDataService, indexCache).query;
    }

    protected Query convert(String expression) {
        return convert(asWhereClause(expression));
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

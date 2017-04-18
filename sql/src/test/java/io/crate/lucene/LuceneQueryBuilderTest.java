/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.lucene.match.CrateRegexQuery;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.spatial.prefix.IntersectsPrefixTreeQuery;
import org.apache.lucene.spatial.prefix.WithinPrefixTreeQuery;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.array.DynamicArrayFieldMapperBuilderFactoryProvider;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.test.IndexSettingsModule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Answers;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LuceneQueryBuilderTest extends CrateUnitTest {

    private LuceneQueryBuilder builder;
    private IndexCache indexCache;
    private SqlExpressions expressions;
    private Map<QualifiedName, AnalyzedRelation> sources;
    private IndexFieldDataService indexFieldDataService;
    private MapperService mapperService;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void prepare() throws Exception {
        DocTableInfo users = TestingTableInfo.builder(new TableIdent(null, "users"), null)
            .add("name", DataTypes.STRING)
            .add("x", DataTypes.INTEGER, null, ColumnPolicy.DYNAMIC, Reference.IndexType.NOT_ANALYZED, false, false)
            .add("d", DataTypes.DOUBLE)
            .add("d_array", new ArrayType(DataTypes.DOUBLE))
            .add("y_array", new ArrayType(DataTypes.LONG))
            .add("shape", DataTypes.GEO_SHAPE)
            .add("point", DataTypes.GEO_POINT)
            .build();
        TableRelation usersTr = new TableRelation(users);
        sources = ImmutableMap.of(new QualifiedName("users"), usersTr);

        expressions = new SqlExpressions(sources, usersTr);
        builder = new LuceneQueryBuilder(expressions.getInstance(Functions.class));
        indexCache = mock(IndexCache.class, Answers.RETURNS_MOCKS.get());

        Path tempDir = createTempDir();
        Index index = new Index(users.ident().indexName(), UUIDs.randomBase64UUID());
        Settings nodeSettings = Settings.builder().put("path.home", tempDir).build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(index, nodeSettings);
        when(indexCache.getIndexSettings()).thenReturn(idxSettings);
        IndexAnalyzers indexAnalyzers = createAnalysisService(idxSettings, nodeSettings);
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
    }

    private MapperService createMapperService(IndexSettings indexSettings, IndexAnalyzers indexAnalyzers) {
        DynamicArrayFieldMapperBuilderFactoryProvider arrayMapperProvider =
            new DynamicArrayFieldMapperBuilderFactoryProvider();
        arrayMapperProvider.dynamicArrayFieldMapperBuilderFactory = new BuilderFactory();
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
            NamedXContentRegistry.EMPTY,
            new SimilarityService(indexSettings, Collections.emptyMap()),
            indicesModule.getMapperRegistry(),
            () -> mock(QueryShardContext.class),
            arrayMapperProvider
        );
    }

    private IndexAnalyzers createAnalysisService(IndexSettings indexSettings, Settings nodeSettings) throws IOException {
        Environment env = new Environment(nodeSettings);
        AnalysisModule analysisModule = new AnalysisModule(env, Collections.emptyList());
        return analysisModule.getAnalysisRegistry().build(indexSettings);
    }

    private WhereClause asWhereClause(String expression) {
        return new WhereClause(expressions.normalize(expressions.asSymbol(expression)));
    }

    private Query convert(WhereClause clause) {
        return builder.convert(clause, mapperService, null, indexFieldDataService, indexCache).query;
    }

    private Query convert(String expression) {
        return convert(asWhereClause(expression));
    }

    @Test
    public void testNoMatchWhereClause() throws Exception {
        Query query = convert(WhereClause.NO_MATCH);
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
    }

    @Test
    public void testWhereRefEqNullWithDifferentTypes() throws Exception {
        for (DataType type : DataTypes.PRIMITIVE_TYPES) {
            DocTableInfo tableInfo = TestingTableInfo.builder(new TableIdent(null, "test_primitive"), null)
                .add("x", type)
                .build();
            TableRelation tableRelation = new TableRelation(tableInfo);
            Map<QualifiedName, AnalyzedRelation> tableSources = ImmutableMap.of(new QualifiedName(tableInfo.ident().name()), tableRelation);
            SqlExpressions sqlExpressions = new SqlExpressions(
                tableSources, tableRelation, new Object[]{null}, SessionContext.SYSTEM_SESSION);

            Query query = convert(new WhereClause(sqlExpressions.normalize(sqlExpressions.asSymbol("x = ?"))));

            // must always become a MatchNoDocsQuery
            // string: term query with null would cause NPE
            // int/numeric: rangeQuery from null to null would match all
            // bool:  term would match false too because of the condition in the eq query builder
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        }
    }

    @Test
    public void testWhereRefEqRef() throws Exception {
        Query query = convert("name = name");
        assertThat(query, instanceOf(GenericFunctionQuery.class));
    }

    @Test
    public void testLteQuery() throws Exception {
        Query query = convert("x <= 10");
        assertThat(query.toString(), is("x:[-2147483648 TO 10]"));
    }

    @Test
    public void testNotEqOnNotNullableColumnQuery() throws Exception {
        Query query = convert("x != 10");
        assertThat(query, instanceOf(BooleanQuery.class));
        assertThat(query.toString(), is("+(+*:* -x:[10 TO 10])"));

        query = convert("not x = 10");
        assertThat(query, instanceOf(BooleanQuery.class));
        assertThat(query.toString(), is("+(+*:* -x:[10 TO 10])"));
    }

    @Test
    public void testEqOnTwoArraysBecomesGenericFunctionQuery() throws Exception {
        Query query = convert("y_array = [10, 20, 30]");
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.clauses().get(0).getQuery(), instanceOf(PointInSetQuery.class));
        assertThat(booleanQuery.clauses().get(1).getQuery(), instanceOf(GenericFunctionQuery.class));
    }

    @Test
    public void testEqOnTwoArraysBecomesGenericFunctionQueryAllValuesNull() throws Exception {
        SqlExpressions sqlExpressions = new SqlExpressions(sources, new Object[]{new Object[]{null, null, null}});
        Query query = convert(new WhereClause(expressions.normalize(sqlExpressions.asSymbol("y_array = ?"))));
        assertThat(query, instanceOf(GenericFunctionQuery.class));
    }

    @Test
    public void testEqOnArrayWithTooManyClauses() throws Exception {
        Object[] values = new Object[2000]; // should trigger the TooManyClauses exception
        Arrays.fill(values, 10L);
        SqlExpressions sqlExpressions = new SqlExpressions(sources, new Object[]{values});
        Query query = convert(new WhereClause(expressions.normalize(sqlExpressions.asSymbol("y_array = ?"))));
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.clauses().get(0).getQuery(), instanceOf(PointInSetQuery.class));
        assertThat(booleanQuery.clauses().get(1).getQuery(), instanceOf(GenericFunctionQuery.class));
    }

    @Test
    public void testGteQuery() throws Exception {
        Query query = convert("x >= 10");
        assertThat(query.toString(), is("x:[10 TO 2147483647]"));
    }

    @Test
    public void testWhereRefInSetLiteralIsConvertedToTermsQuery() throws Exception {
        Query query = convert("x in (1, 3)");
        assertThat(query, instanceOf(PointInSetQuery.class));
    }

    @Test
    public void testWhereStringRefInSetLiteralIsConvertedToTermsQuery() throws Exception {
        Query query = convert("name in ('foo', 'bar')");
        assertThat(query, instanceOf(TermsQuery.class));
    }

    /**
     * Make sure we still sport the fast Lucene regular
     * expression engine when not using PCRE features.
     */
    @Test
    public void testRegexQueryFast() throws Exception {
        Query query = convert("name ~ '[a-z]'");
        assertThat(query, instanceOf(ConstantScoreQuery.class));
        ConstantScoreQuery scoreQuery = (ConstantScoreQuery) query;
        assertThat(scoreQuery.getQuery(), instanceOf(RegexpQuery.class));
    }

    /**
     * When using PCRE features, switch to different
     * regex implementation on top of java.util.regex.
     */
    @Test
    public void testRegexQueryPcre() throws Exception {
        Query query = convert("name ~ '\\D'");
        assertThat(query, instanceOf(CrateRegexQuery.class));
    }

    @Test
    public void testIdQuery() throws Exception {
        Query query = convert("_id = 'i1'");
        assertThat(query, instanceOf(TermQuery.class));
        assertThat(query.toString(), is("_uid:default#i1"));
    }

    @Test
    public void testAnyEqArrayLiteral() throws Exception {
        Query query = convert("d = any([-1.5, 0.0, 1.5])");
        assertThat(query, instanceOf(PointInSetQuery.class));
    }

    @Test
    public void testAnyEqArrayReference() throws Exception {
        Query query = convert("1.5 = any(d_array)");
        assertThat(query, instanceOf(PointRangeQuery.class));
        assertThat(query.toString(), startsWith("d_array"));
    }

    @Test
    public void testAnyGreaterAndSmaller() throws Exception {
        Query ltQuery = convert("1.5 < any(d_array)");
        assertThat(ltQuery.toString(), is("d_array:[1.5000000000000002 TO Infinity]"));

        // d < ANY ([1.2, 3.5])
        Query ltQuery2 = convert("d < any ([1.2, 3.5])");
        assertThat(ltQuery2.toString(), is("(d:[-Infinity TO 1.1999999999999997] d:[-Infinity TO 3.4999999999999996])~1"));

        // 1.5d <= ANY (d_array)
        Query lteQuery = convert("1.5 <= any(d_array)");
        assertThat(lteQuery.toString(), is("d_array:[1.5 TO Infinity]"));

        // d <= ANY ([1.2, 3.5])
        Query lteQuery2 = convert("d <= any([1.2, 3.5])");
        assertThat(lteQuery2.toString(), is("(d:[-Infinity TO 1.2] d:[-Infinity TO 3.5])~1"));

        // 1.5d > ANY (d_array)
        Query gtQuery = convert("1.5 > any(d_array)");
        assertThat(gtQuery.toString(), is("d_array:[-Infinity TO 1.4999999999999998]"));

        // d > ANY ([1.2, 3.5])
        Query gtQuery2 = convert("d > any ([1.2, 3.5])");
        assertThat(gtQuery2.toString(), is("(d:[1.2000000000000002 TO Infinity] d:[3.5000000000000004 TO Infinity])~1"));

        // 1.5d >= ANY (d_array)
        Query gteQuery = convert("1.5 >= any(d_array)");
        assertThat(gteQuery.toString(), is("d_array:[-Infinity TO 1.5]"));

        // d >= ANY ([1.2, 3.5])
        Query gteQuery2 = convert("d >= any ([1.2, 3.5])");
        assertThat(gteQuery2.toString(), is("(d:[1.2 TO Infinity] d:[3.5 TO Infinity])~1"));
    }

    @Test
    public void testNeqAnyOnArrayLiteral() throws Exception {
        Query neqQuery = convert("name != any (['a', 'b', 'c'])");
        assertThat(neqQuery, instanceOf(BooleanQuery.class));

        BooleanClause booleanClause = ((BooleanQuery) neqQuery).clauses().get(1);
        assertThat(booleanClause.getOccur(), is(BooleanClause.Occur.MUST_NOT));
        assertThat(booleanClause.getQuery().toString(), is("+name:a +name:b +name:c"));
    }

    @Test
    public void testLikeAnyOnArrayLiteral() throws Exception {
        Query likeQuery = convert("name like any (['a', 'b', 'c'])");
        assertThat(likeQuery, instanceOf(BooleanQuery.class));
        BooleanQuery likeBQuery = (BooleanQuery) likeQuery;
        assertThat(likeBQuery.clauses().size(), is(3));
        for (int i = 0; i < 2; i++) {
            // like --> ConstantScoreQuery with regexp-filter
            Query filteredQuery = likeBQuery.clauses().get(i).getQuery();
            assertThat(filteredQuery, instanceOf(WildcardQuery.class));
        }
    }

    @Test
    public void testNotLikeAnyOnArrayLiteral() throws Exception {
        Query notLikeQuery = convert("name not like any (['a', 'b', 'c'])");
        assertThat(notLikeQuery, instanceOf(BooleanQuery.class));
        BooleanQuery notLikeBQuery = (BooleanQuery) notLikeQuery;
        assertThat(notLikeBQuery.clauses(), hasSize(2));
        BooleanClause clause = notLikeBQuery.clauses().get(1);
        assertThat(clause.getOccur(), is(BooleanClause.Occur.MUST_NOT));
        assertThat(((BooleanQuery) clause.getQuery()).clauses(), hasSize(3));
        for (BooleanClause innerClause : ((BooleanQuery) clause.getQuery()).clauses()) {
            assertThat(innerClause.getOccur(), is(BooleanClause.Occur.MUST));
            assertThat(innerClause.getQuery(), instanceOf(WildcardQuery.class));
        }
    }

    @Test
    public void testLessThanAnyOnArrayLiteral() throws Exception {
        Query ltQuery2 = convert("name < any (['a', 'b', 'c'])");
        assertThat(ltQuery2, instanceOf(BooleanQuery.class));
        BooleanQuery ltBQuery = (BooleanQuery) ltQuery2;
        assertThat(ltBQuery.toString(), is("(name:{* TO a} name:{* TO b} name:{* TO c})~1"));
    }

    @Test
    public void testSqlLikeToLuceneWildcard() throws Exception {
        assertThat(LuceneQueryBuilder.convertSqlLikeToLuceneWildcard("%\\\\%"), is("*\\\\*"));
        assertThat(LuceneQueryBuilder.convertSqlLikeToLuceneWildcard("%\\\\_"), is("*\\\\?"));
        assertThat(LuceneQueryBuilder.convertSqlLikeToLuceneWildcard("%\\%"), is("*%"));

        assertThat(LuceneQueryBuilder.convertSqlLikeToLuceneWildcard("%me"), is("*me"));
        assertThat(LuceneQueryBuilder.convertSqlLikeToLuceneWildcard("\\%me"), is("%me"));
        assertThat(LuceneQueryBuilder.convertSqlLikeToLuceneWildcard("*me"), is("\\*me"));

        assertThat(LuceneQueryBuilder.convertSqlLikeToLuceneWildcard("_me"), is("?me"));
        assertThat(LuceneQueryBuilder.convertSqlLikeToLuceneWildcard("\\_me"), is("_me"));
        assertThat(LuceneQueryBuilder.convertSqlLikeToLuceneWildcard("?me"), is("\\?me"));
    }


    /**
     * geo match tests below... error cases (wrong matchType, etc.) are not tests here because validation is done in the
     * analyzer
     */

    @Test
    public void testGeoShapeMatchWithDefaultMatchType() throws Exception {
        Query query = convert("match(shape, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')");
        assertThat(query, instanceOf(IntersectsPrefixTreeQuery.class));
    }

    @Test
    public void testGeoShapeMatchDisJoint() throws Exception {
        Query query = convert("match(shape, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') using disjoint");
        assertThat(query, instanceOf(ConstantScoreQuery.class));
        Query booleanQuery = ((ConstantScoreQuery) query).getQuery();
        assertThat(booleanQuery, instanceOf(BooleanQuery.class));

        BooleanClause existsClause = ((BooleanQuery) booleanQuery).clauses().get(0);
        BooleanClause intersectsClause = ((BooleanQuery) booleanQuery).clauses().get(1);

        assertThat(existsClause.getQuery(), instanceOf(TermRangeQuery.class));
        assertThat(intersectsClause.getQuery(), instanceOf(IntersectsPrefixTreeQuery.class));
    }

    @Test
    public void testGeoShapeMatchWithin() throws Exception {
        Query query = convert("match(shape, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') using within");
        assertThat(query, instanceOf(WithinPrefixTreeQuery.class));
    }

    @Test
    public void testWithinFunctionTooFewPoints() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("at least 4 polygon points required");
        convert("within(point, {type='LineString', coordinates=[[0.0, 0.0], [1.0, 1.0]]})");
    }

    @Test
    public void testWithinFunction() throws Exception {
        Query eqWithinQuery = convert("within(point, {type='LineString', coordinates=[[0.0, 0.0], [1.0, 1.0], [2.0, 1.0]]})");
        assertThat(eqWithinQuery.toString(), is("GeoPointInPolygonQuery: field=point: Polygon: [[0.0, 0.0] [1.0, 1.0] [1.0, 2.0] [0.0, 0.0] ]"));
    }

    @Test
    public void testLikeWithBothSidesReferences() throws Exception {
        Query query = convert("name like name");
        assertThat(query, instanceOf(GenericFunctionQuery.class));
    }

    @Test
    public void testWithinFunctionWithShapeReference() throws Exception {
        // shape references cannot use the inverted index, so use generic function here
        Query eqWithinQuery = convert("within(point, shape)");
        assertThat(eqWithinQuery, instanceOf(GenericFunctionQuery.class));
    }

    @Test
    public void testWhereInIsOptimized() throws Exception {
        Query query = convert("name in ('foo', 'bar')");
        assertThat(query, instanceOf(TermsQuery.class));
        assertThat(query.toString(), is("name:bar name:foo"));
    }

    @Test
    public void testRewriteDocReferenceInWhereClause() throws Exception {
        Query query = convert("_doc['name'] = 'foo'");
        assertThat(query, instanceOf(TermQuery.class));
        assertThat(query.toString(), is("name:foo"));
        query = convert("_doc = {\"name\"='foo'}");
        assertThat(query, instanceOf(GenericFunctionQuery.class));
    }

    @Test
    public void testMatchQueryTermMustNotBeNull() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("cannot use NULL as query term in match predicate");
        convert("match(name, null)");
    }

    @Test
    public void testMatchQueryTermMustBeALiteral() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("queryTerm must be a literal");
        convert("match(name, name)");
    }
}

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
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Reference;
import io.crate.metadata.Functions;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.operator.EqOperator;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.sandbox.queries.regex.RegexQuery;
import org.apache.lucene.search.*;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.internal.SearchContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;

import java.util.Arrays;
import java.util.Map;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;


public class LuceneQueryBuilderTest extends CrateUnitTest {

    private LuceneQueryBuilder builder;
    private SearchContext searchContext;
    private IndexCache indexCache;
    private SqlExpressions expressions;
    private EvaluatingNormalizer normalizer;

    @Before
    public void prepare() throws Exception {
        DocTableInfo users = TestingTableInfo.builder(new TableIdent(null, "users"), null)
                .add("name", DataTypes.STRING)
                .add("x", DataTypes.INTEGER)
                .add("d", DataTypes.DOUBLE)
                .add("d_array", new ArrayType(DataTypes.DOUBLE))
                .add("y_array", new ArrayType(DataTypes.LONG))
                .add("shape", DataTypes.GEO_SHAPE)
                .add("point", DataTypes.GEO_POINT)
                .build();
        TableRelation usersTr = new TableRelation(users);
        Map<QualifiedName, AnalyzedRelation> sources = ImmutableMap.<QualifiedName, AnalyzedRelation>of(
                new QualifiedName("users"), usersTr);

        expressions = new SqlExpressions(sources);
        normalizer = new EvaluatingNormalizer(expressions.analysisMD(), usersTr, true);
        builder = new LuceneQueryBuilder(expressions.getInstance(Functions.class));

        searchContext = mock(SearchContext.class, Answers.RETURNS_MOCKS.get());
        MapperService mapperService = mock(MapperService.class);

        // TODO: FIX ME!

        /* Mapper.BuilderContext context = new Mapper.BuilderContext(Settings.EMPTY, new ContentPath(1));
        GeoShapeFieldMapper shape = MapperBuilders.geoShapeField("shape").build(context);
        when(mapperService.smartNameFieldMapper(eq("shape"))).thenReturn(shape);
        when(searchContext.mapperService()).thenReturn(mapperService);
        indexCache = mock(IndexCache.class, Answers.RETURNS_MOCKS.get());
        FilterCache filterCache = mock(FilterCache.class);
        when(indexCache.filter()).thenReturn(filterCache);
        when(filterCache.cache(Matchers.any(Filter.class))).thenAnswer(new Answer<Filter>() {
            @Override
            public Filter answer(InvocationOnMock invocation) throws Throwable {
                return (Filter) invocation.getArguments()[0];
            }
        });

        // mock geo point mapper stuff
        MapperService.SmartNameFieldMappers smartNameFieldMappers = mock(MapperService.SmartNameFieldMappers.class);
        when(mapperService.smartName("point")).thenReturn(smartNameFieldMappers);
        when(smartNameFieldMappers.hasMapper()).thenReturn(true);

        GeoPointFieldMapper mapper = MapperBuilders.geoPointField("point").build(context);
        when(smartNameFieldMappers.mapper()).thenReturn(mapper);

        IndexFieldDataService indexFieldDataService = mock(IndexFieldDataService.class);
        when(searchContext.fieldData()).thenReturn(indexFieldDataService);
        IndexFieldData geoFieldData = mock(IndexGeoPointFieldData.class);
        when(geoFieldData.getFieldNames()).thenReturn(new FieldMapper.Names("point"));
        when(indexFieldDataService.getForField(mapper)).thenReturn(geoFieldData); */
    }

    private WhereClause asWhereClause(String expression) {
        return new WhereClause(normalizer.normalize(expressions.asSymbol(expression)));
    }

    private Query convert(WhereClause clause) {
        return builder.convert(clause, searchContext.mapperService(), searchContext.fieldData(), indexCache).query;
    }

    private Query convert(String expression) {
        return convert(asWhereClause(expression));
    }

    @Test
    public void testNoMatchWhereClause() throws Exception {
        Query query = convert(WhereClause.NO_MATCH);
        assertThat(query, instanceOf(BooleanQuery.class));
        assertThat(((BooleanQuery) query).clauses().size(), is(0));
    }

    @Test
    public void testWhereRefEqNullWithDifferentTypes() throws Exception {
        for (DataType type : DataTypes.PRIMITIVE_TYPES) {
            Reference foo = createReference("foo", type);
            Query query = convert(whereClause(EqOperator.NAME, foo, Literal.newLiteral(type, null)));

            // must always become a MatchNoDocsQuery (empty BooleanQuery)
            // string: term query with null would cause NPE
            // int/numeric: rangeQuery from null to null would match all
            // bool:  term would match false too because of the condition in the eq query builder
            assertThat(query, instanceOf(BooleanQuery.class));
            assertThat(((BooleanQuery) query).clauses().size(), is(0));
        }
    }

    @Test
    public void testWhereRefEqRef() throws Exception {
        Query query = convert("name = name");
        assertThat(query, instanceOf(FilteredQuery.class));
    }

    @Test
    public void testLteQuery() throws Exception {
        Query query = convert("x <= 10");
        assertThat(query, instanceOf(NumericRangeQuery.class));
        assertThat(query.toString(), is("x:{* TO 10]"));
    }

    @Test
    public void testEqOnTwoArraysBecomesGenericFunctionQuery() throws Exception {
        Query query = convert("y_array = [10, 20, 30]");
        assertThat(query, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) query;

        assertThat(filteredQuery.getFilter(), instanceOf(BooleanFilter.class));
        assertThat(filteredQuery.getQuery(), instanceOf(ConstantScoreQuery.class));

        BooleanFilter filter = (BooleanFilter) filteredQuery.getFilter();
        assertThat(filter.clauses().get(0).getFilter(), instanceOf(BooleanFilter.class)); // booleanFilter with terms filter
        assertThat(filter.clauses().get(1).getFilter(), instanceOf(Filter.class)); // generic function filter
    }

    @Test
    public void testEqOnTwoArraysBecomesGenericFunctionQueryAllValuesNull() throws Exception {
        DataType longArray = new ArrayType(DataTypes.LONG);
        Query query = convert(new WhereClause(createFunction(EqOperator.NAME,
                DataTypes.BOOLEAN,
                createReference("x", longArray),
                Literal.newLiteral(longArray, new Object[] { null, null, null }))));
        assertThat(query, instanceOf(FilteredQuery.class));
    }

    @Test
    public void testEqOnArrayWithTooManyClauses() throws Exception {
        Object[] values = new Object[2000]; // should trigger the TooManyClauses exception
        Arrays.fill(values, 10L);
        DataType longArray = new ArrayType(DataTypes.LONG);
        Query query = convert(new WhereClause(createFunction(EqOperator.NAME,
                DataTypes.BOOLEAN,
                createReference("x", longArray),
                Literal.newLiteral(longArray, values))));
        assertThat(query, instanceOf(FilteredQuery.class));
    }

    @Test
    public void testGteQuery() throws Exception {
        Query query = convert("x >= 10");
        assertThat(query, instanceOf(NumericRangeQuery.class));
        assertThat(query.toString(), is("x:[10 TO *}"));
    }

    @Test
    public void testWhereRefInSetLiteralIsConvertedToBooleanQuery() throws Exception {
        Query query = convert("x in (1, 3)");
        assertThat(query, instanceOf(FilteredQuery.class));
        assertThat(((FilteredQuery)query).getFilter(), instanceOf(TermsFilter.class));
    }

    @Test
    public void testWhereStringRefInSetLiteralIsConvertedToBooleanQuery() throws Exception {
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
        // TODO: FIX ME!
        //assertThat(((ConstantScoreQuery)query).getFilter(), instanceOf(RegexpFilter.class));
    }

    /**
     * When using PCRE features, switch to different
     * regex implementation on top of java.util.regex.
     */
    @Test
    public void testRegexQueryPcre() throws Exception {
        Query query = convert("name ~ '\\D'");
        assertThat(query, instanceOf(RegexQuery.class));
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
        assertThat(query, instanceOf(FilteredQuery.class));
        assertThat(((FilteredQuery)query).getFilter(), instanceOf(TermsFilter.class));
    }

    @Test
    public void testAnyEqArrayReference() throws Exception {
        Query query = convert("1.5 = any(d_array)");
        assertThat(query.toString(), is("d_array:[1.5 TO 1.5]"));
    }

    @Test
    public void testAnyGreaterAndSmaller() throws Exception {
        Query ltQuery = convert("1.5 < any(d_array)");
        assertThat(ltQuery.toString(), is("d_array:{1.5 TO *}"));

        // d < ANY ([1.2, 3.5])
        Query ltQuery2 = convert("d < any ([1.2, 3.5])");
        assertThat(ltQuery2.toString(), is("(d:{* TO 1.2} d:{* TO 3.5})~1"));

        // 1.5d <= ANY (d_array)
        Query lteQuery = convert("1.5 <= any(d_array)");
        assertThat(lteQuery.toString(), is("d_array:[1.5 TO *}"));

        // d <= ANY ([1.2, 3.5])
        Query lteQuery2 = convert("d <= any([1.2, 3.5])");
        assertThat(lteQuery2.toString(), is("(d:{* TO 1.2] d:{* TO 3.5])~1"));

        // 1.5d > ANY (d_array)
        Query gtQuery = convert("1.5 > any(d_array)");
        assertThat(gtQuery.toString(), is("d_array:{* TO 1.5}"));

        // d > ANY ([1.2, 3.5])
        Query gtQuery2 = convert("d > any ([1.2, 3.5])");
        assertThat(gtQuery2.toString(), is("(d:{1.2 TO *} d:{3.5 TO *})~1"));

        // 1.5d >= ANY (d_array)
        Query gteQuery = convert("1.5 >= any(d_array)");
        assertThat(gteQuery.toString(), is("d_array:{* TO 1.5]"));

        // d >= ANY ([1.2, 3.5])
        Query gteQuery2 = convert("d >= any ([1.2, 3.5])");
        assertThat(gteQuery2.toString(), is("(d:[1.2 TO *} d:[3.5 TO *})~1"));
    }

    @Test
    public void testAnyOnArrayLiteral() throws Exception {
        Query neqQuery = convert("name != any (['a', 'b', 'c'])");
        assertThat(neqQuery, instanceOf(FilteredQuery.class));
        assertThat(((FilteredQuery)neqQuery).getFilter(), instanceOf(BooleanFilter.class));
        BooleanFilter filter = (BooleanFilter)((FilteredQuery) neqQuery).getFilter();
        assertThat(filter.toString(), is("BooleanFilter(-BooleanFilter(+name:a +name:b +name:c))"));

        Query likeQuery = convert("name like any (['a', 'b', 'c'])");
        assertThat(likeQuery, instanceOf(BooleanQuery.class));
        BooleanQuery likeBQuery = (BooleanQuery)likeQuery;
        assertThat(likeBQuery.clauses().size(), is(3));
        for (int i = 0; i < 2; i++) {
            // like --> ConstantScoreQuery with regexp-filter
            Query filteredQuery = likeBQuery.clauses().get(i).getQuery();
            assertThat(filteredQuery, instanceOf(WildcardQuery.class));
        }

        Query notLikeQuery = convert("name not like any (['a', 'b', 'c'])");
        assertThat(notLikeQuery, instanceOf(BooleanQuery.class));
        BooleanQuery notLikeBQuery = (BooleanQuery)notLikeQuery;
        assertThat(notLikeBQuery.clauses(), hasSize(1));
        BooleanClause clause = notLikeBQuery.clauses().get(0);
        assertThat(clause.getOccur(), is(BooleanClause.Occur.MUST_NOT));
        assertThat(((BooleanQuery)clause.getQuery()).clauses(), hasSize(3));
        for (BooleanClause innerClause : ((BooleanQuery)clause.getQuery()).clauses()) {
            assertThat(innerClause.getOccur(), is(BooleanClause.Occur.MUST));
            assertThat(innerClause.getQuery(), instanceOf(WildcardQuery.class));
        }


        Query ltQuery2 = convert("name < any (['a', 'b', 'c'])");
        assertThat(ltQuery2, instanceOf(BooleanQuery.class));
        BooleanQuery ltBQuery = (BooleanQuery)ltQuery2;
        assertThat(ltBQuery.toString(), is("(name:{* TO a} name:{* TO b} name:{* TO c})~1"));
    }

    @Test
    public void testSqlLikeToLuceneWildcard() throws Exception {
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
        assertThat(query, instanceOf(ConstantScoreQuery.class));
        //TODO: FIX ME!
        // assertThat(((ConstantScoreQuery) query).getFilter(), instanceOf(IntersectsPrefixTreeFilter.class));
    }

    @Test
    public void testGeoShapeMatchDisJoint() throws Exception {
        Query query = convert("match(shape, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') using disjoint");
        assertThat(query, instanceOf(ConstantScoreQuery.class));
        //TODO: FIX ME!
        // assertThat(((ConstantScoreQuery) query).getFilter(), instanceOf(DisjointSpatialFilter.class));
    }

    @Test
    public void testGeoShapeMatchWithin() throws Exception {
        Query query = convert("match(shape, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') using within");
        assertThat(query, instanceOf(ConstantScoreQuery.class));
        //TODO: FIX ME!
        // assertThat(((ConstantScoreQuery) query).getFilter(), instanceOf(WithinPrefixTreeFilter.class));
    }

    @Test
    public void testWithinFunction() throws Exception {
        Query eqWithinQuery = convert("within(point, {type='LineString', coordinates=[[0.0, 0.0], [1.0, 1.0]]})");
        assertThat(eqWithinQuery.toString(), is("filtered(ConstantScore(*:*))->GeoPolygonFilter(point, [[0.0, 0.0], [1.0, 1.0]])"));
    }

    @Test
    public void testWithinFunctionWithShapeReference() throws Exception {
        // shape references cannot use the inverted index, so use generic function here
        Query eqWithinQuery = convert("within(point, shape)");
        assertThat(eqWithinQuery, instanceOf(FilteredQuery.class));
        assertThat(((FilteredQuery)eqWithinQuery).getFilter(), instanceOf(LuceneQueryBuilder.Visitor.FunctionFilter.class));
    }
}
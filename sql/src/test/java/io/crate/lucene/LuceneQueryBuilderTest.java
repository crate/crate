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

import com.google.common.collect.Sets;
import io.crate.analyze.WhereClause;
import io.crate.metadata.Functions;
import io.crate.operation.operator.*;
import io.crate.operation.operator.any.*;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.sandbox.queries.regex.RegexQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.search.internal.SearchContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class LuceneQueryBuilderTest extends CrateUnitTest {

    private LuceneQueryBuilder builder;
    private SearchContext searchContext;
    private IndexCache indexCache;

    @Before
    public void prepare() throws Exception {
        Functions functions = new ModulesBuilder()
                .add(new OperatorModule()).createInjector().getInstance(Functions.class);
        builder = new LuceneQueryBuilder(functions);
        searchContext = mock(SearchContext.class, Answers.RETURNS_MOCKS.get());
        indexCache = mock(IndexCache.class, Answers.RETURNS_MOCKS.get());
    }

    @Test
    public void testNoMatchWhereClause() throws Exception {
        Query query = convert(WhereClause.NO_MATCH);
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
    }

    @Test
    public void testWhereRefEqNullWithDifferentTypes() throws Exception {
        for (DataType type : DataTypes.PRIMITIVE_TYPES) {
            Reference foo = createReference("foo", type);
            Query query = convert(whereClause(EqOperator.NAME, foo, Literal.newLiteral(type, null)));

            // must always become a MatchNoDocsQuery
            // string: term query with null would cause NPE
            // int/numeric: rangeQuery from null to null would match all
            // bool:  term would match false too because of the condition in the eq query builder
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        }
    }

    @Test
    public void testWhereRefEqRef() throws Exception {
        Reference foo = createReference("foo", DataTypes.STRING);
        Query query = convert(whereClause(EqOperator.NAME, foo, foo));
        assertThat(query, instanceOf(FilteredQuery.class));
    }

    @Test
    public void testLteQuery() throws Exception {
        Query query = convert(new WhereClause(createFunction(LteOperator.NAME,
                DataTypes.BOOLEAN,
                createReference("x", DataTypes.INTEGER),
                Literal.newLiteral(10))));
        assertThat(query, instanceOf(NumericRangeQuery.class));
        assertThat(query.toString(), is("x:{* TO 10]"));
    }

    @Test
    public void testEqOnTwoArraysBecomesGenericFunctionQuery() throws Exception {
        DataType longArray = new ArrayType(DataTypes.LONG);
        Query query = convert(new WhereClause(createFunction(EqOperator.NAME,
                DataTypes.BOOLEAN,
                createReference("x", longArray),
                Literal.newLiteral(longArray, new Object[] { 10L, null, 20L }))));
        assertThat(query, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) query;

        assertThat(filteredQuery.getFilter(), instanceOf(BooleanFilter.class));
        assertThat(filteredQuery.getQuery(), instanceOf(XConstantScoreQuery.class));

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
        Query query = convert(new WhereClause(createFunction(GteOperator.NAME,
                DataTypes.BOOLEAN,
                createReference("x", DataTypes.INTEGER),
                Literal.newLiteral(10))));
        assertThat(query, instanceOf(NumericRangeQuery.class));
        assertThat(query.toString(), is("x:[10 TO *}"));
    }

    @Test
    public void testWhereRefInSetLiteralIsConvertedToBooleanQuery() throws Exception {
        DataType dataType = new SetType(DataTypes.INTEGER);
        Reference foo = createReference("foo", DataTypes.INTEGER);
        WhereClause whereClause = new WhereClause(
                createFunction(InOperator.NAME, DataTypes.BOOLEAN,
                        foo,
                        Literal.newLiteral(dataType, Sets.newHashSet(1, 3))));
        Query query = convert(whereClause);
        assertThat(query, instanceOf(FilteredQuery.class));
        assertThat(((FilteredQuery)query).getFilter(), instanceOf(TermsFilter.class));
    }

    @Test
    public void testWhereStringRefInSetLiteralIsConvertedToBooleanQuery() throws Exception {
        DataType dataType = new SetType(DataTypes.STRING);
        Reference foo = createReference("foo", DataTypes.STRING);
        WhereClause whereClause = new WhereClause(
                createFunction(InOperator.NAME, DataTypes.BOOLEAN,
                        foo,
                        Literal.newLiteral(dataType, Sets.newHashSet(new BytesRef("foo"), new BytesRef("bar")))
                ));
        Query query = convert(whereClause);
        assertThat(query, instanceOf(FilteredQuery.class));
        assertThat(((FilteredQuery)query).getFilter(), instanceOf(TermsFilter.class));
    }

    /**
     * Make sure we still sport the fast Lucene regular
     * expression engine when not using PCRE features.
     */
    @Test
    public void testRegexQueryFast() throws Exception {
        Reference value = createReference("foo", DataTypes.STRING);
        Literal pattern = Literal.newLiteral(new BytesRef("[a-z]"));
        Query query = convert(whereClause(RegexpMatchOperator.NAME, value, pattern));
        assertThat(query, instanceOf(RegexpQuery.class));
    }

    /**
     * When using PCRE features, switch to different
     * regex implementation on top of java.util.regex.
     */
    @Test
    public void testRegexQueryPcre() throws Exception {
        Reference value = createReference("foo", DataTypes.STRING);
        Literal pattern = Literal.newLiteral(new BytesRef("\\D"));
        Query query = convert(whereClause(RegexpMatchOperator.NAME, value, pattern));
        assertThat(query, instanceOf(RegexQuery.class));
    }

    @Test
    public void testAnyEqArrayLiteral() throws Exception {
        Reference ref = createReference("d", DataTypes.DOUBLE);
        Literal doubleArrayLiteral = Literal.newLiteral(new Object[]{-1.5d, 0.0d, 1.5d}, new ArrayType(DataTypes.DOUBLE));
        Query query = convert(whereClause(AnyEqOperator.NAME, ref, doubleArrayLiteral));
        assertThat(query, instanceOf(FilteredQuery.class));
        assertThat(((FilteredQuery)query).getFilter(), instanceOf(TermsFilter.class));
    }

    @Test
    public void testAnyEqArrayReference() throws Exception {
        Reference ref = createReference("d_array", new ArrayType(DataTypes.DOUBLE));
        Literal doubleLiteral = Literal.newLiteral(1.5d);
        Query query = convert(whereClause(AnyEqOperator.NAME, doubleLiteral, ref));
        assertThat(query.toString(), is("d_array:[1.5 TO 1.5]"));
    }

    @Test
    public void testAnyGreaterAndSmaller() throws Exception {

        Reference arrayRef = createReference("d_array", new ArrayType(DataTypes.DOUBLE));
        Literal doubleLiteral = Literal.newLiteral(1.5d);

        Reference ref = createReference("d", DataTypes.DOUBLE);
        Literal arrayLiteral = Literal.newLiteral(new Object[]{1.2d, 3.5d}, new ArrayType(DataTypes.DOUBLE));

        // 1.5d < ANY (d_array)
        Query ltQuery = convert(whereClause(AnyLtOperator.NAME, doubleLiteral, arrayRef));
        assertThat(ltQuery.toString(), is("d_array:{1.5 TO *}"));

        // d < ANY ([1.2, 3.5])
        Query ltQuery2 = convert(whereClause(AnyLtOperator.NAME, ref, arrayLiteral));
        assertThat(ltQuery2.toString(), is("(d:{* TO 1.2} d:{* TO 3.5})~1"));

        // 1.5d <= ANY (d_array)
        Query lteQuery = convert(whereClause(AnyLteOperator.NAME, doubleLiteral, arrayRef));
        assertThat(lteQuery.toString(), is("d_array:[1.5 TO *}"));

        // d <= ANY ([1.2, 3.5])
        Query lteQuery2 = convert(whereClause(AnyLteOperator.NAME, ref, arrayLiteral));
        assertThat(lteQuery2.toString(), is("(d:{* TO 1.2] d:{* TO 3.5])~1"));

        // 1.5d > ANY (d_array)
        Query gtQuery = convert(whereClause(AnyGtOperator.NAME, doubleLiteral, arrayRef));
        assertThat(gtQuery.toString(), is("d_array:{* TO 1.5}"));

        // d > ANY ([1.2, 3.5])
        Query gtQuery2 = convert(whereClause(AnyGtOperator.NAME, ref, arrayLiteral));
        assertThat(gtQuery2.toString(), is("(d:{1.2 TO *} d:{3.5 TO *})~1"));

        // 1.5d >= ANY (d_array)
        Query gteQuery = convert(whereClause(AnyGteOperator.NAME, doubleLiteral, arrayRef));
        assertThat(gteQuery.toString(), is("d_array:{* TO 1.5]"));

        // d >= ANY ([1.2, 3.5])
        Query gteQuery2 = convert(whereClause(AnyGteOperator.NAME, ref, arrayLiteral));
        assertThat(gteQuery2.toString(), is("(d:[1.2 TO *} d:[3.5 TO *})~1"));
    }

    @Test
    public void testAnyOnArrayLiteral() throws Exception {
        Reference ref = createReference("d", DataTypes.STRING);
        Literal stringArrayLiteral = Literal.newLiteral(new Object[]{new BytesRef("a"), new BytesRef("b"), new BytesRef("c")}, new ArrayType(DataTypes.STRING));

        // col != ANY (1,2,3)
        Query neqQuery = convert(whereClause(AnyNeqOperator.NAME, ref, stringArrayLiteral));
        assertThat(neqQuery, instanceOf(FilteredQuery.class));
        assertThat(((FilteredQuery)neqQuery).getFilter(), instanceOf(BooleanFilter.class));
        BooleanFilter filter = (BooleanFilter)((FilteredQuery) neqQuery).getFilter();
        assertThat(filter.toString(), is("BooleanFilter(-BooleanFilter(+d:a +d:b +d:c))"));

        // col like any (1,2,3)
        Query likeQuery = convert(whereClause(AnyLikeOperator.NAME, ref, stringArrayLiteral));
        assertThat(likeQuery, instanceOf(BooleanQuery.class));
        BooleanQuery likeBQuery = (BooleanQuery)likeQuery;
        assertThat(likeBQuery.clauses().size(), is(3));
        for (int i = 0; i < 2; i++) {
            assertThat(likeBQuery.clauses().get(i).getQuery(), instanceOf(WildcardQuery.class));
        }

        // col not like any (1,2,3)
        Query notLikeQuery = convert(whereClause(AnyNotLikeOperator.NAME, ref, stringArrayLiteral));
        assertThat(notLikeQuery, instanceOf(BooleanQuery.class));
        BooleanQuery notLikeBQuery = (BooleanQuery)notLikeQuery;
        assertThat(notLikeBQuery.toString(), is("-(+d:a +d:b +d:c)"));


        // col < any (1,2,3)
        Query ltQuery2 = convert(whereClause(AnyLtOperator.NAME, ref, stringArrayLiteral));
        assertThat(ltQuery2, instanceOf(BooleanQuery.class));
        BooleanQuery ltBQuery = (BooleanQuery)ltQuery2;
        assertThat(ltBQuery.toString(), is("(d:{* TO a} d:{* TO b} d:{* TO c})~1"));
    }

    private Query convert(WhereClause clause) {
        return builder.convert(clause, searchContext, indexCache).query;
    }
}
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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.google.common.collect.Sets;
import io.crate.analyze.WhereClause;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.operation.operator.*;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.apache.lucene.queries.BooleanFilter;
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

import static io.crate.testing.TestingHelpers.createFunction;
import static io.crate.testing.TestingHelpers.createReference;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class LuceneQueryBuilderTest extends RandomizedTest {

    private LuceneQueryBuilder builder;

    @Before
    public void setUp() throws Exception {
        Functions functions = new ModulesBuilder()
                .add(new OperatorModule()).createInjector().getInstance(Functions.class);
        builder = new LuceneQueryBuilder(functions,
                mock(SearchContext.class, Answers.RETURNS_MOCKS.get()),
                mock(IndexCache.class, Answers.RETURNS_MOCKS.get()));
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
        DataType dataType = new SetType(DataTypes.STRING);
        Reference foo = createReference("foo", DataTypes.STRING);
        WhereClause whereClause = new WhereClause(
                createFunction(InOperator.NAME, DataTypes.BOOLEAN,
                        foo,
                        Literal.newLiteral(dataType, Sets.newHashSet(new BytesRef("foo"), new BytesRef("bar")))
                ));
        Query query = convert(whereClause);
        assertThat(query, instanceOf(BooleanQuery.class));
        for (BooleanClause booleanClause : (BooleanQuery) query) {
            assertThat(booleanClause.getQuery(), instanceOf(TermQuery.class));
        }
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


    private Query convert(WhereClause clause) {
        return builder.convert(clause).query;
    }

    private WhereClause whereClause(String opname, Symbol left, Symbol right) {
        return new WhereClause(new Function(new FunctionInfo(
                new FunctionIdent(opname, Arrays.asList(left.valueType(), right.valueType())), DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(left, right)
        ));
    }

}
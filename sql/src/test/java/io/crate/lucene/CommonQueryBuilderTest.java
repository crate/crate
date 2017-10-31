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
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.sql.tree.QualifiedName;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.spatial.prefix.IntersectsPrefixTreeQuery;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class CommonQueryBuilderTest extends LuceneQueryBuilderTest {

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
            SqlExpressions sqlExpressions = new SqlExpressions( tableSources, tableRelation, new Object[]{null}, SessionContext.SYSTEM_SESSION);
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
        assertThat(query, instanceOf(TermInSetQuery.class));
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
    public void testLikeWithBothSidesReferences() throws Exception {
        Query query = convert("name like name");
        assertThat(query, instanceOf(GenericFunctionQuery.class));
    }

    @Test
    public void testWhereInIsOptimized() throws Exception {
        Query query = convert("name in ('foo', 'bar')");
        assertThat(query, instanceOf(TermInSetQuery.class));
        assertThat(query.toString(), is("name:bar name:foo"));
    }

    @Test
    public void testIsNullOnObjectArray() throws Exception {
        Query query = convert("o_array IS NULL");
        assertThat(query.toString(), is("+*:* -ConstantScore(_field_names:o_array)"));
        query = convert("o_array IS NOT NULL");
        assertThat(query, instanceOf(GenericFunctionQuery.class));
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

    @Test
    public void testRangeQueryForId() throws Exception {
        Query query = convert("_id > 'foo'");
        assertThat(query, instanceOf(TermRangeQuery.class));
        assertThat(query.toString(), is("_uid:{default#foo TO *}"));
    }

    @Test
    public void testRangeQueryForUid() throws Exception {
        Query query = convert("_uid > 'foo'");
        assertThat(query, instanceOf(TermRangeQuery.class));
    }

    @Test
    public void testRangeQueryOnDocThrowsException() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("unknown function: op_>(object, object)");
        convert("_doc > {\"name\"='foo'}");
    }

    @Test
    public void testIsNullOnGeoPoint() throws Exception {
        Query query = convert("point is null");
        assertThat(query.toString(), is("+*:* -ConstantScore(_field_names:point)"));
    }
}

/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.user.User;
import io.crate.lucene.match.CrateRegexQuery;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.spatial.prefix.IntersectsPrefixTreeQuery;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class CommonQueryBuilderTest extends LuceneQueryBuilderTest {

    @Test
    public void testNoMatchWhereClause() throws Exception {
        Query query = convert(WhereClause.NO_MATCH.queryOrFallback());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
    }

    @Test
    public void testWhereRefEqNullWithDifferentTypes() throws Exception {
        for (DataType<?> type : DataTypes.PRIMITIVE_TYPES) {
            if (type.storageSupport() == null) {
                continue;
            }
            // ensure the test is operating on a fresh, empty cluster state (no existing tables)
            resetClusterService();

            DocTableInfo tableInfo = SQLExecutor.tableInfo(
                new RelationName(DocSchemaInfo.NAME, "test_primitive"),
                "create table doc.test_primitive (" +
                "  x " + type.getName() +
                ")",
                clusterService);

            TableRelation tableRelation = new TableRelation(tableInfo);
            Map<RelationName, AnalyzedRelation> tableSources = Map.of(tableInfo.ident(), tableRelation);
            SqlExpressions sqlExpressions = new SqlExpressions(tableSources, tableRelation, User.CRATE_USER);

            Query query = convert(sqlExpressions.normalize(sqlExpressions.asSymbol("x = null")));

            // must always become a MatchNoDocsQuery
            // string: term query with null would cause NPE
            // int/numeric: rangeQuery from null to null would match all
            // bool:  term would match false too because of the condition in the eq query builder
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        }
    }

    @Test
    public void testWhereRefEqRef() throws Exception {
        // 3vl
        Query query = convert("name = name");
        assertThat(query, instanceOf(ConstantScoreQuery.class));
        ConstantScoreQuery scoreQuery = (ConstantScoreQuery) query;
        assertThat(scoreQuery.getQuery(), instanceOf(DocValuesFieldExistsQuery.class));
        // 2vl
        query = convert("ignore3vl(name = name)");
        assertThat(query, instanceOf(MatchAllDocsQuery.class));
    }

    @Test
    public void testWhereRefEqLiteral() throws Exception {
        Query query = convert("10 = x");
        assertThat(query.toString(), is("x:[10 TO 10]"));
    }

    @Test
    public void testWhereLiteralEqReference() throws Exception {
        Query query = convert("x = 10");
        assertThat(query.toString(), is("x:[10 TO 10]"));
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
        Query query = convert("y_array = [null, null, null]");
        assertThat(query, instanceOf(GenericFunctionQuery.class));
    }

    @Test
    public void testEqOnArrayWithTooManyClauses() throws Exception {
        Object[] values = new Object[2000]; // should trigger the TooManyClauses exception
        Arrays.fill(values, 10L);
        Query query = convert("y_array = ?", new Object[] { values });
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
    public void testGtQuery() throws Exception {
        Query query = convert("x > 10");
        assertThat(query.toString(), is("x:[11 TO 2147483647]"));
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
        assertThat(query.toString(), is("_id:[ff 69 31]"));

        query = convert("_id = 1");
        assertThat(query, instanceOf(TermQuery.class));
        assertThat(query.toString(), is("_id:[fe 1f]"));
    }

    @Test
    public void testAnyEqArrayLiteral() throws Exception {
        Query query = convert("d = any([-1.5, 0.0, 1.5])");
        assertThat(query, instanceOf(PointInSetQuery.class));

        query = convert("_id in ('test','test2')");
        assertThat(query, instanceOf(TermInSetQuery.class));

        query = convert("_id in (1, 2)");
        assertThat(query, instanceOf(TermInSetQuery.class));

        query = convert("_id = any (['test','test2'])");
        assertThat(query, instanceOf(TermInSetQuery.class));

        query = convert("_id = any ([1, 2])");
        assertThat(query, instanceOf(TermInSetQuery.class));
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
    public void testLessThanAnyOnArrayLiteral() throws Exception {
        Query ltQuery2 = convert("name < any (['a', 'b', 'c'])");
        assertThat(ltQuery2, instanceOf(BooleanQuery.class));
        BooleanQuery ltBQuery = (BooleanQuery) ltQuery2;
        assertThat(ltBQuery.toString(), is("(name:{* TO a} name:{* TO b} name:{* TO c})~1"));
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
    public void testWhereInIsOptimized() throws Exception {
        Query query = convert("name in ('foo', 'bar')");
        assertThat(query, instanceOf(TermInSetQuery.class));
        assertThat(query.toString(), is("name:(bar foo)"));
    }

    @Test
    public void testIsNullOnObjectArray() throws Exception {
        Query isNull = convert("o_array IS NULL");
        assertThat(isNull.toString(), is("+*:* -ConstantScore(ConstantScore(DocValuesFieldExistsQuery [field=o_array.xs]))"));
        Query isNotNull = convert("o_array IS NOT NULL");
        assertThat(isNotNull.toString(), is("ConstantScore(ConstantScore(DocValuesFieldExistsQuery [field=o_array.xs]))"));
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
    }

    @Test
    public void testNiceErrorIsThrownOnInvalidTopLevelLiteral() {
        expectedException.expectMessage("Can't build query from symbol 'yes'");
        convert("'yes'");
    }

    @Test
    public void testRangeQueryForUid() throws Exception {
        Query query = convert("_uid > 'foo'");
        assertThat(query, instanceOf(TermRangeQuery.class));
        TermRangeQuery rangeQuery = (TermRangeQuery) query;
        assertThat(rangeQuery.getField(), is("_id"));
        assertThat(rangeQuery.getLowerTerm().utf8ToString(), is("foo"));
    }

    public void testRangeQueryOnDocThrowsException() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unknown function: (doc.users._doc > _map('name', 'foo'))," +
                                        " no overload found for matching argument types: (object, object).");
        convert("_doc > {\"name\"='foo'}");
    }

    @Test
    public void testIsNullOnGeoPoint() throws Exception {
        Query query = convert("point is null");
        assertThat(query.toString(), is("+*:* -ConstantScore(DocValuesFieldExistsQuery [field=point])"));
    }

    @Test
    public void testIpRange() throws Exception {
        Query query = convert("addr between '192.168.0.1' and '192.168.0.255'");
        assertThat(query.toString(), is("+addr:[192.168.0.1 TO ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff] +addr:[0:0:0:0:0:0:0:0 TO 192.168.0.255]"));

        query = convert("addr < 'fe80::1'");
        assertThat(query.toString(), is("addr:[0:0:0:0:0:0:0:0 TO fe80:0:0:0:0:0:0:0]"));
    }

    @Test
    public void test_ip_eq_uses_point_range_query() throws Exception {
        Query query = convert("addr = '192.168.0.1'");
        assertThat(query, instanceOf(PointRangeQuery.class));
    }

    @Test
    public void test_ip_eq_any_uses_point_term_set_query() throws Exception {
        Query query = convert("addr = ANY(['192.168.0.1', '192.168.0.2'])");
        assertThat(query.toString(), is("addr:{192.168.0.1 192.168.0.2}"));
    }

    @Test
    public void testAnyEqOnTimestampArrayColumn() {
        assertThat(
            convert("1129224512000 = ANY(ts_array)").toString(),
            is("ts_array:[1129224512000 TO 1129224512000]")
        );
    }

    @Test
    public void testAnyNotEqOnTimestampColumn() {
        assertThat(
            convert("ts != ANY([1129224512000])").toString(),
            is("+*:* -(+ts:[1129224512000 TO 1129224512000])")
        );
    }

    @Test
    public void testArrayAccessResultsInTermAndFunctionQuery() {
        assertThat(
            convert("ts_array[1] = 1129224512000").toString(),
            is("+ts_array:[1129224512000 TO 1129224512000] " +
                "#(ts_array[1] = 1129224512000::bigint)")
        );
        assertThat(
            convert("ts_array[1] >= 1129224512000").toString(),
            is("+ts_array:[1129224512000 TO 9223372036854775807] " +
                "#(ts_array[1] >= 1129224512000::bigint)")
        );
        assertThat(
            convert("ts_array[1] > 1129224512000").toString(),
            is("+ts_array:[1129224512001 TO 9223372036854775807] " +
                "#(ts_array[1] > 1129224512000::bigint)")
        );
        assertThat(
            convert("ts_array[1] <= 1129224512000").toString(),
            is("+ts_array:[-9223372036854775808 TO 1129224512000] " +
                "#(ts_array[1] <= 1129224512000::bigint)")
        );
        assertThat(
            convert("ts_array[1] < 1129224512000").toString(),
            is("+ts_array:[-9223372036854775808 TO 1129224511999] " +
                "#(ts_array[1] < 1129224512000::bigint)")
        );
    }

    @Test
    public void testObjectArrayAccessResultsInFunctionQuery() {
        assertThat(
            convert("o_array[1] = {x=1}").toString(),
            is("(o_array[1] = {\"x\"=1})")
        );
    }

    @Test
    public void testMatchWithOperator() {
        assertThat(
            convert("match(tags, 'foo bar') using best_fields with (operator='and')").toString(),
            is("+tags:foo +tags:bar")
        );
    }

    @Test
    public void testMultiMatchWithOperator() {
        assertThat(
            convert("match((tags, name), 'foo bar') using best_fields with (operator='and')").toString(),
            is("(name:foo bar | (+tags:foo +tags:bar))")
        );
    }

    @Test
    public void testEqOnObjectPreFiltersOnKnownObjectLiteralContents() {
        // termQuery for obj.x; nothing for obj.z because it's missing in the mapping
        assertThat(
            convert("obj = {x=10, z=20}").toString(),
            is("+obj.x:[10 TO 10] #(obj = {\"x\"=10, \"z\"=20})")
        );
    }

    @Test
    public void testEqOnObjectDoesBoolTermQueryForContents() {
        assertThat(
            convert("obj = {x=10, y=20}").toString(),
            is("+obj.x:[10 TO 10] +obj.y:[20 TO 20]")
        );
    }

    @Test
    public void testEqAnyOnNestedArray() {
        assertThat(
            convert("[1, 2] = any(o_array['xs'])").toString(),
            is("+o_array.xs:{1 2} #([1, 2] = ANY(o_array['xs']))")
        );
    }

    @Test
    public void testGtAnyOnNestedArrayIsNotSupported() {
        expectedException.expectMessage("Cannot use `> ANY` if left side is an array");
        convert("[1, 2] > any(o_array['xs'])");
    }

    @Test
    public void testGteAnyOnNestedArrayIsNotSupported() {
        expectedException.expectMessage("Cannot use `>= ANY` if left side is an array");
        convert("[1, 2] >= any(o_array['xs'])");
    }

    @Test
    public void testLtAnyOnNestedArrayIsNotSupported() {
        expectedException.expectMessage("Cannot use `< ANY` if left side is an array");
        convert("[1, 2] < any(o_array['xs'])");
    }

    @Test
    public void testLteAnyOnNestedArrayIsNotSupported() {
        expectedException.expectMessage("Cannot use `<= ANY` if left side is an array");
        convert("[1, 2] <= any(o_array['xs'])");
    }

    @Test
    public void testAnyOnObjectArrayResultsInXY() {
        Query query = convert("{xs=[1, 1]} = ANY(o_array)");
        assertThat(query, instanceOf(GenericFunctionQuery.class));
    }

    @Test
    public void test_is_null_on_ignored_results_in_function_query() throws Exception {
        Query query = convert("obj_ignored is null");
        assertThat(query.toString(), is("(_doc['obj_ignored'] IS NULL)"));
    }

    @Test
    public void test_is_not_null_on_ignored_results_in_function_query() throws Exception {
        Query query = convert("obj_ignored is not null");
        assertThat(query.toString(), is("(NOT (_doc['obj_ignored'] IS NULL))"));
    }

    @Test
    public void test_equal_on_varchar_column_uses_term_query() throws Exception {
        Query query = convert("vchar_name = 'Trillian'");
        assertThat(query.toString(), is("vchar_name:Trillian"));
        assertThat(query, Matchers.instanceOf(TermQuery.class));
    }

    @Test
    public void test_eq_on_byte_column() throws Exception {
        Query query = convert("byte_col = 127");
        assertThat(query.toString(), is("byte_col:[127 TO 127]"));
    }

    @Test
    public void test_eq_on_float_column_uses_float_point_query() throws Exception {
        Query query = convert("f = 42.0::float");
        assertThat(query.toString(), is("f:[42.0 TO 42.0]"));
    }

    @Test
    public void test_eq_any_on_float_column_uses_set_query() throws Exception {
        Query query = convert("f = ANY([42.0, 41.0])");
        assertThat(query.toString(), is("f:{41.0 42.0}"));
    }

    @Test
    public void test_eq_on_bool_uses_termquery() throws Exception {
        Query query = convert("bool_col = true");
        assertThat(query, instanceOf(TermQuery.class));
    }

    @Test
    public void test_is_null_on_analyzed_text_column_uses_norms_query() throws Exception {
        Query query = convert("content is null");
        assertThat(query.toString(), is("+*:* -NormsFieldExistsQuery [field=content]"));
    }
}

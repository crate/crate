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

import static io.crate.testing.TestingHelpers.createReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldExistsQuery;
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
import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.lucene.match.CrateRegexQuery;
import io.crate.metadata.RelationName;
import io.crate.metadata.Scalar;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.Signature;
import io.crate.testing.QueryTester;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;
import io.crate.role.Role;

public class CommonQueryBuilderTest extends LuceneQueryBuilderTest {

    @Test
    public void testNoMatchWhereClause() throws Exception {
        Query query = convert(WhereClause.NO_MATCH.queryOrFallback());
        assertThat(query).isExactlyInstanceOf(MatchNoDocsQuery.class);
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
            SqlExpressions sqlExpressions = new SqlExpressions(tableSources, tableRelation, Role.CRATE_USER);

            Query query = convert(sqlExpressions.normalize(sqlExpressions.asSymbol("x = null")));

            // must always become a MatchNoDocsQuery
            // string: term query with null would cause NPE
            // int/numeric: rangeQuery from null to null would match all
            // bool:  term would match false too because of the condition in the eq query builder
            assertThat(query).isExactlyInstanceOf(MatchNoDocsQuery.class);
        }
    }

    @Test
    public void testWhereRefEqRef() throws Exception {
        // 3vl
        Query query = convert("name = name");
        assertThat(query).isExactlyInstanceOf(FieldExistsQuery.class);
        // 2vl
        query = convert("ignore3vl(name = name)");
        assertThat(query).isExactlyInstanceOf(MatchAllDocsQuery.class);
    }

    @Test
    public void testWhereRefEqLiteral() throws Exception {
        Query query = convert("10 = x");
        assertThat(query).hasToString("x:[10 TO 10]");
    }

    @Test
    public void testWhereLiteralEqReference() throws Exception {
        Query query = convert("x = 10");
        assertThat(query).hasToString("x:[10 TO 10]");
    }

    @Test
    public void testLteQuery() throws Exception {
        Query query = convert("x <= 10");
        assertThat(query).hasToString("x:[-2147483648 TO 10]");
    }

    @Test
    public void testNotEqOnNotNullableColumnQuery() throws Exception {
        Query query = convert("x != 10");
        assertThat(query)
            .isExactlyInstanceOf(BooleanQuery.class)
            .hasToString("+(+*:* -x:[10 TO 10])");

        query = convert("not x = 10");
        assertThat(query)
            .isExactlyInstanceOf(BooleanQuery.class)
            .hasToString("+(+*:* -x:[10 TO 10])");
    }

    @Test
    public void testEqOnTwoArraysBecomesGenericFunctionQuery() throws Exception {
        Query query = convert("y_array = [10, 20, 30]");
        assertThat(query).isExactlyInstanceOf(BooleanQuery.class);
        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.clauses().get(0).getQuery()).isInstanceOf(PointInSetQuery.class);
        assertThat(booleanQuery.clauses().get(1).getQuery()).isExactlyInstanceOf(GenericFunctionQuery.class);
    }

    @Test
    public void testEqOnTwoArraysBecomesGenericFunctionQueryAllValuesNull() throws Exception {
        Query query = convert("y_array = [null, null, null]");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
    }

    @Test
    public void testEqOnArrayWithTooManyClauses() throws Exception {
        Object[] values = new Object[2000]; // should trigger the TooManyClauses exception
        Arrays.fill(values, 10L);
        Query query = convert("y_array = ?", new Object[] { values });
        assertThat(query).isExactlyInstanceOf(BooleanQuery.class);
        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.clauses().get(0).getQuery()).isInstanceOf(PointInSetQuery.class);
        assertThat(booleanQuery.clauses().get(1).getQuery()).isExactlyInstanceOf(GenericFunctionQuery.class);
    }

    @Test
    public void testGteQuery() throws Exception {
        Query query = convert("x >= 10");
        assertThat(query).hasToString("x:[10 TO 2147483647]");
    }

    @Test
    public void testGtQuery() throws Exception {
        Query query = convert("x > 10");
        assertThat(query).hasToString("x:[11 TO 2147483647]");
    }

    @Test
    public void testWhereRefInSetLiteralIsConvertedToTermsQuery() throws Exception {
        Query query = convert("x in (1, 3)");
        assertThat(query).isInstanceOf(PointInSetQuery.class);
    }

    @Test
    public void testWhereStringRefInSetLiteralIsConvertedToTermsQuery() throws Exception {
        Query query = convert("name in ('foo', 'bar')");
        assertThat(query).isExactlyInstanceOf(TermInSetQuery.class);
    }

    /**
     * Make sure we still sport the fast Lucene regular
     * expression engine when not using PCRE features.
     */
    @Test
    public void testRegexQueryFast() throws Exception {
        Query query = convert("name ~ '[a-z]'");
        assertThat(query).isExactlyInstanceOf(ConstantScoreQuery.class);
        ConstantScoreQuery scoreQuery = (ConstantScoreQuery) query;
        assertThat(scoreQuery.getQuery()).isExactlyInstanceOf(RegexpQuery.class);
    }

    /**
     * When using PCRE features, switch to different
     * regex implementation on top of java.util.regex.
     */
    @Test
    public void testRegexQueryPcre() throws Exception {
        Query query = convert("name ~ '\\D'");
        assertThat(query).isExactlyInstanceOf(CrateRegexQuery.class);
    }

    @Test
    public void testIdQuery() throws Exception {
        Query query = convert("_id = 'i1'");
        assertThat(query)
            .isExactlyInstanceOf(TermQuery.class)
            .hasToString("_id:[ff 69 31]");

        query = convert("_id = 1");
        assertThat(query)
            .isExactlyInstanceOf(TermQuery.class)
            .hasToString("_id:[fe 1f]");
    }

    @Test
    public void testAnyEqArrayLiteral() throws Exception {
        Query query = convert("d = any([-1.5, 0.0, 1.5])");
        assertThat(query).isInstanceOf(PointInSetQuery.class);

        query = convert("_id in ('test','test2')");
        assertThat(query).isInstanceOf(TermInSetQuery.class);

        query = convert("_id in (1, 2)");
        assertThat(query).isInstanceOf(TermInSetQuery.class);

        query = convert("_id = any (['test','test2'])");
        assertThat(query).isInstanceOf(TermInSetQuery.class);

        query = convert("_id = any ([1, 2])");
        assertThat(query).isInstanceOf(TermInSetQuery.class);
    }

    @Test
    public void testAnyEqArrayReference() throws Exception {
        Query query = convert("1.5 = any(d_array)");
        assertThat(query).isInstanceOf(PointRangeQuery.class);
        assertThat(query.toString()).startsWith("d_array");
    }

    @Test
    public void testAnyGreaterAndSmaller() throws Exception {
        Query ltQuery = convert("1.5 < any(d_array)");
        assertThat(ltQuery).hasToString("d_array:[1.5000000000000002 TO Infinity]");

        // d < ANY ([1.2, 3.5])
        Query ltQuery2 = convert("d < any ([1.2, 3.5])");
        assertThat(ltQuery2).hasToString("(d:[-Infinity TO 1.1999999999999997] d:[-Infinity TO 3.4999999999999996])~1");

        // 1.5d <= ANY (d_array)
        Query lteQuery = convert("1.5 <= any(d_array)");
        assertThat(lteQuery).hasToString("d_array:[1.5 TO Infinity]");

        // d <= ANY ([1.2, 3.5])
        Query lteQuery2 = convert("d <= any([1.2, 3.5])");
        assertThat(lteQuery2).hasToString("(d:[-Infinity TO 1.2] d:[-Infinity TO 3.5])~1");

        // 1.5d > ANY (d_array)
        Query gtQuery = convert("1.5 > any(d_array)");
        assertThat(gtQuery).hasToString("d_array:[-Infinity TO 1.4999999999999998]");

        // d > ANY ([1.2, 3.5])
        Query gtQuery2 = convert("d > any ([1.2, 3.5])");
        assertThat(gtQuery2).hasToString(
            "(d:[1.2000000000000002 TO Infinity] d:[3.5000000000000004 TO Infinity])~1");

        // 1.5d >= ANY (d_array)
        Query gteQuery = convert("1.5 >= any(d_array)");
        assertThat(gteQuery).hasToString("d_array:[-Infinity TO 1.5]");

        // d >= ANY ([1.2, 3.5])
        Query gteQuery2 = convert("d >= any ([1.2, 3.5])");
        assertThat(gteQuery2).hasToString("(d:[1.2 TO Infinity] d:[3.5 TO Infinity])~1");
    }

    @Test
    public void testNeqAnyOnArrayLiteral() throws Exception {
        Query query = convert("name != any (['a', 'b', 'c'])");
        assertThat(query).hasToString(
            "+(+*:* -(+name:a +name:b +name:c)) #FieldExistsQuery [field=name]"
        );
    }

    @Test
    public void testLessThanAnyOnArrayLiteral() throws Exception {
        Query ltQuery2 = convert("name < any (['a', 'b', 'c'])");
        assertThat(ltQuery2).isExactlyInstanceOf(BooleanQuery.class);
        BooleanQuery ltBQuery = (BooleanQuery) ltQuery2;
        assertThat(ltBQuery).hasToString("(name:{* TO a} name:{* TO b} name:{* TO c})~1");
    }


    /**
     * geo match tests below... error cases (wrong matchType, etc.) are not tests here because validation is done in the
     * analyzer
     */

    @Test
    public void test_prefix_tree_backed_geo_shape_match_with_default_match_type() throws Exception {
        Query query = convert("match(shape, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')");
        assertThat(query).isExactlyInstanceOf(IntersectsPrefixTreeQuery.class);
    }

    @Test
    public void test_bkd_tree_backed_geo_shape_match_with_default_match_type() {
        Query query = convert("match(bkd_shape, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')");
        assertThat(query).isExactlyInstanceOf(ConstantScoreQuery.class);
        Query bkdQuery = ((ConstantScoreQuery) query).getQuery();
        assertThat(bkdQuery).extracting("queryRelation").isEqualTo(ShapeField.QueryRelation.INTERSECTS);
    }

    @Test
    public void test_prefix_tree_backed_geo_shape_match_disjoint() throws Exception {
        Query query = convert("match(shape, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') using disjoint");
        assertThat(query).isExactlyInstanceOf(ConstantScoreQuery.class);
        Query booleanQuery = ((ConstantScoreQuery) query).getQuery();
        assertThat(booleanQuery).isExactlyInstanceOf(BooleanQuery.class);

        BooleanClause existsClause = ((BooleanQuery) booleanQuery).clauses().get(0);
        BooleanClause intersectsClause = ((BooleanQuery) booleanQuery).clauses().get(1);

        assertThat(existsClause.getQuery()).isExactlyInstanceOf(TermRangeQuery.class);
        assertThat(intersectsClause.getQuery()).isExactlyInstanceOf(IntersectsPrefixTreeQuery.class);
    }

    @Test
    public void test_bkd_tree_backed_geo_shape_match_disjoint() {
        Query query = convert("match(bkd_shape, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') using disjoint");
        assertThat(query).isExactlyInstanceOf(ConstantScoreQuery.class);
        Query bkdQuery = ((ConstantScoreQuery) query).getQuery();
        assertThat(bkdQuery).extracting("queryRelation").isEqualTo(ShapeField.QueryRelation.DISJOINT);
    }

    @Test
    public void testWhereInIsOptimized() throws Exception {
        Query query = convert("name in ('foo', 'bar')");
        assertThat(query)
            .isExactlyInstanceOf(TermInSetQuery.class)
            .hasToString("name:(bar foo)");
    }

    @Test
    public void testIsNullOnObjectArray() throws Exception {
        Query isNull = convert("o_array IS NULL");
        assertThat(isNull).hasToString(
            "(o_array IS NULL)");
        Query isNotNull = convert("o_array IS NOT NULL");
        assertThat(isNotNull).hasToString(
            "(NOT (o_array IS NULL))");
    }

    @Test
    public void testRewriteDocReferenceInWhereClause() throws Exception {
        Query query = convert("_doc['name'] = 'foo'");
        assertThat(query)
            .isExactlyInstanceOf(TermQuery.class)
            .hasToString("name:foo");
        query = convert("_doc = {\"name\"='foo'}");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
    }

    @Test
    public void testMatchQueryTermMustNotBeNull() throws Exception {
        assertThatThrownBy(() -> convert("match(name, null)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("cannot use NULL as query term in match predicate");
    }

    @Test
    public void testMatchQueryTermMustBeALiteral() throws Exception {
        assertThatThrownBy(() -> convert("match(name, name)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("queryTerm must be a literal");
    }

    @Test
    public void testRangeQueryForId() throws Exception {
        Query query = convert("_id > 'foo'");
        assertThat(query).isExactlyInstanceOf(TermRangeQuery.class);
    }

    @Test
    public void testNiceErrorIsThrownOnInvalidTopLevelLiteral() {
        assertThatThrownBy(() -> convert("'yes'"))
            .hasMessage("Can't build query from symbol 'yes'");
    }

    @Test
    public void testRangeQueryForUid() throws Exception {
        Query query = convert("_uid > 'foo'");
        assertThat(query).isExactlyInstanceOf(TermRangeQuery.class);
        TermRangeQuery rangeQuery = (TermRangeQuery) query;
        assertThat(rangeQuery.getField()).isEqualTo("_id");
        assertThat(rangeQuery.getLowerTerm().utf8ToString()).isEqualTo("foo");
    }

    @Test
    public void testRangeQueryOnDocThrowsException() throws Exception {
        assertThatThrownBy(() -> convert("_doc > {\"name\"='foo'}"))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith(
                "Unknown function: (doc.users._doc > _map('name', 'foo'))," +
                " no overload found for matching argument types: (object, object).");

    }

    @Test
    public void testIsNullOnGeoPoint() throws Exception {
        Query query = convert("point is null");
        assertThat(query).hasToString("+*:* -FieldExistsQuery [field=point]");
    }

    @Test
    public void testIpRange() throws Exception {
        Query query = convert("addr between '192.168.0.1' and '192.168.0.255'");
        assertThat(query).hasToString(
            "+addr:[192.168.0.1 TO ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff] +addr:[0:0:0:0:0:0:0:0 TO 192.168.0.255]");

        query = convert("addr < 'fe80::1'");
        assertThat(query).hasToString(
            "addr:[0:0:0:0:0:0:0:0 TO fe80:0:0:0:0:0:0:0]");
    }

    @Test
    public void test_ip_eq_uses_point_range_query() throws Exception {
        Query query = convert("addr = '192.168.0.1'");
        assertThat(query).isInstanceOf(PointRangeQuery.class);
    }

    @Test
    public void test_ip_eq_any_uses_point_term_set_query() throws Exception {
        Query query = convert("addr = ANY(['192.168.0.1', '192.168.0.2'])");
        assertThat(query).hasToString("addr:{192.168.0.1 192.168.0.2}");
    }

    @Test
    public void testAnyEqOnTimestampArrayColumn() {
        assertThat(convert("1129224512000 = ANY(ts_array)")).hasToString(
             "ts_array:[1129224512000 TO 1129224512000]");
    }

    @Test
    public void testAnyNotEqOnTimestampColumn() {
        assertThat(convert("ts != ANY([1129224512000])")).hasToString(
            "+(+*:* -(+ts:[1129224512000 TO 1129224512000])) #FieldExistsQuery [field=ts]");
    }

    @Test
    public void testArrayAccessResultsInTermAndFunctionQuery() {
        assertThat(convert("ts_array[1] = 1129224512000")).hasToString(
            "+ts_array:[1129224512000 TO 1129224512000] " +
            "#(ts_array[1] = 1129224512000::bigint)");
        assertThat(convert("ts_array[1] >= 1129224512000")).hasToString(
            "+ts_array:[1129224512000 TO 9223372036854775807] " +
            "#(ts_array[1] >= 1129224512000::bigint)");
        assertThat(convert("ts_array[1] > 1129224512000")).hasToString(
            "+ts_array:[1129224512001 TO 9223372036854775807] " +
            "#(ts_array[1] > 1129224512000::bigint)");
        assertThat(convert("ts_array[1] <= 1129224512000")).hasToString(
            "+ts_array:[-9223372036854775808 TO 1129224512000] " +
            "#(ts_array[1] <= 1129224512000::bigint)");
        assertThat(convert("ts_array[1] < 1129224512000")).hasToString(
            "+ts_array:[-9223372036854775808 TO 1129224511999] " +
            "#(ts_array[1] < 1129224512000::bigint)");
    }

    @Test
    public void testObjectArrayAccessResultsInFunctionQuery() {
        assertThat(convert("o_array[1] = {x=1}")).hasToString(
            "(o_array[1] = {\"x\"=1})");
    }

    @Test
    public void test_is_null_on_subscript_function() {
        Query query = convert("o_array[1]['xs'] is null");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
    }

    @Test
    public void testMatchWithOperator() {
        assertThat(convert("match(tags, 'foo bar') using best_fields with (operator='and')")).hasToString(
            "+tags:foo +tags:bar");
    }

    @Test
    public void testMultiMatchWithOperator() {
        assertThat(convert("match((tags, name), 'foo bar') using best_fields with (operator='and')").toString())
            .satisfiesAnyOf(
                x -> assertThat(x).isEqualTo("(name:foo bar | (+tags:foo +tags:bar))"),
                x -> assertThat(x).isEqualTo("((+tags:foo +tags:bar) | name:foo bar)")
            );
    }

    @Test
    public void testEqOnObjectPreFiltersOnKnownObjectLiteralContents() {
        // termQuery for obj.x; nothing for obj.z because it's missing in the mapping
        assertThat(convert("obj = {x=10, z=20}")).hasToString(
            "+obj.x:[10 TO 10] #(obj = {\"x\"=10, \"z\"=20})");
    }

    @Test
    public void testEqOnObjectDoesBoolTermQueryForContents() {
        assertThat(convert("obj = {x=10, y=20}")).hasToString(
            "+obj.x:[10 TO 10] +obj.y:[20 TO 20]");
    }

    @Test
    public void testEqAnyOnNestedArray() {
        assertThat(convert("[1, 2] = any(o_array['xs'])")).hasToString(
            "+o_array.xs:{1 2} #([1, 2] = ANY(o_array['xs']))");
    }

    @Test
    public void testGtAnyOnNestedArrayIsNotSupported() {
        assertThatThrownBy(() -> convert("[1, 2] > any(o_array['xs'])"))
            .hasMessage("Cannot use `> ANY` if left side is an array");
    }

    @Test
    public void testGteAnyOnNestedArrayIsNotSupported() {
        assertThatThrownBy(() -> convert("[1, 2] >= any(o_array['xs'])"))
            .hasMessage("Cannot use `>= ANY` if left side is an array");
    }

    @Test
    public void testLtAnyOnNestedArrayIsNotSupported() {
        assertThatThrownBy(() -> convert("[1, 2] < any(o_array['xs'])"))
            .hasMessage("Cannot use `< ANY` if left side is an array");
    }

    @Test
    public void testLteAnyOnNestedArrayIsNotSupported() {
        assertThatThrownBy(() -> convert("[1, 2] <= any(o_array['xs'])"))
            .hasMessage("Cannot use `<= ANY` if left side is an array");
    }

    @Test
    public void testAnyOnObjectArrayResultsInXY() {
        Query query = convert("{xs=[1, 1]} = ANY(o_array)");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
    }

    @Test
    public void test_is_null_on_ignored_results_in_function_query() throws Exception {
        Query query = convert("obj_ignored is null");
        assertThat(query).hasToString("(_doc['obj_ignored'] IS NULL)");
    }

    @Test
    public void test_is_not_null_on_ignored_results_in_function_query() throws Exception {
        Query query = convert("obj_ignored is not null");
        assertThat(query).hasToString("(NOT (_doc['obj_ignored'] IS NULL))");
    }

    @Test
    public void test_equal_on_varchar_column_uses_term_query() throws Exception {
        Query query = convert("vchar_name = 'Trillian'");
        assertThat(query)
            .hasToString("vchar_name:Trillian")
            .isExactlyInstanceOf(TermQuery.class);
    }

    @Test
    public void test_eq_on_byte_column() throws Exception {
        Query query = convert("byte_col = 127");
        assertThat(query).hasToString("byte_col:[127 TO 127]");
    }

    @Test
    public void test_eq_on_float_column_uses_float_point_query() throws Exception {
        Query query = convert("f = 42.0::float");
        assertThat(query).hasToString("f:[42.0 TO 42.0]");
    }

    @Test
    public void test_eq_any_on_float_column_uses_set_query() throws Exception {
        Query query = convert("f = ANY([42.0, 41.0])");
        assertThat(query).hasToString("f:{41.0 42.0}");
    }

    @Test
    public void test_eq_on_bool_uses_termquery() throws Exception {
        Query query = convert("bool_col = true");
        assertThat(query).isExactlyInstanceOf(TermQuery.class);
    }

    @Test
    public void test_eq_on_alias_uses_termquery() throws Exception {
        // Testing expression: col as alias = 'foo'
        AliasSymbol alias = new AliasSymbol("aliased", createReference("name", DataTypes.STRING));
        var literal = Literal.of("foo");
        var func = new Function(EqOperator.SIGNATURE, List.of(alias, literal), DataTypes.BOOLEAN);
        Query query = queryTester.toQuery(func);
        assertThat(query).isExactlyInstanceOf(TermQuery.class);
    }

    @Test
    public void test_eq_on_alias_inner_func_uses_termquery() throws Exception {
        // Testing expression: f(col as alias) = 'foo'
        AliasSymbol alias = new AliasSymbol("aliased", createReference("arr", DataTypes.INTEGER_ARRAY));
        var innerFunction = new Function(
            Signature.scalar(
                "array_length",
                TypeSignature.parse("array(E)"),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ).withFeature(Scalar.Feature.DETERMINISTIC),
            List.of(alias, Literal.of(1)), // 1 is a dummy argument for dimension.
            DataTypes.INTEGER
        );

        var func = new Function(EqOperator.SIGNATURE, List.of(innerFunction, Literal.of(5)), DataTypes.BOOLEAN);
        Query query = queryTester.toQuery(func);
        assertThat(query).isExactlyInstanceOf(BooleanQuery.class);
    }

    @Test
    public void test_is_null_on_analyzed_text_column_uses_norms_query() throws Exception {
        Query query = convert("content is null");
        assertThat(query).hasToString("+*:* -FieldExistsQuery [field=content]");
    }

    @Test
    public void test_is_null_without_index_and_docvalues() {
        Query query = convert("text_no_index is null");
        assertThat(query)
            .hasToString("(text_no_index IS NULL)")
            .isExactlyInstanceOf(GenericFunctionQuery.class);
    }

    @Test
    public void test_array_not_any_with_and_without_docvalues() {
        Query query = convert("10 != ANY(y_array)");
        assertThat(query)
                .hasToString("(y_array:[11 TO 9223372036854775807] y_array:[-9223372036854775808 TO 9])~1")
                .isExactlyInstanceOf(BooleanQuery.class);
        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.clauses()).satisfiesExactly(
            // the query class is anonymous
            x -> assertThat(x.getQuery().getClass().getName()).endsWith("LongPoint$1"),
            x -> assertThat(x.getQuery().getClass().getName()).endsWith("LongPoint$1")
        );

        query = convert("10 != ANY(x_array_no_docvalues)");
        assertThat(query)
                .hasToString("(x_array_no_docvalues:[11 TO 2147483647] x_array_no_docvalues:[-2147483648 TO 9])~1")
                .isExactlyInstanceOf(BooleanQuery.class);
        booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.clauses()).satisfiesExactly(
            // the query class is anonymous
            x -> assertThat(x.getQuery().getClass().getName()).doesNotEndWith("LongPoint$1"),
            x -> assertThat(x.getQuery().getClass().getName()).doesNotEndWith("LongPoint$1")
        );
    }

    @Test
    public void test_arr_eq_empty_array_literal_is_optimized() {
        Query query = convert("y_array = []");
        assertThat(query).hasToString("+NumTermsPerDoc: y_array +(y_array = [])");
    }

    @Test
    public void test_any_operators_with_operands_containing_nulls() {
        Query query = convert("x != any([1, null, 2])");
        assertThat(query).hasToString("+(+*:* -(+x:[1 TO 1] +x:[2 TO 2])) #FieldExistsQuery [field=x]");

        query = convert("x = any([1, null, 2])");
        assertThat(query).hasToString("x:{1 2}");

        query = convert("x < any([1, null, 2])");
        assertThat(query).hasToString("(x:[-2147483648 TO 0] x:[-2147483648 TO 1])~1");

        query = convert("name like any(['bar', null, 'foo'])");
        assertThat(query).hasToString("(name:bar name:foo)~1");

        query = convert("name not ilike any(['bar', null, 'foo'])");
        assertThat(query).hasToString("+*:* -(+name:^bar$,flags:66 +name:^foo$,flags:66)");
    }

    @Test
    public void test_any_neq_operator_maps_column_names_to_oids() throws Exception {
        final long oid = 123;
        try (QueryTester tester = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table t (a text)",
            () -> oid
        ).indexValues("a", "s", "t").build()) {
            Query query = tester.toQuery("a != any(['s', 't'])");
            assertThat(query).hasToString(String.format("+(+*:* -(+%s:s +%s:t)) #FieldExistsQuery [field=%s]", oid, oid, oid));
            assertThat(tester.runQuery("a", "a != any(['s'])")).containsExactly("t");
        }
    }

    @Test
    public void test_eq_object_with_undefined_key() {
        Query query = convert("obj = {x=1, y=2, z=3}"); // z undefined
        assertThat(query).hasToString("+obj.x:[1 TO 1] +obj.y:[2 TO 2] #(obj = {\"x\"=1, \"y\"=2, \"z\"=3})");
    }

    @Test
    public void test_equality_query_on_double_array_with_index_off_and_no_docvalues_falls_back_to_generic_query() {
        Query query = convert("d_array_index_off_no_docvalues[1] = 12.34");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);

        query = convert("12.34 != any(d_array_index_off_no_docvalues)");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
    }

    @Test
    public void test_is_not_null_on_not_null_ref() {
        Query query = convert("x is not null");
        assertThat(query).isExactlyInstanceOf(MatchAllDocsQuery.class);
    }

    // tracks a bug: https://github.com/crate/crate/issues/15202
    @Test
    public void test_neq_operator_on_nullable_and_not_nullable_args_filters_nulls() throws Exception {
        final long oid = 123;
        try (QueryTester tester = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table t (a int)",
            () -> oid
        ).indexValues("a", new Object[]{1, null, 2}).build()) {
            Query query = tester.toQuery("a != a||1"); // where a is nullable and a||1 is not null
            assertThat(query).hasToString(String.format("+(+*:* -(a = concat(a, '1'))) +FieldExistsQuery [field=%s]", oid));
            assertThat(tester.runQuery("a", "a != a||1")).containsExactly(1, 2);
        }
    }

    // tracks a bug : https://github.com/crate/crate/pull/15280#issue-2064743724
    @Test
    public void test_neq_operator_on_nullable_and_not_nullable_args_does_not_filter_nulls_from_non_nullable_arg() throws Exception {
        long[] oid = new long[] {123, 124};
        int[] oidIdx = new int[]{0};
        try (QueryTester tester = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table t (a int, b int)",
            () -> oid[oidIdx[0]++]) // oid mapping: a: 123, b: 124
            .indexValues(List.of("a", "b"), null, null)
            .indexValues(List.of("a", "b"), null, 2)
            .indexValues(List.of("a", "b"), 2, null)
            .indexValues(List.of("a", "b"), 2, 2)
            .build()) {
            assertThat(oidIdx[0]).isEqualTo(2);
            Query query = tester.toQuery("a != b||1"); // where a is nullable and b||1 is not null
            assertThat(query).hasToString(String.format("+(+*:* -(a = concat(b, '1'))) +FieldExistsQuery [field=%s]", oid[0]));
            assertThat(tester.runQuery("b", "a != b||1")).containsExactlyInAnyOrder(2, null);
        }
    }

    // tracks a bug: https://github.com/crate/crate/issues/15232
    @Test
    public void test_cannot_use_field_exists_query_on_args_of_coalesce_function() {
        Query query = convert("coalesce(x, y) <> 0");
        assertThat(query).hasToString("+(+*:* -(coalesce(x, y) = 0)) #(NOT (coalesce(x, y) = 0))");
    }

    // tracks a bug : https://github.com/crate/crate/issues/15265
    @Test
    public void test_nested_not_operators() {
        Query query = convert("not (y is not null)");
        assertThat(query).hasToString("+(+*:* -FieldExistsQuery [field=y])");
    }

    @Test
    public void test_not_operator_on_query_equivalent_to_null() {
        Query query = convert("(y % null != 1)");
        assertThat(query).hasToString("+(+*:* -((y % NULL) = 1)) #(NOT ((y % NULL) = 1))");
    }
}

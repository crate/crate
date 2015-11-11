/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.predicate.IsNullPredicate;
import io.crate.operation.predicate.MatchPredicate;
import io.crate.operation.scalar.arithmetic.LogFunction;
import io.crate.operation.scalar.arithmetic.RoundFunction;
import io.crate.operation.scalar.geo.WithinFunction;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.TestingHelpers;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static io.crate.testing.TestingHelpers.createFunction;
import static io.crate.testing.TestingHelpers.createReference;
import static org.hamcrest.core.Is.is;

public class ESQueryBuilderTest extends CrateUnitTest {

    Functions functions;
    static final TableIdent characters = new TableIdent(null, "characters");
    static final Reference name_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "name"), RowGranularity.DOC, DataTypes.STRING));
    private ESQueryBuilder generator;
    private SqlExpressions expressions;
    private EvaluatingNormalizer normalizer;

    @Before
    public void prepare() throws Exception {

        DocTableInfo users = TestingTableInfo.builder(new TableIdent(null, "users"), null)
                .add("name", DataTypes.STRING)
                .add("tags", new ArrayType(DataTypes.STRING))
                .add("extrafield", DataTypes.STRING)
                .add("obj", DataTypes.OBJECT)
                .add("obj", DataTypes.STRING, Arrays.asList("nested"))
                .add("age", DataTypes.INTEGER)
                .add("weight", DataTypes.DOUBLE)
                .add("d_array", new ArrayType(DataTypes.DOUBLE))
                .add("l", DataTypes.LONG)
                .add("f", DataTypes.FLOAT)
                .add("location", DataTypes.GEO_POINT)
                .add("shape_col", DataTypes.GEO_SHAPE)
                .add("short_ref", DataTypes.SHORT)
                .add("is_paranoid", DataTypes.BOOLEAN)
                .build();
        TableRelation usersTr = new TableRelation(users);

        Map<QualifiedName, AnalyzedRelation> sources =
                ImmutableMap.<QualifiedName, AnalyzedRelation>of(new QualifiedName("users"), usersTr);

        generator = new ESQueryBuilder();
        expressions = new SqlExpressions(sources);
        functions = expressions.analysisMD().functions();
        normalizer = new EvaluatingNormalizer(expressions.analysisMD(), usersTr, true);
    }

    private void xcontentAssert(Function whereClause, String expected) throws IOException {
        BytesReference reference = generator.convert(new WhereClause(whereClause));
        String actual = reference.toUtf8();
        assertThat(actual, is(expected));
    }

    private void xcontentAssert(String expression, String expected) throws IOException {
        String actual = convert(expression).toUtf8();
        assertThat(actual, is(expected));
    }

    private String convertSorted(String expression) throws IOException {
        BytesReference query = convert(expression);
        return TestingHelpers.mapToSortedString(XContentHelper.convertToMap(query, false).v2());
    }

    private BytesReference convert(String expression) throws IOException {
        WhereClause whereClause = new WhereClause(normalizer.normalize(expressions.asSymbol(expression)));
        return generator.convert(whereClause);
    }

    @Test
    public void testConvertNestedAnd() throws Exception {
        xcontentAssert("name = 'Marvin' and (age = 84 and l = 8)",
                "{\"query\":{\"bool\":{\"must\":[{\"term\":{\"name\":\"Marvin\"}},{\"bool\":{\"must\":[{\"term\":{\"age\":84}},{\"term\":{\"l\":8}}]}}]}}}");
    }


    @Test
    public void testWhereWithOr() throws Exception {
        xcontentAssert("name = 'Marvin' or name = 'Trillian'",
                "{\"query\":{\"bool\":{\"minimum_should_match\":1,\"should\":[{\"term\":{\"name\":\"Marvin\"}},{\"term\":{\"name\":\"Trillian\"}}]}}}");
    }

    @Test
    public void testWhereReferenceEqStringLiteral() throws Exception {
        xcontentAssert("name = 'Marvin'", "{\"query\":{\"term\":{\"name\":\"Marvin\"}}}");
    }

    @Test
    public void testWhereReferenceEqIntegerLiteral() throws Exception {
        xcontentAssert("age = 40", "{\"query\":{\"term\":{\"age\":40}}}");
    }

    @Test
    public void testWhereReferenceLtDoubleLiteral() throws Exception {
        xcontentAssert("weight < 54.3", "{\"query\":{\"range\":{\"weight\":{\"lt\":54.3}}}}");
    }

    @Test
    public void testWhereReferenceLteFloatLiteral() throws Exception {
        xcontentAssert("f <= 42.1", "{\"query\":{\"range\":{\"f\":{\"lte\":42.1}}}}");
    }

    @Test
    public void testWhereReferenceGtLong() throws Exception {
        xcontentAssert("l > 8", "{\"query\":{\"range\":{\"l\":{\"gt\":8}}}}");
    }

    @Test
    public void testWhereReferenceEqShort() throws Exception {
        xcontentAssert("short_ref = 2", "{\"query\":{\"term\":{\"short_ref\":2}}}");
    }

    @Test
    public void testWhereReferenceEqBoolean() throws Exception {
        xcontentAssert("is_paranoid = true", "{\"query\":{\"term\":{\"is_paranoid\":true}}}");
    }

    @Test
    public void testWhereReferenceLikeString() throws Exception {
        xcontentAssert("name like '%thu%'", "{\"query\":{\"wildcard\":{\"name\":\"*thu*\"}}}");
    }

    @Test
    public void testWhereNotReferenceLikeString() throws Exception {
        xcontentAssert("not name like '%thu%'", "{\"query\":{\"bool\":{\"must_not\":{\"wildcard\":{\"name\":\"*thu*\"}}}}}");
    }

    @Test
    public void testWhereReferenceIsNull() throws Exception {
        xcontentAssert("name is null", "{\"query\":{\"filtered\":{\"filter\":{\"missing\":{\"field\":\"name\",\"existence\":true,\"null_value\":true}}}}}");
    }

    @Test
    public void testWhereReferenceInStringList() throws Exception {
        BytesReference reference = convert("name in ('alpha', 'bravo', 'charlie')");
        Tuple<XContentType, Map<String, Object>> actualMap =
                XContentHelper.convertToMap(reference, true);
        Collection<String> terms = (Collection<String>) ((Map) ((Map) ((Map) ((Map) actualMap.v2()
                .get("query")).get("filtered")).get("filter")).get("terms")).get("name");

        assertThat(terms, Matchers.containsInAnyOrder("alpha", "bravo", "charlie"));
    }

    @Test
    public void testWhereReferenceMatchString() throws Exception {
        xcontentAssert("match(name, 'arthur')", "{\"query\":{\"match\":{\"name\":{\"query\":\"arthur\"}}}}");
    }

    @Test
    public void testWhereRerenceMatchStringWithBoost() throws Exception {
        xcontentAssert("match(name 0.42, 'trillian')", "{\"query\":{\"match\":{\"name\":{\"query\":\"trillian\",\"boost\":0.42}}}}");
    }

    @Test
    public void testWhereMultiMatchString() throws Exception {
        assertThat(
                convertSorted("match((extrafield 4.5, name), 'arthur') using best_fields with (tie_breaker=0.5, analyzer='english')"),
                is("query={multi_match={analyzer=english, fields=[extrafield^4.5, name], query=arthur, tie_breaker=0.5, type=best_fields}}"));
    }

    @Test
    public void testMultiMatchType() throws Exception {
        assertThat(convertSorted("match( (name 0.003, obj['nested']), 'arthur') using phrase with (max_expansions=6, fuzziness=3)"),
                is("query={multi_match={fields=[name^0.003, obj.nested], fuzziness=3, max_expansions=6, query=arthur, type=phrase}}"));
    }


    @Test
    public void testMatchNull() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("cannot use NULL as query term in match predicate");

        FunctionIdent ident = new FunctionIdent(
                MatchPredicate.NAME, ImmutableList.<DataType>of(DataTypes.OBJECT, DataTypes.STRING, DataTypes.STRING, DataTypes.OBJECT)
        );
        MatchPredicate matchImpl = (MatchPredicate) functions.get(ident);
        Function match = new Function(matchImpl.info(),
                Arrays.<Symbol>asList(
                        Literal.newLiteral(
                                new MapBuilder<String, Object>()
                                        .put(name_ref.info().ident().columnIdent().fqn(), 0.003d).map()),
                        Literal.newLiteral(DataTypes.STRING, null),
                        Literal.newLiteral(MatchPredicate.DEFAULT_MATCH_TYPE),
                        Literal.newLiteral(ImmutableMap.<String, Object>of())
                ));
        generator.convert(new WhereClause(match));
    }

    @Test
    public void testWhereMatchAll() throws Exception {
        assertThat(generator.convert(WhereClause.MATCH_ALL).toUtf8(), is("{\"query\":{\"match_all\":{}}}"));
    }

    @Test
    public void testWhereReferenceAnyLike() throws Exception {
        xcontentAssert("'foo%' like any (tags)", "{\"query\":{\"wildcard\":{\"tags\":\"foo*\"}}}");
    }

    @Test
    public void testWhereReferenceAnyNotLike() throws Exception {
        xcontentAssert("'foo%' not like any (tags)",
                "{\"query\":{\"regexp\":{\"tags\":{\"value\":\"~(foo.*)\",\"flags\":\"COMPLEMENT\"}}}}");
    }

    @Test
    public void testAnyLikeArrayReference() throws Exception {
        xcontentAssert("name like any (['foo%', '%bar'])", "{\"query\":{\"bool\":{\"minimum_should_match\":1,\"should\":[" +
                    "{\"wildcard\":{\"name\":\"foo*\"}}," +
                    "{\"wildcard\":{\"name\":\"*bar\"}}" +
                "]}}}");
    }

    @Test
    public void testAnyNotLikeArrayReference() throws Exception {
        xcontentAssert("name not like any (['foo%', '%bar'])",
                "{\"query\":{" +
                        "\"bool\":{\"must_not\":{" +
                            "\"bool\":{\"must\":[" +
                                "{\"wildcard\":{\"name\":\"foo*\"}}," +
                                "{\"wildcard\":{\"name\":\"*bar\"}}" +
                            "]" +
                        "}}}}}"
        );
    }

    @Test (expected = UnsupportedOperationException.class)
    public void testQueryWith_Version() throws Exception {
        convert("_version = 4");
    }

    @Test
    public void testAnyGreater() throws Exception {
        xcontentAssert("0.0 > any(d_array)", "{\"query\":{\"range\":{\"d_array\":{\"lt\":0.0}}}}");
    }

    @Test
    public void testAnyGreaterEquals() throws Exception {
        xcontentAssert("0.0 >= any(d_array)", "{\"query\":{\"range\":{\"d_array\":{\"lte\":0.0}}}}");
    }

    @Test
    public void testAnyGreaterEqualsArrayLiteral() throws Exception {
        xcontentAssert("weight >= any ([1.0, 0.5, -1.0])", "{\"query\":{\"bool\":{\"minimum_should_match\":1,\"should\":[" +
                    "{\"range\":{\"weight\":{\"gte\":1.0}}}," +
                    "{\"range\":{\"weight\":{\"gte\":0.5}}}," +
                    "{\"range\":{\"weight\":{\"gte\":-1.0}}}" +
                "]}}}");
    }

    @Test
    public void testAnyEqLiteralArrayColumn() throws Exception {
        xcontentAssert("4.2 = any(d_array)", "{\"query\":{\"term\":{\"d_array\":4.2}}}");
    }

    @Test
    public void testAnyEqReferenceAndArrayLiteral() throws Exception {
        xcontentAssert("weight = any([-1.5, 0.0, 1.5])",
                "{\"query\":{\"filtered\":{\"query\":{\"match_all\":{}},\"filter\":{\"terms\":{\"weight\":[-1.5,0.0,1.5]}}}}}");
    }

    @Test
    public void testAnyNeqReferenceAndArrayLiteral() throws Exception {
        xcontentAssert("weight != any ([-1.5, 0.0, 1.5])", "{\"query\":{" +
                "\"filtered\":{" +
                    "\"query\":{\"match_all\":{}}," +
                    "\"filter\":{" +
                        "\"bool\":{\"must_not\":{" +
                            "\"bool\":{\"must\":[" +
                                "{\"term\":{\"weight\":-1.5}}," +
                                "{\"term\":{\"weight\":0.0}}," +
                                "{\"term\":{\"weight\":1.5}}]" +
                            "}}}}}}}");
    }

    @Test
    public void testDistanceGteQuery() throws Exception {
        xcontentAssert("distance(location, 'POINT (10 20)') >= 20.0",
                "{\"query\":{\"filtered\":{\"query\":{\"match_all\":{}},\"filter\":{\"geo_distance_range\":{\"location\":[10.0,20.0],\"gte\":20.0}}}}}");
    }

    @Test
    public void testDistanceGteQuerySwappedArgs() throws Exception {
        xcontentAssert("distance('POINT (10 20)', location) >= 20.0",
                "{\"query\":{\"filtered\":{\"query\":{\"match_all\":{}},\"filter\":{\"geo_distance_range\":{\"location\":[10.0,20.0],\"gte\":20.0}}}}}");
    }

    @Test
    public void testDistanceEqQuery() throws Exception {
        xcontentAssert("distance(location, 'POINT(10 20)') = 20.0",
                "{\"query\":{\"filtered\":{\"query\":{\"match_all\":{}},\"filter\":{\"geo_distance_range\":{\"location\":[10.0,20.0],\"from\":20.0,\"to\":20.0,\"include_upper\":true,\"include_lower\":true}}}}}");
    }

    @Test
    public void testWhereDistanceFunctionEqDistanceFunction() throws Exception {
        /**
         * distance(p, ...) = distance(p, ...) isn't supported
         */
        expectedException.expect(IllegalArgumentException.class);
        convert("distance(location, 'POINT (10 20)') = distance(location, 'POINT (10 20)')");
    }


    @Test
    public void testWhereClauseWithWithinPolygonQuery() throws Exception {
        xcontentAssert("within(location, 'POLYGON (( 5 5, 30 5, 30 30, 5 35, 5 5 ))')",
                "{\"query\":{\"filtered\":{\"query\":{\"match_all\":{}},\"filter\":{\"geo_polygon\":{\"location\":{\"points\":[{\"lon\":30.0,\"lat\":5.0},{\"lon\":30.0,\"lat\":30.0},{\"lon\":5.0,\"lat\":35.0},{\"lon\":5.0,\"lat\":5.0},{\"lon\":30.0,\"lat\":5.0}]}}}}}}");
    }

    @Test
    public void testWhereClauseWithWithinRectangleQuery() throws Exception {
        xcontentAssert("within(location, 'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))')",
                "{\"query\":{\"filtered\":{\"query\":{\"match_all\":{}},\"filter\":{\"geo_bounding_box\":{\"location\":{\"top_left\":{\"lon\":5.0,\"lat\":30.0},\"bottom_right\":{\"lon\":30.0,\"lat\":5.0}}}}}}}");
    }

    @Test
    public void testWhereClauseWithWithinPolygonEqualsTrueQuery() throws Exception {
        xcontentAssert("within(location, 'POLYGON (( 5 5, 30 5, 30 30, 5 35, 5 5 ))') = true",
                "{\"query\":{\"filtered\":{\"query\":{\"match_all\":{}},\"filter\":{\"geo_polygon\":{\"location\":{\"points\":[{\"lon\":30.0,\"lat\":5.0},{\"lon\":30.0,\"lat\":30.0},{\"lon\":5.0,\"lat\":35.0},{\"lon\":5.0,\"lat\":5.0},{\"lon\":30.0,\"lat\":5.0}]}}}}}}");
    }

    @Test
    public void testWhereClauseWithWithinEqualsFalseQuery() throws Exception {
        xcontentAssert("within(location, 'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))') = false",
                "{\"query\":{\"bool\":{\"must_not\":{\"filtered\":{\"query\":{\"match_all\":{}},\"filter\":{\"geo_bounding_box\":{\"location\":{\"top_left\":{\"lon\":5.0,\"lat\":30.0},\"bottom_right\":{\"lon\":30.0,\"lat\":5.0}}}}}}}}}");
    }

    @Test
    public void testWithinEqWithin() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        convert("within(location, 'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))') = within(location, 'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))')");
    }

    @Test
    public void testWithinBBox() throws Exception {
        xcontentAssert("within(location, 'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))') = false",
                "{\"query\":{\"bool\":{\"must_not\":{" +
                "\"filtered\":{\"query\":{\"match_all\":{}}," +
                "\"filter\":{" +
                "\"geo_bounding_box\":{" +
                "\"location\":{\"top_left\":{\"lon\":5.0,\"lat\":30.0},\"bottom_right\":{\"lon\":30.0,\"lat\":5.0}}}}}}}}}");

    }

    @Test
    public void testWithinHolePolygon() throws Exception {
        xcontentAssert("within(location, 'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ), (10 10, 10 11, 12 10, 10 10))') = false",
                "{\"query\":{\"bool\":{\"must_not\":{\"filtered\":{\"query\":{\"match_all\":{}}," +
                "\"filter\":{" +
                "\"geo_polygon\":{" +
                "\"location\":{\"points\":[" +
                "{\"lon\":30.0,\"lat\":5.0},{\"lon\":30.0,\"lat\":30.0},{\"lon\":5.0,\"lat\":30.0}," +
                "{\"lon\":5.0,\"lat\":5.0},{\"lon\":30.0,\"lat\":5.0},{\"lon\":10.0,\"lat\":11.0}," +
                "{\"lon\":12.0,\"lat\":10.0},{\"lon\":10.0,\"lat\":10.0},{\"lon\":10.0,\"lat\":11.0}]}}}}}}}}");

    }

    @Test
    public void testWithinShapeReference() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Second argument to the within function must be a literal");
        Function withinFunction = createFunction(
                WithinFunction.NAME,
                DataTypes.BOOLEAN,
                createReference("location", DataTypes.GEO_POINT),
                createReference("shape", DataTypes.GEO_SHAPE)
        );
        Function eqFunction = createFunction(
                EqOperator.NAME,
                DataTypes.BOOLEAN,
                withinFunction,
                Literal.BOOLEAN_TRUE
        );
        generator.convert(new WhereClause(eqFunction));


    }

    @Test
    public void testWhereNumericScalar() throws Exception {
        Function scalarFunction = new Function(
                new FunctionInfo(
                        new FunctionIdent(RoundFunction.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE)),
                        DataTypes.LONG),
                Arrays.<Symbol>asList(createReference("price", DataTypes.DOUBLE))
        );
        Function whereClause = new Function(
                new FunctionInfo(
                        new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE, DataTypes.DOUBLE)),
                        DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(scalarFunction, Literal.newLiteral(20.0d))
        );

        xcontentAssert(whereClause,
                "{\"query\":{\"filtered\":{\"query\":{\"match_all\":{}}," +
                        "\"filter\":{\"script\":{\"script\":\"numeric_scalar_search\"," +
                        "\"lang\":\"native\",\"params\":" +
                        "{\"op\":\"op_=\",\"args\":[{\"scalar_name\":\"round\"," +
                        "\"type\":10,\"args\":[{\"field_name\":\"price\",\"type\":6}]}," +
                        "{\"value\":20.0,\"type\":6}]}}}}}}");
    }

    @Test
    public void testWhereNumericScalarWithArguments() throws Exception {
        Function scalarFunction = new Function(
                new FunctionInfo(
                        new FunctionIdent(LogFunction.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE, DataTypes.INTEGER)),
                        DataTypes.LONG),
                Arrays.<Symbol>asList(
                        createReference("price", DataTypes.DOUBLE),
                        Literal.newLiteral(DataTypes.INTEGER, 100)
                        )
        );
        Function whereClause = new Function(
                new FunctionInfo(
                        new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE, DataTypes.DOUBLE)),
                        DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(scalarFunction, Literal.newLiteral(20.0))
        );

        xcontentAssert(whereClause, "{\"query\":{\"filtered\":{\"query\":{\"match_all\":{}}," +
                "\"filter\":{\"script\":{\"script\":\"numeric_scalar_search\"," +
                "\"lang\":\"native\",\"params\":" +
                "{\"op\":\"op_=\",\"args\":[{\"scalar_name\":\"log\"," +
                "\"type\":10,\"args\":[" +
                "{\"field_name\":\"price\",\"type\":6}," +
                "{\"value\":100,\"type\":9}" +
                "]},{\"value\":20.0,\"type\":6}]}}}}}}");
    }

    @Test
    public void testWhereNumericScalarWithArgumentsTwoReferences() throws Exception {
        Function scalarFunction = new Function(
                new FunctionInfo(
                        new FunctionIdent(LogFunction.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE, DataTypes.INTEGER)),
                        DataTypes.LONG),
                Arrays.<Symbol>asList(
                        createReference("price", DataTypes.DOUBLE),
                        createReference("base", DataTypes.INTEGER)
                )
        );
        Function whereClause = new Function(
                new FunctionInfo(
                        new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE, DataTypes.DOUBLE)),
                        DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(scalarFunction, Literal.newLiteral(20.0))
        );

        xcontentAssert(whereClause, "{\"query\":{\"filtered\":{\"query\":{\"match_all\":{}}," +
                "\"filter\":{\"script\":{\"script\":\"numeric_scalar_search\"," +
                "\"lang\":\"native\",\"params\":" +
                "{\"op\":\"op_=\",\"args\":[{\"scalar_name\":\"log\"," +
                "\"type\":10,\"args\":[" +
                "{\"field_name\":\"price\",\"type\":6}," +
                "{\"field_name\":\"base\",\"type\":9}" +
                "]},{\"value\":20.0,\"type\":6}]}}}}}}");
    }

    @Test
    public void testEqOnTwoArrayShouldFail() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot compare two arrays");

        DataType intArray = new ArrayType(DataTypes.INTEGER);
        generator.convert(new WhereClause(createFunction(EqOperator.NAME, DataTypes.BOOLEAN,
                createReference("a", intArray),
                Literal.newLiteral(intArray, new Object[]{10, 20})
        )));
    }

    @Test
    public void testWhereNumericScalarEqNumericScalar() throws Exception {
        /**
         * round(a) = round(b) isn't supported
         */
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Can't compare two scalar functions");
        convert("round(weight) = round(age)");
    }

    @Test
    public void testNestedScalar() throws Exception {
        /**
         * round(round(a) isn't supported
         */
        Function scalarFunction = new Function(
                new FunctionInfo(
                        new FunctionIdent(RoundFunction.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE)),
                        DataTypes.LONG),
                Arrays.<Symbol>asList(new Function(
                        new FunctionInfo(
                                new FunctionIdent(RoundFunction.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE)),
                                DataTypes.LONG),
                        Arrays.<Symbol>asList(createReference("price", DataTypes.DOUBLE))))
        );
        Function whereClause = new Function(
                new FunctionInfo(
                        new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE, DataTypes.DOUBLE)),
                        DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(scalarFunction, Literal.newLiteral(DataTypes.INTEGER, 100))
        );
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Nested scalar functions are not supported");
        generator.convert(new WhereClause(whereClause));
    }

    @Test
    public void testIsNullOnFunctionThrowsMeaningfulError() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("IS NULL only works on columns, not on functions or other expressions");

        Function query = createFunction(IsNullPredicate.NAME, DataTypes.BOOLEAN,
                createFunction(EqOperator.NAME, DataTypes.BOOLEAN,
                        createReference("x", DataTypes.INTEGER), Literal.newLiteral(10)));
        generator.convert(new WhereClause(query));
    }

    @Test
    public void testGeoShapeMatchWithDefaultMatchType() throws Exception {
        xcontentAssert("match(shape_col, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')",
                "{\"query\":" +
                "{\"geo_shape\":{\"shape_col\":{\"shape\":{\"type\":\"Polygon\",\"coordinates\":" +
                "[[[30.0,10.0],[40.0,40.0],[20.0,40.0],[10.0,20.0],[30.0,10.0]]]},\"relation\":\"intersects\"}}}}");
    }

    @Test
    public void testGeoShapeMatchDisJoint() throws Exception {
        xcontentAssert("match(shape_col, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') using disjoint",
        "{\"query\":" +
        "{\"geo_shape\":{\"shape_col\":{\"shape\":{\"type\":\"Polygon\",\"coordinates\":" +
        "[[[30.0,10.0],[40.0,40.0],[20.0,40.0],[10.0,20.0],[30.0,10.0]]]},\"relation\":\"disjoint\"}}}}");
    }

    @Test
    public void testGeoShapeMatchContains() throws Exception {
        xcontentAssert("match(shape_col, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') using contains",
        "{\"query\":" +
        "{\"geo_shape\":{\"shape_col\":{\"shape\":{\"type\":\"Polygon\",\"coordinates\":" +
        "[[[30.0,10.0],[40.0,40.0],[20.0,40.0],[10.0,20.0],[30.0,10.0]]]},\"relation\":\"contains\"}}}}");
    }

    @Test
    public void testGeoShapeMatchWithin() throws Exception {
        xcontentAssert("match(shape_col, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') using within",
        "{\"query\":" +
        "{\"geo_shape\":{\"shape_col\":{\"shape\":{\"type\":\"Polygon\",\"coordinates\":" +
        "[[[30.0,10.0],[40.0,40.0],[20.0,40.0],[10.0,20.0],[30.0,10.0]]]},\"relation\":\"within\"}}}}");
    }
}

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
import com.google.common.collect.ImmutableSet;
import io.crate.PartitionName;
import io.crate.analyze.where.WhereClause;
import io.crate.metadata.*;
import io.crate.operation.operator.*;
import io.crate.operation.operator.any.AnyLikeOperator;
import io.crate.operation.operator.any.AnyNotLikeOperator;
import io.crate.operation.predicate.IsNullPredicate;
import io.crate.operation.predicate.MatchPredicate;
import io.crate.operation.predicate.NotPredicate;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.operation.scalar.arithmetic.LogFunction;
import io.crate.operation.scalar.arithmetic.RoundFunction;
import io.crate.operation.scalar.geo.DistanceFunction;
import io.crate.operation.scalar.geo.WithinFunction;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dml.ESDeleteByQueryNode;
import io.crate.planner.node.dql.QueryThenFetchNode;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.*;

import static io.crate.testing.TestingHelpers.createFunction;
import static io.crate.testing.TestingHelpers.createReference;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ESQueryBuilderTest {

    Functions functions;
    static final TableIdent characters = new TableIdent(null, "characters");
    static final Reference name_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "name"), RowGranularity.DOC, DataTypes.STRING));
    static final Reference age_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "age"), RowGranularity.DOC, DataTypes.INTEGER));
    static final Reference weight_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "weight"), RowGranularity.DOC, DataTypes.DOUBLE));
    static final Reference float_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "float_ref"), RowGranularity.DOC, DataTypes.FLOAT));
    static final Reference long_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "long_ref"), RowGranularity.DOC, DataTypes.LONG));
    static final Reference short_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "short_ref"), RowGranularity.DOC, DataTypes.SHORT));
    static final Reference isParanoid = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "isParanoid"), RowGranularity.DOC, DataTypes.BOOLEAN));
    static final Reference extrafield = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "extrafield"), RowGranularity.DOC, DataTypes.STRING));
    static final Reference tagsField = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "tags"), RowGranularity.DOC, new ArrayType(DataTypes.STRING)
    ));
    static final Reference objectField = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "object_field"), RowGranularity.DOC, DataTypes.OBJECT
    ));
    static final Reference nestedField = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "object_field", Arrays.asList("nested")), RowGranularity.DOC, DataTypes.STRING
    ));
    private ESQueryBuilder generator;

    private List<DataType> typeX2(DataType type) {
        return Arrays.asList(type, type);
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        functions = new ModulesBuilder()
                .add(new OperatorModule())
                .add(new PredicateModule())
                .add(new ScalarFunctionModule())
                .createInjector().getInstance(Functions.class);
        generator = new ESQueryBuilder();
    }

    private void xcontentAssert(Function whereClause, String expected) throws IOException {
        BytesReference reference = generator.convert(new WhereClause(whereClause));
        String actual = reference.toUtf8();
        assertThat(actual, is(expected));
    }

    @Test
    public void testConvertNestedAnd() throws Exception {
        FunctionImplementation eqStringImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataTypes.STRING)));
        FunctionImplementation eqAgeImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataTypes.INTEGER)));
        FunctionImplementation eqLongImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataTypes.LONG)));
        FunctionImplementation andImpl = functions.get(new FunctionIdent(AndOperator.NAME, typeX2(DataTypes.BOOLEAN)));

        Function eqName = new Function(eqStringImpl.info(), Arrays.<Symbol>asList(name_ref, Literal.newLiteral("Marvin")));
        Function eqAge = new Function(eqAgeImpl.info(), Arrays.<Symbol>asList(age_ref, Literal.newLiteral(84)));
        Function eqLong = new Function(eqLongImpl.info(), Arrays.<Symbol>asList(long_ref, Literal.newLiteral(8L)));

        Function rightAnd = new Function(andImpl.info(), Arrays.<Symbol>asList(eqAge, eqLong));
        Function leftAnd = new Function(andImpl.info(), Arrays.<Symbol>asList(eqName, rightAnd));

        xcontentAssert(leftAnd, "{\"query\":{\"bool\":{\"must\":[{\"term\":{\"name\":\"Marvin\"}},{\"bool\":{\"must\":[{\"term\":{\"age\":84}},{\"term\":{\"long_ref\":8}}]}}]}}}");
    }


    @Test
    public void testWhereWithOr() throws Exception {
        // where name = marvin and age = 84 and longField = 8

        FunctionImplementation eqStringImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataTypes.STRING)));
        FunctionImplementation orImpl = functions.get(new FunctionIdent(OrOperator.NAME, typeX2(DataTypes.BOOLEAN)));

        Function eqMarvin = new Function(eqStringImpl.info(), Arrays.<Symbol>asList(name_ref, Literal.newLiteral("Marvin")));
        Function eqTrillian = new Function(eqStringImpl.info(), Arrays.<Symbol>asList(name_ref, Literal.newLiteral("Trillian")));

        Function whereClause = new Function(orImpl.info(), Arrays.<Symbol>asList(eqMarvin, eqTrillian));

        xcontentAssert(whereClause, "{\"query\":{\"bool\":{\"minimum_should_match\":1,\"should\":[{\"term\":{\"name\":\"Marvin\"}},{\"term\":{\"name\":\"Trillian\"}}]}}}");
    }

    @Test
    public void testWhereReferenceEqStringLiteral() throws Exception {
        FunctionImplementation eqImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataTypes.STRING)));
        Function whereClause = new Function(eqImpl.info(), Arrays.<Symbol>asList(name_ref, Literal.newLiteral("Marvin")));

        xcontentAssert(whereClause, "{\"query\":{\"term\":{\"name\":\"Marvin\"}}}");
    }

    @Test
    public void testWhereReferenceEqIntegerLiteral() throws Exception {
        FunctionImplementation eqImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataTypes.INTEGER)));
        Function whereClause = new Function(eqImpl.info(), Arrays.<Symbol>asList(age_ref, Literal.newLiteral(40)));
        xcontentAssert(whereClause, "{\"query\":{\"term\":{\"age\":40}}}");
    }

    @Test
    public void testWhereReferenceLtDoubleLiteral() throws Exception {
        FunctionImplementation ltImpl = functions.get(new FunctionIdent(LtOperator.NAME, typeX2(DataTypes.DOUBLE)));
        Function whereClause = new Function(ltImpl.info(), Arrays.<Symbol>asList(weight_ref, Literal.newLiteral(54.3)));
        xcontentAssert(whereClause, "{\"query\":{\"range\":{\"weight\":{\"lt\":54.3}}}}");
    }

    @Test
    public void testWhereReferenceLteFloatLiteral() throws Exception {
        FunctionImplementation impl = functions.get(new FunctionIdent(LteOperator.NAME, typeX2(DataTypes.FLOAT)));
        Function whereClause = new Function(impl.info(), Arrays.<Symbol>asList(float_ref, Literal.newLiteral(42.1)));
        xcontentAssert(whereClause, "{\"query\":{\"range\":{\"float_ref\":{\"lte\":42.1}}}}");
    }

    @Test
    public void testWhereReferenceGtLong() throws Exception {
        FunctionImplementation impl = functions.get(new FunctionIdent(GtOperator.NAME, typeX2(DataTypes.LONG)));
        Function whereClause = new Function(impl.info(), Arrays.<Symbol>asList(long_ref, Literal.newLiteral(8L)));
        xcontentAssert(whereClause, "{\"query\":{\"range\":{\"long_ref\":{\"gt\":8}}}}");
    }

    @Test
    public void testWhereReferenceEqShort() throws Exception {
        FunctionImplementation impl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataTypes.SHORT)));
        Function whereClause = new Function(impl.info(),
                Arrays.<Symbol>asList(short_ref, Literal.newLiteral(DataTypes.SHORT, (short)2)));
        xcontentAssert(whereClause, "{\"query\":{\"term\":{\"short_ref\":2}}}");
    }

    @Test
    public void testWhereReferenceEqBoolean() throws Exception {
        FunctionImplementation impl = functions.get(new FunctionIdent(
                EqOperator.NAME, typeX2(isParanoid.valueType())));
        Function whereClause = new Function(impl.info(),
                Arrays.<Symbol>asList(isParanoid, Literal.newLiteral(isParanoid.valueType(), true)));
        xcontentAssert(whereClause, "{\"query\":{\"term\":{\"isParanoid\":true}}}");
    }

    @Test
    public void testWhereReferenceLikeString() throws Exception {
        FunctionImplementation impl = functions.get(new FunctionIdent(LikeOperator.NAME, typeX2(name_ref.valueType())));
        Function whereClause = new Function(impl.info(),
                Arrays.<Symbol>asList(name_ref, Literal.newLiteral("%thu%")));
        xcontentAssert(whereClause, "{\"query\":{\"wildcard\":{\"name\":\"*thu*\"}}}");
    }

    @Test
    public void testWhereNotReferenceLikeString() throws Exception {
        FunctionImplementation notOp = functions.get(
                new FunctionIdent(NotPredicate.NAME, Arrays.<DataType>asList(DataTypes.BOOLEAN)));
        FunctionImplementation likeOp = functions.get(new FunctionIdent(LikeOperator.NAME, typeX2(name_ref.valueType())));

        Function likeClause = new Function(likeOp.info(),
                Arrays.<Symbol>asList(name_ref, Literal.newLiteral("%thu%")));
        Function whereClause = new Function(notOp.info(), Arrays.<Symbol>asList(likeClause));
        xcontentAssert(whereClause, "{\"query\":{\"bool\":{\"must_not\":{\"wildcard\":{\"name\":\"*thu*\"}}}}}");
    }

    @Test
    public void testWhereReferenceIsNull() throws Exception {
        FunctionImplementation isNullImpl = functions.get(
                new FunctionIdent(IsNullPredicate.NAME, Arrays.asList(extrafield.valueType())));

        Function isNull = new Function(isNullImpl.info(), Arrays.<Symbol>asList(extrafield));
        xcontentAssert(isNull, "{\"query\":{\"filtered\":{\"filter\":{\"missing\":{\"field\":\"extrafield\",\"existence\":true,\"null_value\":true}}}}}");
    }

    @Test
    public void testWhereReferenceInStringList() throws Exception {
        // where name in ("alpha", "bravo", "charlie")
        Reference ref = name_ref;
        FunctionImplementation inListImpl = functions.get(
                new FunctionIdent(InOperator.NAME,
                Arrays.<DataType>asList(DataTypes.STRING, new SetType(DataTypes.STRING))
        ));

        ImmutableSet<BytesRef> list = ImmutableSet.of(
                new BytesRef("alpha"), new BytesRef("bravo"), new BytesRef("charlie"));
        Literal set = Literal.newLiteral(new SetType(DataTypes.STRING), list);
        Function inList = new Function(inListImpl.info(), Arrays.<Symbol>asList(ref, set));

        BytesReference reference = generator.convert(new WhereClause(inList));
        Tuple<XContentType, Map<String, Object>> actualMap =
                XContentHelper.convertToMap(reference, true);
        ArrayList<String> actualList = ((ArrayList)
                ((Map)((Map)actualMap.v2()
                .get("query"))
                .get("terms"))
                .get("name"));

        assertEquals(ImmutableSet.of("alpha", "bravo", "charlie"), new HashSet<>(actualList));
    }

    @Test
    public void testWhereReferenceMatchString() throws Exception {
        FunctionIdent functionIdent = new FunctionIdent(
                MatchPredicate.NAME, ImmutableList.<DataType>of(DataTypes.OBJECT, DataTypes.STRING, DataTypes.STRING, DataTypes.OBJECT));
        MatchPredicate matchImpl = (MatchPredicate)functions.get(functionIdent);
        Function match = new Function(matchImpl.info(),
                Arrays.<Symbol>asList(
                        Literal.newLiteral(
                                new MapBuilder<String, Object>().put(name_ref.info().ident().columnIdent().fqn(), null).map()),
                        Literal.newLiteral("arthur"),
                        Literal.newLiteral(MatchPredicate.DEFAULT_MATCH_TYPE),
                        Literal.newLiteral(DataTypes.OBJECT, null)
                ));

        xcontentAssert(match, "{\"query\":{\"match\":{\"name\":\"arthur\"}}}");
    }

    @Test
    public void testWhereMultiMatchString() throws Exception {
        FunctionIdent ident = new FunctionIdent(
                MatchPredicate.NAME, ImmutableList.<DataType>of(DataTypes.OBJECT, DataTypes.STRING, DataTypes.STRING, DataTypes.OBJECT)
        );
        MatchPredicate matchImpl = (MatchPredicate)functions.get(ident);
        Function match = new Function(matchImpl.info(),
                Arrays.<Symbol>asList(
                        Literal.newLiteral(
                                new MapBuilder<String, Object>()
                                        .put(name_ref.info().ident().columnIdent().fqn(), null)
                                        .put(extrafield.info().ident().columnIdent().fqn(), 4.5d)
                                        .map()),
                        Literal.newLiteral("arthur"),
                        Literal.newLiteral(MatchPredicate.DEFAULT_MATCH_TYPE),
                        Literal.newLiteral(
                                new MapBuilder<String, Object>()
                                        .put("tie_breaker", 0.5)
                                        .put("analyzer", "english")
                                        .map()
                        )
                ));
        xcontentAssert(match, "{\"query\":{\"multi_match\":{\"type\":\"best_fields\"," +
                "\"fields\":[\"extrafield^4.5\",\"name\"]," +
                "\"query\":\"arthur\",\"tie_breaker\":0.5,\"analyzer\":\"english\"}}}");
    }

    @Test
    public void testMultiMatchType() throws Exception {
        FunctionIdent ident = new FunctionIdent(
                MatchPredicate.NAME, ImmutableList.<DataType>of(DataTypes.OBJECT, DataTypes.STRING, DataTypes.STRING, DataTypes.OBJECT)
        );
        MatchPredicate matchImpl = (MatchPredicate) functions.get(ident);
        Function match = new Function(matchImpl.info(),
                Arrays.<Symbol>asList(
                        Literal.newLiteral(
                                new MapBuilder<String, Object>()
                                        .put(name_ref.info().ident().columnIdent().fqn(), 0.003d)
                                        .put(nestedField.info().ident().columnIdent().fqn(), null)
                                        .map()),
                        Literal.newLiteral("arthur"),
                        Literal.newLiteral("phrase"),
                        Literal.newLiteral(
                                new MapBuilder<String, Object>()
                                        .put("fuzziness", 3)
                                        .put("max_expansions", 6)
                                        .map()
                        )
                ));
        xcontentAssert(match, "{\"query\":{\"multi_match\":{\"type\":\"phrase\"," +
                "\"fields\":[\"name^0.003\",\"object_field.nested\"]," +
                "\"query\":\"arthur\",\"max_expansions\":6,\"fuzziness\":3}}}");
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
                                        .put(name_ref.info().ident().columnIdent().fqn(), 0.003d)
                                        .put(nestedField.info().ident().columnIdent().fqn(), null)
                                        .map()),
                        Literal.newLiteral(DataTypes.STRING, null),
                        Literal.newLiteral(MatchPredicate.DEFAULT_MATCH_TYPE),
                        Literal.newLiteral(
                                new MapBuilder<String, Object>()
                                        .put("fuzziness", 3)
                                        .put("max_expansions", 6)
                                        .map()
                        )
                ));
        generator.convert(new WhereClause(match));
    }

    @Test
    public void testWhereNoMatch() throws Exception {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("A where clause with no match should not result in an ES query");

        generator.convert(WhereClause.NO_MATCH);
    }

    @Test
    public void testWhereMatchAll() throws Exception {
        assertThat(generator.convert(WhereClause.MATCH_ALL).toUtf8(), is("{\"query\":{\"match_all\":{}}}"));
    }

    @Test
    public void testWhereReferenceAnyLike() throws Exception {
        FunctionIdent functionIdent = new FunctionIdent(AnyLikeOperator.NAME,
                Arrays.<DataType>asList(new ArrayType(DataTypes.STRING), DataTypes.STRING));
        FunctionImplementation anyLikeImpl = functions.get(functionIdent);
        Function anyLike = new Function(anyLikeImpl.info(),
                Arrays.<Symbol>asList(tagsField,
                        Literal.newLiteral("foo%")));
        xcontentAssert(anyLike, "{\"query\":{\"wildcard\":{\"tags\":\"foo*\"}}}");
    }

    @Test
    public void testWhereReferenceAnyNotLike() throws Exception {
        FunctionIdent functionIdent = new FunctionIdent(AnyNotLikeOperator.NAME,
                Arrays.<DataType>asList(new ArrayType(DataTypes.STRING), DataTypes.STRING));
        FunctionImplementation anyNotLikeImpl = functions.get(functionIdent);
        Function anyNotLike = new Function(anyNotLikeImpl.info(),
                Arrays.<Symbol>asList(tagsField,
                        Literal.newLiteral("foo%")));
        xcontentAssert(anyNotLike, "{\"query\":{\"regexp\":{\"tags\":{\"value\":\"~(foo.*)\",\"flags\":\"COMPLEMENT\"}}}}");
    }

    @Test
    public void testMinScoreIsSet() throws Exception {
        Reference minScore_ref = new Reference(
                new ReferenceInfo(new ReferenceIdent(null, "_score"), RowGranularity.DOC, DataTypes.DOUBLE));

        Function whereClause = new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE, DataTypes.DOUBLE)),
                DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(minScore_ref, Literal.newLiteral(0.4))
        );
        QueryThenFetchNode node = new QueryThenFetchNode(new Routing(),
                ImmutableList.<Symbol>of(), null, null, null, null, null, new WhereClause(whereClause), null);
        BytesReference bytesReference = generator.convert(node);

        assertThat(bytesReference.toUtf8(),
                is("{\"_source\":false,\"query\":{\"match_all\":{}},\"min_score\":0.4,\"from\":0,\"size\":10000}"));
    }

    @Test
    public void testConvertESSearchNode() throws Exception {
        FunctionImplementation eqImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataTypes.STRING)));
        Function whereClause = new Function(eqImpl.info(), Arrays.<Symbol>asList(name_ref, Literal.newLiteral("Marvin")));

        QueryThenFetchNode searchNode = new QueryThenFetchNode(
                new Routing(),
                ImmutableList.<Symbol>of(name_ref),
                ImmutableList.<Symbol>of(),
                new boolean[0],
                new Boolean[0],
                null,
                null,
                new WhereClause(whereClause),
                null);

        BytesReference reference = generator.convert(searchNode);
        String actual = reference.toUtf8();
        assertThat(actual, is(
                "{\"_source\":{\"include\":[\"name\"]},\"query\":{\"term\":{\"name\":\"Marvin\"}},\"from\":0,\"size\":10000}"));
    }

    @Test (expected = UnsupportedOperationException.class)
    public void testQueryWith_Version() throws Exception {
        FunctionImplementation eqImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataTypes.STRING)));
        Function whereClause = new Function(eqImpl.info(), Arrays.<Symbol>asList(
                createReference("_version", DataTypes.INTEGER),
                Literal.newLiteral(4)));

        generator.convert(new WhereClause(whereClause));
    }

    @Test
    public void testConvertESDeleteByQueryNode() throws Exception {
        FunctionImplementation eqImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataTypes.STRING)));
        Function whereClause = new Function(eqImpl.info(), Arrays.<Symbol>asList(name_ref, Literal.newLiteral("Marvin")));

        ESDeleteByQueryNode deleteByQueryNode = new ESDeleteByQueryNode(
                new String[]{characters.name()},
                new WhereClause(whereClause));

        BytesReference reference = generator.convert(deleteByQueryNode);
        String actual = reference.toUtf8();
        assertThat(actual, is("{\"query\":{\"term\":{\"name\":\"Marvin\"}}}"));
    }

    @Test
    public void testSelect_OnlyVersion() throws Exception {
        Reference version_ref = createReference("_version", DataTypes.INTEGER);
        QueryThenFetchNode searchNode = new QueryThenFetchNode(
                new Routing(),
                ImmutableList.<Symbol>of(version_ref),
                null,
                null,
                null,
                null,
                null,
                WhereClause.MATCH_ALL,
                null);

        BytesReference reference = generator.convert(searchNode);
        String actual = reference.toUtf8();
        assertThat(actual, is(
                "{\"version\":true,\"_source\":false,\"query\":{\"match_all\":{}},\"from\":0,\"size\":10000}"));
    }

    @Test
    public void testSelect_WholeObjectAndPartial() throws Exception {
        Reference author = createReference("author", DataTypes.OBJECT);
        Reference age = createReference(
                ColumnIdent.getChild(author.info().ident().columnIdent(), "age"), DataTypes.INTEGER);

        QueryThenFetchNode searchNode = new QueryThenFetchNode(
                new Routing(),
                ImmutableList.<Symbol>of(author, age),
                null,
                null,
                null,
                null,
                null,
                WhereClause.MATCH_ALL,
                null);

        BytesReference reference = generator.convert(searchNode);
        String actual = reference.toUtf8();
        assertThat(actual, is(
                "{\"_source\":{\"include\":[\"author\"]},\"query\":{\"match_all\":{}},\"from\":0,\"size\":10000}"));
    }

    @Test
    public void testSelect_excludePartitionedColumns() throws Exception {
        PartitionName partitionName = new PartitionName(characters.name(), Arrays.asList(new BytesRef("0.5")));
        QueryThenFetchNode searchNode = new QueryThenFetchNode(
                new Routing(),
                ImmutableList.<Symbol>of(name_ref, weight_ref),
                null,
                null,
                null,
                null,
                null,
                WhereClause.MATCH_ALL,
                Arrays.asList(weight_ref.info()));

        BytesReference reference = generator.convert(searchNode);
        String actual = reference.toUtf8();
        assertThat(actual, is(
                "{\"_source\":{\"include\":[\"name\"]},\"query\":{\"match_all\":{}},\"from\":0,\"size\":10000}"));
    }


    @Test
    public void testAnyGreater() throws Exception {
        // 0.0 < ANY_OF (d_array)

        DataType doubleArrayType = new ArrayType(DataTypes.DOUBLE);
        Reference doubleArrayRef = createReference("d_array", doubleArrayType);
        FunctionImplementation anyGreaterImpl = functions.get(new FunctionIdent("any_>",
                Arrays.<DataType>asList(doubleArrayType, DataTypes.DOUBLE)));

        Function whereClause = new Function(anyGreaterImpl.info(), Arrays.<Symbol>asList(doubleArrayRef, Literal.newLiteral(0.0)));

        xcontentAssert(whereClause, "{\"query\":{\"range\":{\"d_array\":{\"gt\":0.0}}}}");
    }

    @Test
    public void testAnyGreaterEquals() throws Exception {
        // 0.0 <= ANY_OF (d_array)

        DataType doubleArrayType = new ArrayType(DataTypes.DOUBLE);
        Reference doubleArrayRef = createReference("d_array", doubleArrayType);
        FunctionImplementation anyGreaterImpl = functions.get(new FunctionIdent("any_>=",
                Arrays.<DataType>asList(doubleArrayType, DataTypes.DOUBLE)));

        Function whereClause = new Function(anyGreaterImpl.info(),
                Arrays.<Symbol>asList(doubleArrayRef, Literal.newLiteral(0.0)));

        xcontentAssert(whereClause, "{\"query\":{\"range\":{\"d_array\":{\"gte\":0.0}}}}");
    }

    @Test
    public void testDistanceGteQuery() throws Exception {
        Function distanceFunction = new Function(
                new FunctionInfo(
                        new FunctionIdent(DistanceFunction.NAME, Arrays.<DataType>asList(DataTypes.GEO_POINT, DataTypes.GEO_POINT)),
                        DataTypes.DOUBLE),
                Arrays.<Symbol>asList(
                        createReference("location", DataTypes.GEO_POINT),
                        Literal.newLiteral(DataTypes.GEO_POINT, DataTypes.GEO_POINT.value("POINT (10 20)"))
                )
        );
        Function whereClause = new Function(
                new FunctionInfo(
                        new FunctionIdent(GteOperator.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE, DataTypes.DOUBLE)),
                        DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(distanceFunction, Literal.newLiteral(20.0d))
        );
        xcontentAssert(whereClause,
                "{\"query\":{\"filtered\":{\"query\":{\"match_all\":{}},\"filter\":{\"geo_distance_range\":{\"location\":[10.0,20.0],\"gte\":20.0}}}}}");
    }

    @Test
    public void testDistanceGteQuerySwappedArgs() throws Exception {
        Function distanceFunction = new Function(
                new FunctionInfo(
                        new FunctionIdent(DistanceFunction.NAME, Arrays.<DataType>asList(DataTypes.GEO_POINT, DataTypes.GEO_POINT)),
                        DataTypes.DOUBLE),
                Arrays.<Symbol>asList(
                        Literal.newLiteral(DataTypes.GEO_POINT, DataTypes.GEO_POINT.value("POINT (10 20)")),
                        createReference("location", DataTypes.GEO_POINT)
                )
        );
        Function whereClause = new Function(
                new FunctionInfo(
                        new FunctionIdent(GteOperator.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE, DataTypes.DOUBLE)),
                        DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(Literal.newLiteral(20.d), distanceFunction)
        );
        xcontentAssert(whereClause,
                "{\"query\":{\"filtered\":{\"query\":{\"match_all\":{}},\"filter\":{\"geo_distance_range\":{\"location\":[10.0,20.0],\"gte\":20.0}}}}}");
    }

    @Test
    public void testDistanceEqQuery() throws Exception {
        Function distanceFunction = new Function(
                new FunctionInfo(
                        new FunctionIdent(DistanceFunction.NAME, Arrays.<DataType>asList(DataTypes.GEO_POINT, DataTypes.GEO_POINT)),
                        DataTypes.DOUBLE),
                Arrays.<Symbol>asList(
                        createReference("location", DataTypes.GEO_POINT),
                        Literal.newLiteral(DataTypes.GEO_POINT, DataTypes.GEO_POINT.value("POINT (10 20)"))
                )
        );
        Function whereClause = new Function(
                new FunctionInfo(
                        new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE, DataTypes.DOUBLE)),
                        DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(distanceFunction, Literal.newLiteral(20.0d))
        );
        xcontentAssert(whereClause,
                "{\"query\":{\"filtered\":{\"query\":{\"match_all\":{}},\"filter\":{\"geo_distance_range\":{\"location\":[10.0,20.0],\"from\":20.0,\"to\":20.0,\"include_upper\":true,\"include_lower\":true}}}}}");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testWhereDistanceFunctionEqDistanceFunction() throws Exception {
        /**
         * distance(p, ...) = distance(p, ...) isn't supported
         */
        Function distanceFunction = new Function(
                new FunctionInfo(
                        new FunctionIdent(DistanceFunction.NAME, Arrays.<DataType>asList(DataTypes.GEO_POINT, DataTypes.GEO_POINT)),
                        DataTypes.DOUBLE),
                Arrays.<Symbol>asList(
                        createReference("location", DataTypes.GEO_POINT),
                        Literal.newLiteral(DataTypes.GEO_POINT, DataTypes.GEO_POINT.value("POINT (10 20)"))
                )
        );
        Function whereClause = new Function(
                new FunctionInfo(
                        new FunctionIdent(GteOperator.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE, DataTypes.DOUBLE)),
                        DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(distanceFunction, distanceFunction)
        );
        generator.convert(new WhereClause(whereClause));
    }

    @Test
    public void testConvertESSearchNodeWithOrderByDistance() throws Exception {
        Function distanceFunction = new Function(
                new FunctionInfo(
                        new FunctionIdent(DistanceFunction.NAME, Arrays.<DataType>asList(DataTypes.GEO_POINT, DataTypes.GEO_POINT)),
                        DataTypes.DOUBLE),
                Arrays.<Symbol>asList(
                        createReference("location", DataTypes.GEO_POINT),
                        Literal.newLiteral(DataTypes.GEO_POINT, DataTypes.GEO_POINT.value("POINT (10 20)"))
                )
        );
        QueryThenFetchNode searchNode = new QueryThenFetchNode(
                new Routing(),
                ImmutableList.<Symbol>of(name_ref),
                ImmutableList.<Symbol>of(distanceFunction),
                new boolean[] { false },
                new Boolean[] { null },
                null,
                null,
                WhereClause.MATCH_ALL,
                null);
        BytesReference reference = generator.convert(searchNode);
        String actual = reference.toUtf8();
        assertThat(actual, is(
                "{\"_source\":{\"include\":[\"name\"]},\"query\":{\"match_all\":{}},\"from\":0,\"size\":10000}"));
    }

    @Test
    public void testConvertESSearchNodeWithOrderByScalar() throws Exception {
        Function scalarFunction = new Function(
                new FunctionInfo(
                        new FunctionIdent(RoundFunction.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE)),
                        DataTypes.LONG),
                Arrays.<Symbol>asList(createReference("price", DataTypes.DOUBLE))
        );
        QueryThenFetchNode searchNode = new QueryThenFetchNode(
                new Routing(),
                ImmutableList.<Symbol>of(name_ref),
                ImmutableList.<Symbol>of(scalarFunction),
                new boolean[] { false },
                new Boolean[] { null },
                null,
                null,
                WhereClause.MATCH_ALL,
                null);
        BytesReference reference = generator.convert(searchNode);
        String actual = reference.toUtf8();
        assertThat(actual, is(
                "{\"_source\":{\"include\":[\"name\"]},\"query\":{\"match_all\":{}}," +
                        "\"from\":0,\"size\":10000}"));
    }

    @Test
    public void testWhereClauseWithWithinPolygonQuery() throws Exception {
        Function withinFunction = createFunction(
                WithinFunction.NAME,
                DataTypes.BOOLEAN,
                createReference("location", DataTypes.GEO_POINT),
                Literal.newGeoShape("POLYGON (( 5 5, 30 5, 30 30, 5 35, 5 5 ))")
        );
        xcontentAssert(withinFunction,
                "{\"query\":{\"filtered\":{\"query\":{\"match_all\":{}},\"filter\":{\"geo_polygon\":{\"location\":{\"points\":[{\"lon\":5.0,\"lat\":5.0},{\"lon\":30.0,\"lat\":5.0},{\"lon\":30.0,\"lat\":30.0},{\"lon\":5.0,\"lat\":35.0},{\"lon\":5.0,\"lat\":5.0}]}}}}}}");
    }

    @Test
    public void testWhereClauseWithWithinRectangleQuery() throws Exception {
        Function withinFunction = createFunction(
                WithinFunction.NAME,
                DataTypes.BOOLEAN,
                createReference("location", DataTypes.GEO_POINT),
                Literal.newGeoShape("POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))")
        );
        xcontentAssert(withinFunction,
                "{\"query\":{\"filtered\":{\"query\":{\"match_all\":{}},\"filter\":{\"geo_bounding_box\":{\"location\":{\"top_left\":{\"lon\":5.0,\"lat\":30.0},\"bottom_right\":{\"lon\":30.0,\"lat\":5.0}}}}}}}");
    }

    @Test
    public void testWhereClauseWithWithinPolygonEqualsTrueQuery() throws Exception {
        Function withinFunction = createFunction(
                WithinFunction.NAME,
                DataTypes.BOOLEAN,
                createReference("location", DataTypes.GEO_POINT),
                Literal.newGeoShape("POLYGON (( 5 5, 30 5, 30 30, 5 35, 5 5 ))")
        );
        Function eqFunction = createFunction(
                EqOperator.NAME,
                DataTypes.BOOLEAN,
                Literal.newLiteral(true),
                withinFunction
        );
        xcontentAssert(eqFunction,
                "{\"query\":{\"filtered\":{\"query\":{\"match_all\":{}},\"filter\":{\"geo_polygon\":{\"location\":{\"points\":[{\"lon\":5.0,\"lat\":5.0},{\"lon\":30.0,\"lat\":5.0},{\"lon\":30.0,\"lat\":30.0},{\"lon\":5.0,\"lat\":35.0},{\"lon\":5.0,\"lat\":5.0}]}}}}}}");
    }

    @Test
    public void testWhereClauseWithWithinEqualsFalseQuery() throws Exception {
        Function withinFunction = createFunction(
                WithinFunction.NAME,
                DataTypes.BOOLEAN,
                createReference("location", DataTypes.GEO_POINT),
                Literal.newGeoShape("POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))")
        );
        Function eqFunction = createFunction(
                EqOperator.NAME,
                DataTypes.BOOLEAN,
                withinFunction,
                Literal.newLiteral(false)
        );
        xcontentAssert(eqFunction,
                "{\"query\":{\"bool\":{\"must_not\":{\"filtered\":{\"query\":{\"match_all\":{}},\"filter\":{\"geo_bounding_box\":{\"location\":{\"top_left\":{\"lon\":5.0,\"lat\":30.0},\"bottom_right\":{\"lon\":30.0,\"lat\":5.0}}}}}}}}}");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testWithinEqWithin() throws Exception {
        Function withinFunction = createFunction(
                WithinFunction.NAME,
                DataTypes.BOOLEAN,
                createReference("location", DataTypes.GEO_POINT),
                Literal.newGeoShape("POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))")
        );
        Function eqFunction = createFunction(
                EqOperator.NAME,
                DataTypes.BOOLEAN,
                withinFunction,
                withinFunction
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
    public void testWhereNumericScalarEqNumericScalar() throws Exception {
        /**
         * round(a) = round(b) isn't supported
         */
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
                Arrays.<Symbol>asList(scalarFunction, scalarFunction)
        );


        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Can't compare two scalar functions");
        generator.convert(new WhereClause(whereClause));
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

}

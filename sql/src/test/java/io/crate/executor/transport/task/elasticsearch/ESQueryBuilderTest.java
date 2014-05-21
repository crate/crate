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
import io.crate.analyze.WhereClause;
import io.crate.metadata.*;
import io.crate.operation.operator.*;
import io.crate.operation.predicate.IsNullPredicate;
import io.crate.operation.predicate.NotPredicate;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.MatchFunction;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dml.ESDeleteByQueryNode;
import io.crate.planner.node.dql.ESSearchNode;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.testing.TestingHelpers;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ESQueryBuilderTest {

    Functions functions;
    static TableIdent characters = new TableIdent(null, "characters");
    static Reference name_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "name"), RowGranularity.DOC, DataTypes.STRING));
    static Reference age_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "age"), RowGranularity.DOC, DataTypes.INTEGER));
    static Reference weight_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "weight"), RowGranularity.DOC, DataTypes.DOUBLE));
    static Reference float_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "float_ref"), RowGranularity.DOC, DataTypes.FLOAT));
    static Reference long_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "long_ref"), RowGranularity.DOC, DataTypes.LONG));
    static Reference short_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "short_ref"), RowGranularity.DOC, DataTypes.SHORT));
    static Reference isParanoid = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "isParanoid"), RowGranularity.DOC, DataTypes.BOOLEAN));
    static Reference extrafield = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "extrafield"), RowGranularity.DOC, DataTypes.STRING));
    private ESQueryBuilder generator;

    private List<DataType> typeX2(DataType type) {
        return Arrays.asList(type, type);
    }


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
                MatchFunction.NAME, ImmutableList.<DataType>of(DataTypes.STRING, DataTypes.STRING));
        FunctionImplementation matchImpl = functions.get(functionIdent);
        Function match = new Function(matchImpl.info(),
                Arrays.<Symbol>asList(name_ref, Literal.newLiteral("arthur")));

        xcontentAssert(match, "{\"query\":{\"match\":{\"name\":\"arthur\"}}}");
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
        ESSearchNode node = new ESSearchNode(new String[]{"something"},
                ImmutableList.<Symbol>of(), null, null, null, null, null, new WhereClause(whereClause), null);
        BytesReference bytesReference = generator.convert(node);

        assertThat(bytesReference.toUtf8(),
                is("{\"_source\":false,\"query\":{\"match_all\":{}},\"min_score\":0.4,\"from\":0,\"size\":10000}"));
    }

    @Test
    public void testConvertESSearchNode() throws Exception {
        FunctionImplementation eqImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataTypes.STRING)));
        Function whereClause = new Function(eqImpl.info(), Arrays.<Symbol>asList(name_ref, Literal.newLiteral("Marvin")));

        ESSearchNode searchNode = new ESSearchNode(
                new String[]{characters.name()},
                ImmutableList.<Symbol>of(name_ref),
                ImmutableList.<Reference>of(),
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
                TestingHelpers.createReference("_version", DataTypes.INTEGER),
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
        Reference version_ref = TestingHelpers.createReference("_version", DataTypes.INTEGER);
        ESSearchNode searchNode = new ESSearchNode(
                new String[]{characters.name()},
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
    public void testCommonAncestors() throws Exception {
        assertEquals(ImmutableSet.of("a"), ESQueryBuilder.commonAncestors(Arrays.asList("a", "a.b")));

        assertEquals(ImmutableSet.of("d", "a", "b"),
                ESQueryBuilder.commonAncestors(Arrays.asList("a.c", "b", "b.c.d", "a", "a.b", "d")));

        assertEquals(ImmutableSet.of("d", "a", "b.c"),
                ESQueryBuilder.commonAncestors(Arrays.asList("a.c", "b.c", "b.c.d", "a", "a.b", "d")));


    }

    @Test
    public void testSelect_WholeObjectAndPartial() throws Exception {
        Reference author = TestingHelpers.createReference("author", DataTypes.OBJECT);
        Reference age = TestingHelpers.createReference(
                ColumnIdent.getChild(author.info().ident().columnIdent(), "age"), DataTypes.INTEGER);

        ESSearchNode searchNode = new ESSearchNode(
                new String[]{characters.name()},
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
        PartitionName partitionName = new PartitionName(characters.name(), Arrays.asList("0.5"));
        ESSearchNode searchNode = new ESSearchNode(
                new String[]{partitionName.stringValue()},
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
        Reference doubleArrayRef = TestingHelpers.createReference("d_array", doubleArrayType);
        FunctionImplementation anyGreaterImpl = functions.get(new FunctionIdent("any_>",
                Arrays.<DataType>asList(doubleArrayType, DataTypes.DOUBLE)));

        Function whereClause = new Function(anyGreaterImpl.info(), Arrays.<Symbol>asList(doubleArrayRef, Literal.newLiteral(0.0)));

        xcontentAssert(whereClause, "{\"query\":{\"range\":{\"d_array\":{\"gt\":0.0}}}}");
    }

    @Test
    public void testAnyGreaterEquals() throws Exception {
        // 0.0 <= ANY_OF (d_array)

        DataType doubleArrayType = new ArrayType(DataTypes.DOUBLE);
        Reference doubleArrayRef = TestingHelpers.createReference("d_array", doubleArrayType);
        FunctionImplementation anyGreaterImpl = functions.get(new FunctionIdent("any_>=",
                Arrays.<DataType>asList(doubleArrayType, DataTypes.DOUBLE)));

        Function whereClause = new Function(anyGreaterImpl.info(),
                Arrays.<Symbol>asList(doubleArrayRef, Literal.newLiteral(0.0)));

        xcontentAssert(whereClause, "{\"query\":{\"range\":{\"d_array\":{\"gte\":0.0}}}}");
    }
}

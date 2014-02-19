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

package io.crate.analyze.elasticsearch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.WhereClause;
import io.crate.metadata.*;
import io.crate.operator.operator.*;
import io.crate.operator.predicate.IsNullPredicate;
import io.crate.operator.predicate.NotPredicate;
import io.crate.operator.predicate.PredicateModule;
import io.crate.operator.scalar.MatchFunction;
import io.crate.operator.scalar.ScalarFunctionModule;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.ESDeleteByQueryNode;
import io.crate.planner.node.ESSearchNode;
import io.crate.planner.symbol.*;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
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
import static org.junit.Assert.*;

public class ESQueryBuilderTest {

    Functions functions;
    static TableIdent characters = new TableIdent(null, "characters");
    static Reference name_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "name"), RowGranularity.DOC, DataType.STRING));
    static Reference age_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "age"), RowGranularity.DOC, DataType.INTEGER));
    static Reference weight_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "weight"), RowGranularity.DOC, DataType.DOUBLE));
    static Reference float_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "float_ref"), RowGranularity.DOC, DataType.FLOAT));
    static Reference long_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "long_ref"), RowGranularity.DOC, DataType.LONG));
    static Reference short_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "short_ref"), RowGranularity.DOC, DataType.SHORT));
    static Reference isParanoid = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "isParanoid"), RowGranularity.DOC, DataType.BOOLEAN));
    static Reference extrafield = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "extrafield"), RowGranularity.DOC, DataType.STRING));
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
        generator = new ESQueryBuilder(functions, null);
    }

    private void xcontetAssert(Function whereClause, String expected) throws IOException {
        BytesReference reference = generator.convert(new WhereClause(whereClause));
        String actual = reference.toUtf8();
        assertThat(actual, is(expected));
    }

    @Test
    public void testConvertNestedAnd() throws Exception {
        FunctionImplementation eqStringImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataType.STRING)));
        FunctionImplementation eqAgeImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataType.INTEGER)));
        FunctionImplementation eqLongImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataType.LONG)));
        FunctionImplementation andImpl = functions.get(new FunctionIdent(AndOperator.NAME, typeX2(DataType.BOOLEAN)));

        Function eqName = new Function(eqStringImpl.info(), Arrays.<Symbol>asList(name_ref, new StringLiteral("Marvin")));
        Function eqAge = new Function(eqAgeImpl.info(), Arrays.<Symbol>asList(age_ref, new IntegerLiteral(84)));
        Function eqLong = new Function(eqLongImpl.info(), Arrays.<Symbol>asList(long_ref, new LongLiteral(8L)));

        Function rightAnd = new Function(andImpl.info(), Arrays.<Symbol>asList(eqAge, eqLong));
        Function leftAnd = new Function(andImpl.info(), Arrays.<Symbol>asList(eqName, rightAnd));

        xcontetAssert(leftAnd, "{\"query\":{\"bool\":{\"must\":[{\"term\":{\"name\":\"Marvin\"}},{\"bool\":{\"must\":[{\"term\":{\"age\":84}},{\"term\":{\"long_ref\":8}}]}}]}}}");
    }


    @Test
    public void testWhereWithOr() throws Exception {
        // where name = marvin and age = 84 and longField = 8

        FunctionImplementation eqStringImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataType.STRING)));
        FunctionImplementation orImpl = functions.get(new FunctionIdent(OrOperator.NAME, typeX2(DataType.BOOLEAN)));

        Function eqMarvin = new Function(eqStringImpl.info(), Arrays.<Symbol>asList(name_ref, new StringLiteral("Marvin")));
        Function eqTrillian = new Function(eqStringImpl.info(), Arrays.<Symbol>asList(name_ref, new StringLiteral("Trillian")));

        Function whereClause = new Function(orImpl.info(), Arrays.<Symbol>asList(eqMarvin, eqTrillian));

        xcontetAssert(whereClause, "{\"query\":{\"bool\":{\"minimum_should_match\":1,\"should\":[{\"term\":{\"name\":\"Marvin\"}},{\"term\":{\"name\":\"Trillian\"}}]}}}");
    }

    @Test
    public void testWhereReferenceEqStringLiteral() throws Exception {
        FunctionImplementation eqImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataType.STRING)));
        Function whereClause = new Function(eqImpl.info(), Arrays.<Symbol>asList(name_ref, new StringLiteral("Marvin")));

        xcontetAssert(whereClause, "{\"query\":{\"term\":{\"name\":\"Marvin\"}}}");
    }


    @Test
    public void testWhereReferenceNotEqStringLiteral() throws Exception {
        FunctionImplementation impl = functions.get(new FunctionIdent(NotEqOperator.NAME, typeX2(DataType.STRING)));
        Function whereClause = new Function(impl.info(), Arrays.<Symbol>asList(name_ref, new StringLiteral("Marvin")));
        xcontetAssert(whereClause, "{\"query\":{\"bool\":{\"must_not\":{\"term\":{\"name\":\"Marvin\"}}}}}");
    }

    @Test
    public void testWhereReferenceEqIntegerLiteral() throws Exception {
        FunctionImplementation eqImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataType.INTEGER)));
        Function whereClause = new Function(eqImpl.info(), Arrays.<Symbol>asList(age_ref, new IntegerLiteral(40)));
        xcontetAssert(whereClause, "{\"query\":{\"term\":{\"age\":40}}}");
    }

    @Test
    public void testWhereReferenceLtDoubleLiteral() throws Exception {
        FunctionImplementation ltImpl = functions.get(new FunctionIdent(LtOperator.NAME, typeX2(DataType.DOUBLE)));
        Function whereClause = new Function(ltImpl.info(), Arrays.<Symbol>asList(weight_ref, new DoubleLiteral(54.3)));
        xcontetAssert(whereClause, "{\"query\":{\"range\":{\"weight\":{\"lt\":54.3}}}}");
    }

    @Test
    public void testWhereReferenceLteFloatLiteral() throws Exception {
        FunctionImplementation impl = functions.get(new FunctionIdent(LteOperator.NAME, typeX2(DataType.FLOAT)));
        Function whereClause = new Function(impl.info(), Arrays.<Symbol>asList(float_ref, new FloatLiteral(42.1)));
        xcontetAssert(whereClause, "{\"query\":{\"range\":{\"float_ref\":{\"lte\":42.1}}}}");
    }

    @Test
    public void testWhereReferenceGtLong() throws Exception {
        FunctionImplementation impl = functions.get(new FunctionIdent(GtOperator.NAME, typeX2(DataType.LONG)));
        Function whereClause = new Function(impl.info(), Arrays.<Symbol>asList(long_ref, new LongLiteral(8L)));
        xcontetAssert(whereClause, "{\"query\":{\"range\":{\"long_ref\":{\"gt\":8}}}}");
    }

    @Test
    public void testWhereReferenceEqShort() throws Exception {
        FunctionImplementation impl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataType.SHORT)));
        Function whereClause = new Function(impl.info(), Arrays.<Symbol>asList(short_ref, Literal.forType(DataType.SHORT, (short)2)));
        xcontetAssert(whereClause, "{\"query\":{\"term\":{\"short_ref\":2}}}");
    }

    @Test
    public void testWhereReferenceEqBoolean() throws Exception {
        FunctionImplementation impl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(isParanoid.valueType())));
        Function whereClause = new Function(impl.info(),
                Arrays.<Symbol>asList(isParanoid, Literal.forType(isParanoid.valueType(), true)));

        xcontetAssert(whereClause, "{\"query\":{\"term\":{\"isParanoid\":true}}}");
    }

    @Test
    public void testWhereReferenceLikeString() throws Exception {
        FunctionImplementation impl = functions.get(new FunctionIdent(LikeOperator.NAME, typeX2(name_ref.valueType())));
        Function whereClause = new Function(impl.info(),
                Arrays.<Symbol>asList(name_ref, new StringLiteral("%thu%")));
        xcontetAssert(whereClause, "{\"query\":{\"wildcard\":{\"name\":\"*thu*\"}}}");
    }

    @Test
    public void testWhereNotReferenceLikeString() throws Exception {
        FunctionImplementation notOp = functions.get(new FunctionIdent(NotPredicate.NAME, Arrays.asList(DataType.BOOLEAN)));
        FunctionImplementation likeOp = functions.get(new FunctionIdent(LikeOperator.NAME, typeX2(name_ref.valueType())));

        Function likeClause = new Function(likeOp.info(),
                Arrays.<Symbol>asList(name_ref, new StringLiteral("%thu%")));
        Function whereClause = new Function(notOp.info(), Arrays.<Symbol>asList(likeClause));
        xcontetAssert(whereClause, "{\"query\":{\"bool\":{\"must_not\":{\"wildcard\":{\"name\":\"*thu*\"}}}}}");
    }

    @Test
    public void testWhereReferenceIsNull() throws Exception {
        FunctionImplementation isNullImpl = functions.get(
                new FunctionIdent(IsNullPredicate.NAME, Arrays.asList(extrafield.valueType())));

        Function isNull = new Function(isNullImpl.info(), Arrays.<Symbol>asList(extrafield));
        xcontetAssert(isNull, "{\"query\":{\"filtered\":{\"filter\":{\"missing\":{\"field\":\"extrafield\",\"existence\":true,\"null_value\":true}}}}}");
    }

    @Test
    public void testWhereReferenceInStringList() throws Exception {
        // where name in ("alpha", "bravo", "charlie")
        Reference ref = name_ref;
        FunctionImplementation inListImpl = functions.get(
                new FunctionIdent(InOperator.NAME,
                Arrays.asList(DataType.STRING, DataType.STRING_SET))
        );

        ImmutableSet<BytesRef> list = ImmutableSet.of(
                new BytesRef("alpha"), new BytesRef("bravo"), new BytesRef("charlie"));
        SetLiteral set = new SetLiteral(DataType.STRING, list);
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
                MatchFunction.NAME, ImmutableList.of(DataType.STRING, DataType.STRING));
        FunctionImplementation matchImpl = functions.get(functionIdent);
        Function match = new Function(matchImpl.info(),
                Arrays.<Symbol>asList(name_ref, new StringLiteral("arthur")));

        xcontetAssert(match, "{\"query\":{\"match\":{\"name\":\"arthur\"}}}");
    }

    @Test
    public void testMinScoreIsSet() throws Exception {
        Reference minScore_ref = new Reference(
                new ReferenceInfo(new ReferenceIdent(null, "_score"), RowGranularity.DOC, DataType.DOUBLE));

        Function whereClause = new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.asList(DataType.DOUBLE, DataType.DOUBLE)),
                DataType.BOOLEAN),
                Arrays.<Symbol>asList(minScore_ref, new DoubleLiteral(0.4))
        );
        ESSearchNode node = new ESSearchNode(
                ImmutableList.<Symbol>of(), null, null, null, null, new WhereClause(whereClause));
        BytesReference bytesReference = generator.convert(node, ImmutableList.<Reference>of());

        assertThat(bytesReference.toUtf8(),
                is("{\"fields\":[],\"query\":{\"match_all\":{}},\"min_score\":0.4,\"from\":0,\"size\":10000}"));
    }

    @Test
    public void testConvertESSearchNode() throws Exception {
        FunctionImplementation eqImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataType.STRING)));
        Function whereClause = new Function(eqImpl.info(), Arrays.<Symbol>asList(name_ref, new StringLiteral("Marvin")));

        ESSearchNode searchNode = new ESSearchNode(ImmutableList.<Symbol>of(name_ref),
                ImmutableList.<Reference>of(),
                new boolean[0],
                null,
                null,
                new WhereClause(whereClause));

        BytesReference reference = generator.convert(searchNode, ImmutableList.<Reference>of(name_ref));
        String actual = reference.toUtf8();
        assertThat(actual, is("{\"fields\":[\"name\"],\"query\":{\"term\":{\"name\":\"Marvin\"}},\"from\":0,\"size\":10000}"));
    }

    @Test (expected = UnsupportedOperationException.class)
    public void testQueryWith_Version() throws Exception {
        FunctionImplementation eqImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataType.STRING)));
        Function whereClause = new Function(eqImpl.info(), Arrays.<Symbol>asList(
                TestingHelpers.createReference("_version", DataType.INTEGER),
                new IntegerLiteral(4)));

        generator.convert(new WhereClause(whereClause));
    }

    @Test
    public void testConvertESDeleteByQueryNode() throws Exception {
        FunctionImplementation eqImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataType.STRING)));
        Function whereClause = new Function(eqImpl.info(), Arrays.<Symbol>asList(name_ref, new StringLiteral("Marvin")));

        ESDeleteByQueryNode deleteByQueryNode = new ESDeleteByQueryNode(
                ImmutableSet.<String>of(characters.name()),
                new WhereClause(whereClause));

        BytesReference reference = generator.convert(deleteByQueryNode);
        String actual = reference.toUtf8();
        assertThat(actual, is("{\"term\":{\"name\":\"Marvin\"}}"));

    }
}

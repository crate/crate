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

package io.crate.operation.aggregation;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import io.crate.Streamer;
import io.crate.execution.engine.aggregation.impl.HyperLogLogPlusPlus;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.SearchPath;
import io.crate.metadata.functions.Signature;
import io.crate.module.ExtraFunctionsModule;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.hamcrest.Matchers.is;

public class HyperLogLogDistinctAggregationTest extends AggregationTestCase {

    @Before
    public void prepareFunctions() throws Exception {
        nodeCtx = createNodeContext(new ExtraFunctionsModule());
    }

    private Object executeAggregation(DataType<?> argumentType, Object[][] data) throws Exception {
        return executeAggregation(
            Signature.aggregate(
                HyperLogLogDistinctAggregation.NAME,
                argumentType.getTypeSignature(),
                DataTypes.LONG.getTypeSignature()
            ),
            data,
            List.of()
        );
    }

    private Object executeAggregationWithPrecision(DataType<?> argumentType, Object[][] data, List<Literal<?>> optionalParams) throws Exception {
        return executeAggregation(
            Signature.aggregate(
                HyperLogLogDistinctAggregation.NAME,
                argumentType.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.LONG.getTypeSignature()
            ),
            data,
            optionalParams
        );
    }

    private <T> Object[][] createTestData(int numRows, Function<Integer, T> generator, @Nullable Integer precision) {
        Object[][] data = new Object[numRows][];
        for (int i = 0; i < numRows; i++) {
            if (precision != null) {
                data[i] = new Object[]{generator.apply(i), precision};
            } else {
                data[i] = new Object[]{generator.apply(i)};
            }
        }
        return data;
    }

    @Test
    public void test_function_implements_doc_values_aggregator_for_string_based_types() {
        for (var dataType : List.of(DataTypes.STRING, DataTypes.IP)) {
            assertHasDocValueAggregator(HyperLogLogDistinctAggregation.NAME, List.of(dataType));
        }
    }

    @Test
    public void testReturnTypeIsAlwaysLong() {
        // Return type is fixed to Long
        FunctionImplementation func = nodeCtx.functions().get(
            null,
            HyperLogLogDistinctAggregation.NAME,
            List.of(Literal.of(1)),
            SearchPath.pathWithPGCatalogAndDoc()
        );
        assertEquals(DataTypes.LONG, func.info().returnType());
        func = nodeCtx.functions().get(
            null,
            HyperLogLogDistinctAggregation.NAME,
            List.of(Literal.of(1), Literal.of(2)),
            SearchPath.pathWithPGCatalogAndDoc()
        );
        assertEquals(DataTypes.LONG, func.info().returnType());
    }

    @Test
    public void testCallWithInvalidPrecisionResultsInAnError() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("precision must be >= 4 and <= 18");
        executeAggregationWithPrecision(DataTypes.INTEGER, new Object[][]{{4, 1}}, List.of(Literal.of(1)));
    }

    @Test
    public void testWithoutPrecision() throws Exception {
        Object result = executeAggregation(DataTypes.DOUBLE, createTestData(10_000, i -> i, null));
        assertThat(result, is(9899L));
    }

    @Test
    public void test_without_precision_string() throws Exception {
        Object result = executeAggregation(DataTypes.STRING, new Object[][]{{"Youri"}, {"Ruben"}, {"Ruben"}});
        assertThat(result, is(2L));
    }

    @Test
    public void test_without_precision_long() throws Exception {
        // Dedicated test case to prove that integral and floating types need different implementations.
        // values.nextValue and NumericUtils.sortableDoubleBits(values.nextValue) return same result in many cases.
        // They return different result for Long.MIN_VALUE (and all close numbers with same 63-th bit)
        // but providing 1 Long.MIN_VALUE as input is still not enough.
        // The difference is revealed only when input data consists of many tricky values i.e values, close to Long.MIN_VALUE.
        Object result = executeAggregation(DataTypes.LONG, createTestData(10_000, i -> Long.MIN_VALUE + i, null));
        assertThat(result, is(9925L));
    }

    @Test
    public void test_random_type_random_values() throws Exception {
        var validTypes =  DataTypes.PRIMITIVE_TYPES.stream()
            .filter(x -> x.storageSupport() != null)
            .collect(Collectors.toList());
        var type = RandomPicks.randomFrom(RandomizedContext.current().getRandom(), validTypes);

        var data = TestingHelpers.getRandomsOfType(10000, 10000, type)
            .stream()
            .map(val -> new Object[]{val})
            .toArray(size -> new Object[size][1]);

        // No explicit assertions here as the purpose of that test to check random type/value of regular and docValue aggregations.
        // Needed assertions executed internally in executeAggregation.
        executeAggregation(type, data);
    }

    @Test
    public void testWithPrecision() throws Exception {
        int precision = 18;
        // Precision in createTestData is used to create function arguments for regular aggregation.
        // Literal.of parameter is used to create function for DocValueAggregation - otherwise it gets ignored.
        Object result = executeAggregationWithPrecision(DataTypes.DOUBLE,
            createTestData(10_000, i -> i, precision),
            List.of(Literal.of(precision))
        );
        assertThat(result, is(9997L));
    }

    @Test
    public void testMurmur3HashCalculationsForAllTypes() throws Exception {
        // double types
        assertThat(HyperLogLogDistinctAggregation.Murmur3Hash.getForType(DataTypes.DOUBLE, true).hash(1.3d),
            is(3706823019612663850L));
        assertThat(HyperLogLogDistinctAggregation.Murmur3Hash.getForType(DataTypes.FLOAT, true).hash(1.3f),
            is(1386670595997310747L));

        // long types
        assertThat(HyperLogLogDistinctAggregation.Murmur3Hash.getForType(DataTypes.LONG, true).hash(1L),
            is(-2508561340476696217L));
        assertThat(HyperLogLogDistinctAggregation.Murmur3Hash.getForType(DataTypes.INTEGER, true).hash(1),
            is(-2508561340476696217L));
        assertThat(HyperLogLogDistinctAggregation.Murmur3Hash.getForType(DataTypes.SHORT, true).hash(Short.valueOf("1")),
            is(-2508561340476696217L));
        assertThat(HyperLogLogDistinctAggregation.Murmur3Hash.getForType(DataTypes.BYTE, true).hash(Byte.valueOf("1")),
            is(-2508561340476696217L));
        assertThat(HyperLogLogDistinctAggregation.Murmur3Hash.getForType(DataTypes.TIMESTAMPZ, true).hash(1512569562000L),
            is(-3066297687939346384L));

        // bytes types
        assertThat(HyperLogLogDistinctAggregation.Murmur3Hash.getForType(DataTypes.STRING, true).hash("foo"),
            is(1208210750032620489L));
        assertThat(HyperLogLogDistinctAggregation.Murmur3Hash.getForType(DataTypes.BOOLEAN, true).hash(true),
            is(4312328700069294139L));

        // ip type
        assertThat(HyperLogLogDistinctAggregation.Murmur3Hash.getForType(DataTypes.IP, true).hash("127.0.0.1"),
            is(6044143379282500354L));
    }

    @Test
    public void testStreaming() throws Exception {
        HyperLogLogDistinctAggregation.HllState hllState1 = new HyperLogLogDistinctAggregation.HllState(DataTypes.IP, true);
        hllState1.init(memoryManager, HyperLogLogPlusPlus.DEFAULT_PRECISION);
        BytesStreamOutput out = new BytesStreamOutput();
        Streamer streamer = HyperLogLogDistinctAggregation.HllStateType.INSTANCE.streamer();
        streamer.writeValueTo(out, hllState1);
        StreamInput in = out.bytes().streamInput();
        HyperLogLogDistinctAggregation.HllState hllState2 = (HyperLogLogDistinctAggregation.HllState) streamer.readValueFrom(in);
        // test that murmur3hash and HLL++ is correctly initialized with streamed dataType and version
        hllState1.add("127.0.0.1");
        hllState2.add("127.0.0.1");
        assertThat(hllState2.value(), is(hllState1.value()));
    }
}

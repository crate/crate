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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import io.crate.Streamer;
import io.crate.execution.engine.aggregation.impl.HyperLogLogPlusPlus;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.SearchPath;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class HyperLogLogDistinctAggregationTest extends AggregationTestCase {

    @Before
    public void prepareFunctions() {
        Functions functions = Functions.load(Settings.EMPTY, new SessionSettingRegistry(Set.of()));
        Roles roles = () -> List.of(Role.CRATE_USER);
        nodeCtx = new NodeContext(functions, roles);
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
        assertThat(func.boundSignature().returnType()).isEqualTo(DataTypes.LONG);
        func = nodeCtx.functions().get(
            null,
            HyperLogLogDistinctAggregation.NAME,
            List.of(Literal.of(1), Literal.of(2)),
            SearchPath.pathWithPGCatalogAndDoc()
        );
        assertThat(func.boundSignature().returnType()).isEqualTo(DataTypes.LONG);
    }

    @Test
    public void testCallWithInvalidPrecisionResultsInAnError() {
        assertThatThrownBy(
            () -> executeAggregationWithPrecision(DataTypes.INTEGER, new Object[][]{{4, 1}}, List.of(Literal.of(1))))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("precision must be >= 4 and <= 18");
    }

    @Test
    public void testWithoutPrecision() throws Exception {
        Object result = executeAggregation(DataTypes.DOUBLE, createTestData(10_000, i -> i, null));
        assertThat(result).isEqualTo(9899L);
    }

    @Test
    public void test_without_precision_string() throws Exception {
        Object result = executeAggregation(DataTypes.STRING, new Object[][]{{"Youri"}, {"Ruben"}, {"Ruben"}});
        assertThat(result).isEqualTo(2L);
    }

    @Test
    public void test_without_precision_long() throws Exception {
        // Dedicated test case to prove that integral and floating types need different implementations.
        // values.nextValue and NumericUtils.sortableDoubleBits(values.nextValue) return same result in many cases.
        // They return different result for Long.MIN_VALUE (and all close numbers with same 63-th bit)
        // but providing 1 Long.MIN_VALUE as input is still not enough.
        // The difference is revealed only when input data consists of many tricky values i.e values, close to Long.MIN_VALUE.
        Object result = executeAggregation(DataTypes.LONG, createTestData(10_000, i -> Long.MIN_VALUE + i, null));
        assertThat(result).isEqualTo(9925L);
    }

    @Test
    public void test_random_type_random_values() throws Exception {
        var validTypes = DataTypes.PRIMITIVE_TYPES.stream()
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
        assertThat(result).isEqualTo(9997L);
    }

    @Test
    public void testMurmur3HashCalculationsForAllTypes() {
        // double types
        assertThat(HyperLogLogDistinctAggregation.Murmur3Hash.getForType(DataTypes.DOUBLE, true).hash(1.3d))
            .isEqualTo(3706823019612663850L);
        assertThat(HyperLogLogDistinctAggregation.Murmur3Hash.getForType(DataTypes.FLOAT, true).hash(1.3f))
            .isEqualTo(1386670595997310747L);

        // long types
        assertThat(HyperLogLogDistinctAggregation.Murmur3Hash.getForType(DataTypes.LONG, true).hash(1L))
            .isEqualTo(-2508561340476696217L);
        assertThat(HyperLogLogDistinctAggregation.Murmur3Hash.getForType(DataTypes.INTEGER, true).hash(1))
            .isEqualTo(-2508561340476696217L);
        assertThat(HyperLogLogDistinctAggregation.Murmur3Hash.getForType(DataTypes.SHORT, true).hash(Short.valueOf("1")))
            .isEqualTo(-2508561340476696217L);
        assertThat(HyperLogLogDistinctAggregation.Murmur3Hash.getForType(DataTypes.BYTE, true).hash(Byte.valueOf("1")))
            .isEqualTo(-2508561340476696217L);
        assertThat(HyperLogLogDistinctAggregation.Murmur3Hash.getForType(DataTypes.TIMESTAMPZ, true).hash(1512569562000L))
            .isEqualTo(-3066297687939346384L);

        // bytes types
        assertThat(HyperLogLogDistinctAggregation.Murmur3Hash.getForType(DataTypes.STRING, true).hash("foo"))
            .isEqualTo(1208210750032620489L);
        assertThat(HyperLogLogDistinctAggregation.Murmur3Hash.getForType(DataTypes.BOOLEAN, true).hash(true))
            .isEqualTo(4312328700069294139L);

        // ip type
        assertThat(HyperLogLogDistinctAggregation.Murmur3Hash.getForType(DataTypes.IP, true).hash("127.0.0.1"))
            .isEqualTo(6044143379282500354L);
    }

    @Test
    public void testStreaming() throws Exception {
        HyperLogLogDistinctAggregation.HllState hllState1 = new HyperLogLogDistinctAggregation.HllState(DataTypes.IP, true);
        hllState1.init(memoryManager, HyperLogLogPlusPlus.DEFAULT_PRECISION);
        BytesStreamOutput out = new BytesStreamOutput();
        Streamer<HyperLogLogDistinctAggregation.HllState> streamer =
            HyperLogLogDistinctAggregation.HllStateType.INSTANCE.streamer();
        streamer.writeValueTo(out, hllState1);
        StreamInput in = out.bytes().streamInput();
        HyperLogLogDistinctAggregation.HllState hllState2 = streamer.readValueFrom(in);
        // test that murmur3hash and HLL++ is correctly initialized with streamed dataType and version
        hllState1.add("127.0.0.1");
        hllState2.add("127.0.0.1");
        assertThat(hllState2.value()).isEqualTo(hllState1.value());
    }
}

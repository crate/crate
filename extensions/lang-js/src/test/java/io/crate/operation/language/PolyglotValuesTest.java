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

package io.crate.operation.language;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.elasticsearch.test.ESTestCase;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.Test;

import io.crate.sql.tree.BitString;
import io.crate.types.BitStringType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FloatVectorType;
import io.crate.types.NumericType;

public class PolyglotValuesTest extends ESTestCase {

    @Test
    public void test_polyglot_value_conversion_boolean() throws Exception {
        try (var context = createContext()) {
            assertEvaluatesTo(
                context,
                """
                function getValue() {
                    return true;
                }
                """,
                DataTypes.BOOLEAN,
                true
            );
        }
    }

    @Test
    public void test_polyglot_value_conversion_string() throws Exception {
        try (var context = createContext()) {
            assertEvaluatesTo(
                context,
                """
                function getValue() {
                    return "Hoschi";
                }
                """,
                DataTypes.STRING,
                "Hoschi"
            );
        }
    }

    @Test
    public void test_polyglot_value_conversion_numbers() throws Exception {
        try (var context = createContext()) {
            for (DataType<?> type : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
                assertEvaluatesTo(
                    context,
                    "function getValue() { return 42; }",
                    type,
                    type.implicitCast(42)
                );
                assertEvaluatesTo(
                    context,
                    "function getValue() { return 3.14; }",
                    type,
                    type.implicitCast(3.14)
                );
            }

            NumericType type = new NumericType(18, 9);
            assertEvaluatesTo(
                context,
                "function getValue() { return 42; }",
                type,
                type.implicitCast(42)
            );
            assertEvaluatesTo(
                context,
                "function getValue() { return 3.14; }",
                type,
                type.implicitCast(3.14)
            );
            assertEvaluatesTo(
                context,
                """
                function getValue() {
                    return "123456789.123456789";
                }
                """,
                type,
                new BigDecimal("123456789.123456789")
            );
        }

    }


    @Test
    public void test_polyglot_value_conversion_object() throws Exception {
        try (var context = createContext()) {
            assertEvaluatesTo(
                context,
                """
                function getValue() {
                    return {
                        x: 10,
                        y: 20,
                        obj: {
                            a: "a",
                            obj2: {
                                b: "b"
                            }
                        }
                    }
                }
                """,
                DataTypes.UNTYPED_OBJECT,
                Map.of(
                    "x", 10,
                    "y", 20,
                    "obj", Map.of(
                        "a", "a",
                        "obj2", Map.of("b", "b")
                    )
                )
            );
        }
    }

    @Test
    public void test_polyglot_value_conversion_geo_shape() throws Exception {
        try (var context = createContext()) {
            assertEvaluatesTo(
                context,
                """
                function getValue() {
                    return {
                        coordinates: [[
                            [2.0, 2.0],
                            [2.0, 3.0],
                            [1.0, 3.0],
                            [1.0, 2.0],
                            [2.0, 2.0]]],
                        type: "Polygon"
                    }
                }
                """,
                DataTypes.GEO_SHAPE,
                Map.of(
                    "coordinates", List.of(List.of(
                        List.of(2, 2),
                        List.of(2, 3),
                        List.of(1, 3),
                        List.of(1, 2),
                        List.of(2, 2)
                    )),
                    "type", "Polygon"
                )
            );
        }
    }

    @Test
    public void test_polyglot_value_conversion_float_vector() throws Exception {
        try (var context = createContext()) {
            assertEvaluatesTo(
                context,
                """
                function getValue() {
                    return [0.4, 0.5, 0.6, 0.2];
                }
                """,
                new FloatVectorType(4),
                new float[] { 0.4f, 0.5f, 0.6f, 0.2f }
            );
        }
    }

    @Test
    public void test_polyglot_value_conversion_bitstring() throws Exception {
        try (var context = createContext()) {
            BitSet bitSet = new BitSet(4);
            bitSet.set(0, false);
            bitSet.set(1, true);
            bitSet.set(0, false);
            bitSet.set(0, false);
            assertEvaluatesTo(
                context,
                """
                function getValue() {
                    return "0100";
                }
                """,
                new BitStringType(4),
                new BitString(bitSet, 4)
            );
        }
    }

    /**
     * @param sourceStr function definition. Function must be called "getValue"
     **/
    private void assertEvaluatesTo(Context context,
                                   String sourceStr,
                                   DataType<?> type,
                                   Object expectedValue) throws Exception {
        var source = Source
            .newBuilder("js", sourceStr, "getValue")
            .build();
        context.eval(source);
        Value func = context.getBindings("js").getMember("getValue");
        Value result = func.execute(new Object[0]);
        Object output = PolyglotValues.toCrateObject(result, type);
        assertThat(output).isEqualTo(expectedValue);

        // Ensure concurrent access on the value is possible
        int numThreads = 5;
        ArrayList<Thread> threads = new ArrayList<>(numThreads);
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        for (int i = 0; i < numThreads; i++) {
            var thread = new Thread(() -> {
                try {
                    barrier.await();
                } catch (BrokenBarrierException | InterruptedException e) {
                    // ignore
                }
                type.implicitCast(output);
                assertThat(output).isEqualTo(expectedValue);
            });
            thread.start();
            threads.add(thread);
        }
        for (var thread : threads) {
            thread.join();
        }
    }

    private static Context createContext() {
        return Context.newBuilder("js")
            .engine(
                Engine.newBuilder()
                    .option("js.foreign-object-prototype", "true")
                    .option("engine.WarnInterpreterOnly", "false")
                    .build()
            )
            .allowHostAccess(
                HostAccess.newBuilder()
                    .allowListAccess(true)
                    .allowArrayAccess(true)
                    .allowMapAccess(true)
                    .build()
            )
            .build();
    }
}


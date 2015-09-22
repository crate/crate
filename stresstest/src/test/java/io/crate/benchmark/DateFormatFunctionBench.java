/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.benchmark;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.operation.scalar.DateFormatFunction;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DateFormatFunctionBench {

    private static final int BENCHMARK_ROUNDS = 100;
    private static final int ITERATIONS = 1_000_000;

    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    Scalar<BytesRef, Object> singleArgument;
    Scalar<BytesRef, Object> twoArgFunction;
    Scalar<BytesRef, Object> withTimeZone;

    List<Symbol> singleArgs = Collections.<Symbol>singletonList(
            Literal.newLiteral(DataTypes.TIMESTAMP, 0L)
    );
    Input<Object>[] singleArgInputs = singleArgs.toArray(new Input[1]);
    List<Symbol> twoArgs = Arrays.<Symbol>asList(
            Literal.newLiteral("%Y-%m-%d &H:%i:%S"),
            Literal.newLiteral(DataTypes.TIMESTAMP, 0L)
    );
    Input<Object>[] twoArgInputs = twoArgs.toArray(new Input[2]);
    List<Symbol> threeArgs = Arrays.<Symbol>asList(
            Literal.newLiteral("%Y-%m-%d &H:%i:%S"),
            Literal.newLiteral("Europe/Rome"),
            Literal.newLiteral(DataTypes.TIMESTAMP, 0L)
    );
    Input<Object>[] threeArgInputs = threeArgs.toArray(new Input[3]);


    @Before
    public void prepare() {
        singleArgument = new DateFormatFunction(new FunctionInfo(
                new FunctionIdent(DateFormatFunction.NAME, ImmutableList.<DataType>of(DataTypes.TIMESTAMP)),
                DataTypes.STRING,
                FunctionInfo.Type.SCALAR,
                true
        )).compile(singleArgs);
        twoArgFunction = new DateFormatFunction(new FunctionInfo(
                new FunctionIdent(DateFormatFunction.NAME, ImmutableList.<DataType>of(DataTypes.STRING, DataTypes.TIMESTAMP)),
                DataTypes.STRING,
                FunctionInfo.Type.SCALAR,
                true
        )).compile(twoArgs);
        withTimeZone = new DateFormatFunction(new FunctionInfo(
                new FunctionIdent(DateFormatFunction.NAME, ImmutableList.<DataType>of(DataTypes.STRING, DataTypes.STRING, DataTypes.TIMESTAMP)),
                DataTypes.STRING,
                FunctionInfo.Type.SCALAR,
                true
        )).compile(threeArgs);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testDateFormatSingleArg() throws Exception {
        for (int i = 0; i < ITERATIONS; i++) {
            singleArgument.evaluate(singleArgInputs);
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testDateFormatTwoArgs() throws Exception {
        for (int i = 0; i < ITERATIONS; i++) {
            twoArgFunction.evaluate(twoArgInputs);
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testDateFormatThreeArgs() throws Exception {
        for (int i = 0; i < ITERATIONS; i++) {
            withTimeZone.evaluate(threeArgInputs);
        }
    }
}

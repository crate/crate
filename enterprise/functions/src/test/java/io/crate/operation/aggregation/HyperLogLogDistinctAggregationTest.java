/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.operation.aggregation;

import io.crate.Streamer;
import io.crate.execution.engine.aggregation.impl.HyperLogLogPlusPlus;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Functions;
import io.crate.metadata.SearchPath;
import io.crate.module.EnterpriseFunctionsModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class HyperLogLogDistinctAggregationTest extends AggregationTest {

    @Before
    public void prepareFunctions() throws Exception {
        functions = new ModulesBuilder()
            .add(new EnterpriseFunctionsModule())
            .createInjector().getInstance(Functions.class);
    }

    private Object executeAggregation(DataType dataType, Object[][] data) throws Exception {
        return executeAggregation(HyperLogLogDistinctAggregation.NAME, dataType, data, Collections.singletonList(dataType));
    }

    private Object executeAggregationWithPrecision(DataType dataType, Object[][] data) throws Exception {
        return executeAggregation(HyperLogLogDistinctAggregation.NAME, dataType, data,
            List.of(dataType, DataTypes.INTEGER));
    }

    private Object[][] createTestData(int numRows, @Nullable Integer precision) {
        Object[][] data = new Object[numRows][];
        for (int i = 0; i < numRows; i++) {
            if (precision != null) {
                data[i] = new Object[]{i, precision};
            } else {
                data[i] = new Object[]{i};
            }
        }
        return data;
    }

    @Test
    public void testReturnTypeIsAlwaysLong() {
        // Return type is fixed to Long
        FunctionImplementation func = functions.resolveBuiltInFunctionBySignature(
            new FunctionName(null, HyperLogLogDistinctAggregation.NAME),
            List.of(Literal.of(1)),
            SearchPath.pathWithPGCatalogAndDoc()
        );
        assertEquals(DataTypes.LONG, func.info().returnType());
        func = functions.resolveBuiltInFunctionBySignature(
            new FunctionName(null, HyperLogLogDistinctAggregation.NAME),
            List.of(Literal.of(1), Literal.of(2)),
            SearchPath.pathWithPGCatalogAndDoc()
        );
        assertEquals(DataTypes.LONG, func.info().returnType());
    }

    @Test
    public void testCallWithInvalidPrecisionResultsInAnError() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("precision must be >= 4 and <= 18");
        executeAggregationWithPrecision(DataTypes.INTEGER, new Object[][]{{4, 1}});
    }

    @Test
    public void testWithoutPrecision() throws Exception {
        Object result = executeAggregation(DataTypes.DOUBLE, createTestData(10_000,null));
        assertThat(result, is(9899L));
    }

    @Test
    public void testWithPrecision() throws Exception {
        Object result = executeAggregationWithPrecision(DataTypes.DOUBLE, createTestData(10_000,18));
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

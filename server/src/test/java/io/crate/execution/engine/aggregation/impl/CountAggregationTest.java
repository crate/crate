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

package io.crate.execution.engine.aggregation.impl;

import io.crate.common.MutableLong;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.SearchPath;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import java.util.List;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static org.hamcrest.CoreMatchers.is;


public class CountAggregationTest extends AggregationTestCase {

    private Object executeAggregation(DataType<?> argumentType, Object[][] data) throws Exception {
        return executeAggregation(
            CountAggregation.SIGNATURE,
            List.of(argumentType),
            DataTypes.LONG,
            data,
            true
        );
    }

    @Test
    public void testReturnType() {
        var countFunction = nodeCtx.functions().get(
            null,
            CountAggregation.NAME,
            List.of(Literal.of(DataTypes.INTEGER, null)),
            SearchPath.pathWithPGCatalogAndDoc()
        );
        assertThat(countFunction.boundSignature().getReturnType().createType(), is(DataTypes.LONG));
    }

    @Test
    public void test_function_implements_doc_values_aggregator_for_numeric_types() {
        for (var dataType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            assertHasDocValueAggregator(CountAggregation.NAME, List.of(dataType));
        }
    }

    @Test
    public void test_function_implements_doc_values_aggregator_for_string_based_types() {
        for (var dataType : List.of(DataTypes.STRING, DataTypes.IP)) {
            assertHasDocValueAggregator(CountAggregation.NAME, List.of(dataType));
        }
    }

    @Test
    public void testDouble() throws Exception {
        assertThat(executeAggregation(DataTypes.DOUBLE, new Object[][]{{0.7d}, {0.3d}}), is(2L));
    }

    @Test
    public void testFloat() throws Exception {
        assertThat(executeAggregation(DataTypes.FLOAT, new Object[][]{{0.7f}, {0.3f}}), is(2L));
    }

    @Test
    public void testInteger() throws Exception {
        assertThat(executeAggregation(DataTypes.INTEGER, new Object[][]{{7}, {3}}), is(2L));
    }

    @Test
    public void testLong() throws Exception {
        assertThat(executeAggregation(DataTypes.LONG, new Object[][]{{7L}, {3L}}), is(2L));
    }

    @Test
    public void testShort() throws Exception {
        assertThat(executeAggregation(DataTypes.SHORT, new Object[][]{{(short) 7}, {(short) 3}}), is(2L));
    }

    @Test
    public void testString() throws Exception {
        assertThat(executeAggregation(DataTypes.STRING, new Object[][]{{"Youri"}, {"Ruben"}}), is(2L));
    }

    @Test
    public void test_count_with_ip_argument() throws Exception {
        assertThat(executeAggregation(DataTypes.IP, new Object[][]{{"127.0.0.1"}}), is(1L));
    }

    @Test
    public void test_count_with_geo_point_argument() throws Exception {
        assertThat(executeAggregation(DataTypes.GEO_POINT, new Object[][]{{new double[]{1, 2}}}), is(1L));
    }

    @Test
    public void testNormalizeWithNullLiteral() {
        assertThat(normalize("count", null, DataTypes.STRING), isLiteral(0L));
        assertThat(normalize("count", null, DataTypes.UNDEFINED), isLiteral(0L));
    }

    @Test
    public void test_count_star() throws Exception {
        assertThat(executeAggregation(CountAggregation.COUNT_STAR_SIGNATURE, new Object[][]{{}, {}}), is(2L));
    }

    @Test
    public void testStreaming() throws Exception {
        MutableLong l1 = new MutableLong(12345L);
        BytesStreamOutput out = new BytesStreamOutput();
        var streamer = CountAggregation.LongStateType.INSTANCE.streamer();
        streamer.writeValueTo(out, l1);
        StreamInput in = out.bytes().streamInput();
        MutableLong l2 = streamer.readValueFrom(in);
        assertThat(l1.value(), is(l2.value()));
    }
}

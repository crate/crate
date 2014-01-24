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

package io.crate.operator.collector;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.MetaDataModule;
import io.crate.operator.Input;
import io.crate.operator.aggregation.AggregationFunction;
import io.crate.operator.aggregation.impl.AggregationImplModule;
import io.crate.operator.aggregation.impl.AverageAggregation;
import io.crate.planner.symbol.*;
import org.cratedb.DataType;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GroupingCollectorTest {

    private Functions functions;
    private FunctionIdent functionIdent;
    private ValueSymbol doubleValue;
    private Aggregation[] aggregations;

    @Before
    public void setUp() throws Exception {
        Injector injector = new ModulesBuilder().add(
                new MetaDataModule(),
                new AggregationImplModule()
        ).createInjector();

        functions = injector.getInstance(Functions.class);
        functionIdent = new FunctionIdent(AverageAggregation.NAME, ImmutableList.of(DataType.DOUBLE));

        aggregations = new Aggregation[] {
                new Aggregation(functionIdent, ImmutableList.<Symbol>of(new InputColumn(0)),
                        Aggregation.Step.ITER, Aggregation.Step.FINAL)
        };
    }

    @Test
    public void testGroupingCollectorOneKey() {
        Input keyInput = mock(Input.class);
        when(keyInput.value()).thenReturn("a");

        Input aggregationInput = mock(Input.class);
        when(aggregationInput.value()).thenReturn(0.5);

        GroupingCollector collector = new GroupingCollector(
                new Input[] { keyInput },
                new Input[] { aggregationInput },
                aggregations,
                new AggregationFunction[] { (AggregationFunction)functions.get(functionIdent) }
        );

        collector.startCollect();
        for (int i = 0; i < 5; i++) {
            collector.processRow();
        }

        when(keyInput.value()).thenReturn("b");
        for (int i = 0; i < 2; i++) {
            collector.processRow();
        }

        when(aggregationInput.value()).thenReturn(20);
        for (int i = 0; i < 2; i++) {
            collector.processRow();
        }
        Object[][] result = collector.finishCollect();

        assertEquals(2, result.length);
        assertEquals("b", result[0][0]);
        assertThat((double)result[0][1], is(10.25));

        assertEquals(2, result.length);
        assertEquals("a", result[1][0]);
        assertEquals(0.5, result[1][1]);
    }

    @Test
    public void testGroupingCollectorTwoKeys() {
        Input keyInput1 = mock(Input.class);
        when(keyInput1.value()).thenReturn("a");
        Input keyInput2 = mock(Input.class);
        when(keyInput2.value()).thenReturn("b");

        Input aggregationInput = mock(Input.class);
        when(aggregationInput.value()).thenReturn(0.5);

        GroupingCollector collector = new GroupingCollector(
                new Input[] { keyInput1, keyInput2 },
                new Input[] { aggregationInput },
                aggregations,
                new AggregationFunction[] { (AggregationFunction)functions.get(functionIdent) }
        );

        collector.startCollect();
        for (int i = 0; i < 5; i++) {
            collector.processRow();
        }

        when(keyInput1.value()).thenReturn("b");
        for (int i = 0; i < 2; i++) {
            collector.processRow();
        }

        when(aggregationInput.value()).thenReturn(20);
        for (int i = 0; i < 2; i++) {
            collector.processRow();
        }

        Object[][] result = collector.finishCollect();

        assertEquals(2, result.length);
        assertEquals("b", result[0][0]);
        assertEquals("b", result[0][1]);
        assertEquals(10.25, result[0][2]);

        assertEquals("a", result[1][0]);
        assertEquals("b", result[1][1]);
        assertEquals(0.5, result[1][2]);

    }
}

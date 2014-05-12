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

package io.crate.operation.aggregation;

import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.operation.Input;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class AggregationCollectorTest {

    private FunctionIdent countAggIdent;
    private AggregationFunction countImpl;

    @Before
    public void setUpFunctions() {
        Injector injector = new ModulesBuilder().add(new AggregationImplModule()).createInjector();
        Functions functions = injector.getInstance(Functions.class);
        countAggIdent = new FunctionIdent(CountAggregation.NAME, Arrays.<DataType>asList(DataTypes.STRING));
        countImpl = (AggregationFunction) functions.get(countAggIdent);
    }

    @Test
    public void testAggregationFromPartial() {
        Aggregation aggregation = new Aggregation(
                countImpl.info(),
                Arrays.<Symbol>asList(new InputColumn(0)),
                Aggregation.Step.PARTIAL,
                Aggregation.Step.FINAL
        );
        Input dummyInput = new Input() {
            CountAggregation.CountAggState state = new CountAggregation.CountAggState() {{ value = 10L; }};


            @Override
            public Object value() {
                return state;
            }
        };

        AggregationCollector collector = new AggregationCollector(aggregation, countImpl, dummyInput);
        collector.startCollect();
        collector.processRow();
        collector.processRow();
        Object result = collector.finishCollect();

        assertThat((Long)result, is(20L));
    }

    @Test
    public void testAggregationFromIterToFinal() {
        Aggregation aggregation = new Aggregation(
                countImpl.info(),
                Arrays.<Symbol>asList(new InputColumn(0)),
                Aggregation.Step.ITER,
                Aggregation.Step.FINAL
        );

        Input dummyInput = new Input() {

            @Override
            public Object value() {
                return 300L;
            }
        };

        AggregationCollector collector = new AggregationCollector(aggregation, countImpl, dummyInput);
        collector.startCollect();
        for (int i = 0; i < 5; i++) {
            collector.processRow();
        }

        long result = (Long)collector.finishCollect();
        assertThat(result, is(5L));
    }
}

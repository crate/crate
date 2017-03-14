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

import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.Input;
import io.crate.metadata.Functions;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.core.Is.is;

public class AggregatorTest extends CrateUnitTest {

    protected static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));

    private AggregationFunction countImpl;

    @Before
    public void setUpFunctions() {
        Functions functions = getFunctions();
        countImpl = (AggregationFunction) functions
            .get(null, CountAggregation.NAME, Collections.singletonList(DataTypes.STRING));
    }

    @Test
    public void testAggregationFromPartial() {
        Aggregation aggregation = Aggregation.finalAggregation(
            countImpl.info(),
            Collections.<Symbol>singletonList(new InputColumn(0)),
            Aggregation.Step.PARTIAL
        );
        Input dummyInput = new Input() {
            CountAggregation.LongState state = new CountAggregation.LongState(10L);


            @Override
            public Object value() {
                return state;
            }
        };

        Aggregator aggregator = new Aggregator(RAM_ACCOUNTING_CONTEXT, aggregation, countImpl, dummyInput);
        Object state = aggregator.prepareState();
        state = aggregator.processRow(state);
        state = aggregator.processRow(state);
        Object result = aggregator.finishCollect(state);

        assertThat((Long) result, is(20L));
    }

    @Test
    public void testAggregationFromIterToFinal() {
        Aggregation aggregation = Aggregation.finalAggregation(
            countImpl.info(),
            Collections.<Symbol>singletonList(new InputColumn(0)),
            Aggregation.Step.ITER
        );

        Input dummyInput = new Input() {

            @Override
            public Object value() {
                return 300L;
            }
        };

        Aggregator collector = new Aggregator(RAM_ACCOUNTING_CONTEXT, aggregation, countImpl, dummyInput);
        Object state = collector.prepareState();
        for (int i = 0; i < 5; i++) {
            state = collector.processRow(state);
        }

        long result = (Long) collector.finishCollect(state);
        assertThat(result, is(5L));
    }
}

/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.engine;

import static io.crate.testing.Asserts.assertThat;

import java.util.List;
import java.util.stream.Collector;

import org.junit.Test;

import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.breaker.RowAccounting;
import io.crate.expression.symbol.SelectSymbol;


public class FirstColumnConsumersTest {

    @Test
    public void test_accounting_for_all_consumer() {
        TestingRowAccounting accounting = new TestingRowAccounting();
        Collector<Row, List<Object>, ?> collector = (Collector<Row, List<Object>, ?>) FirstColumnConsumers
            .getCollector(SelectSymbol.ResultType.SINGLE_COLUMN_MULTIPLE_VALUES, accounting);
        List<Object> supllier = collector.supplier().get();
        collector.accumulator().accept(supllier, new Row1(1));
        assertThat(accounting.calls).isEqualTo(1);
    }

    static class TestingRowAccounting implements RowAccounting<Row> {

        int calls;

        @Override
        public long accountForAndMaybeBreak(Row row) {
            calls++;
            return calls;
        }

        @Override
        public void release() {
        }
    }
}

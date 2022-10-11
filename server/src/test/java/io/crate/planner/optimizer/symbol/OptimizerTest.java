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

package io.crate.planner.optimizer.symbol;

import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isLiteral;

import org.junit.Test;

import io.crate.expression.operator.GtOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class OptimizerTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_like_on_numeric_columns_keeps_cast_around_reference() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int)")
            .build();
        Symbol symbol = Optimizer.optimizeCasts(e.asSymbol("x like 10"), e.getPlannerContext(clusterService.state()));
        assertThat(symbol).isFunction("op_like", isFunction("_cast"), isLiteral("10"));
    }

    @Test
    public void test_cast_is_not_swapped_when_column_explicitly_casted() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (strCol string, intCol int)")
            .build();

        Symbol symbol = Optimizer.optimizeCasts(e.asSymbol("strCol::bigint > 3"), e.getPlannerContext(clusterService.state()));

        assertThat(symbol).isFunction(GtOperator.NAME, isFunction("cast"), isLiteral(3L));
    }
}

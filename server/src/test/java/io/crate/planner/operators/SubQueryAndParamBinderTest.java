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

package io.crate.planner.operators;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

public class SubQueryAndParamBinderTest extends ESTestCase {

    @Test
    public void test_user_friendly_error_if_not_enough_param_values_provided() {
        SubQueryAndParamBinder paramBinder = new SubQueryAndParamBinder(Row.EMPTY, SubQueryResults.EMPTY);
        Symbol symbol = new SqlExpressions(Map.of()).asSymbol("$1 > 10");

        assertThatThrownBy(() -> paramBinder.apply(symbol))
            .hasMessage("The query contains a parameter placeholder $1, but there are only 0 parameter values");
    }

    @Test
    public void test_nested_conversion_exception_are_not_ignored() {
        var paramSymbol = new ParameterSymbol(0, ObjectType.of(ColumnPolicy.DYNAMIC).setInnerType("x", DataTypes.INTEGER).build());
        SubQueryAndParamBinder paramBinder = new SubQueryAndParamBinder(new Row1(Map.of("x", "foo")), SubQueryResults.EMPTY);

        assertThatThrownBy(() -> paramBinder.apply(paramSymbol))
            .hasMessage("Cannot cast object element `x` with value `foo` to type `integer`");
    }
}

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

package io.crate.expression.scalar;

import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isLiteral;

import java.util.List;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.data.Row1;
import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;
import io.crate.types.RowType;

public class SubscriptFunctionsTest extends ESTestCase {

    @Test
    public void test_subscript_can_be_created_for_accessing_field_within_nested_record() throws Exception {
        var rowType = new RowType(
            List.of(
                new RowType(
                    List.of(DataTypes.INTEGER),
                    List.of("y")
                )
            ),
            List.of("x")
        );
        var rowExpression = Literal.of(rowType, new Row1(new Row1(10)));
        var subscript = SubscriptFunctions.tryCreateSubscript(rowExpression, List.of("x", "y"));
        assertThat(subscript)
            .isFunction("_subscript_record", isFunction("_subscript_record"), isLiteral("y"));
    }
}

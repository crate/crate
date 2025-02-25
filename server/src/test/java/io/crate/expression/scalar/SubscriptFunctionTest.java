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
import static io.crate.testing.Asserts.exactlyInstanceOf;
import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ConversionException;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.types.DataTypes;


public class SubscriptFunctionTest extends ScalarTestCase {

    @Test
    public void test_long_can_be_used_as_array_index() {
        assertEvaluate("['Youri', 'Ruben'][x]", "Youri", Literal.of(1L));
    }

    @Test
    public void test_index_out_of_range_array_access() {
        assertThatThrownBy(
            () -> assertEvaluate("['Youri', 'Ruben'][x]",
                                 null,
                                 Literal.of((long) Integer.MAX_VALUE + 1)))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessage("Cannot cast value `2147483648` to type `integer`");
        assertThatThrownBy(
            () -> assertEvaluate("['Youri', 'Ruben'][x]",
                                 null,
                                 Literal.of((long) Integer.MIN_VALUE - 1)))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessage("Cannot cast value `-2147483649` to type `integer`");
    }

    @Test
    public void test_valid_min_and_max_array_index_access() {
        assertNormalize("subscript([1,2,3], 2147483647)", isLiteral(null, DataTypes.INTEGER));
        assertNormalize("subscript([1,2,3], -2147483648)", isLiteral(null, DataTypes.INTEGER));
    }

    @Test
    public void test_subscript_can_retrieve_items_of_objects_within_array() {
        assertEvaluate("[{x=10}, {x=2}]['x']", List.of(10, 2));
    }

    @Test
    public void test_subscript_can_access_item_from_array_based_on_object() {
        assertNormalize("[{x=10}, {x=2}]['x'][1]", isLiteral(10, DataTypes.INTEGER));
    }

    @Test
    public void test_subscript_can_be_used_on_subqueries_returning_objects() {
        assertNormalize(
            "(select {x=10})['x']",
            isFunction("subscript", exactlyInstanceOf(SelectSymbol.class), isLiteral("x"))
        );
    }

    @Test
    public void testEvaluate() throws Exception {
        assertNormalize("subscript(['Youri', 'Ruben'], cast(1 as integer))", isLiteral("Youri", DataTypes.STRING));
    }

    @Test
    public void testNormalizeSymbol() throws Exception {
        assertNormalize("subscript(tags, cast(1 as integer))", isFunction("subscript", DataTypes.STRING));
    }

    @Test
    public void testIndexOutOfRange() throws Exception {
        assertNormalize("subscript(['Youri', 'Ruben'], cast(3 as integer))", isLiteral(null, DataTypes.STRING));
    }

    @Test
    public void testIndexExpressionIsNotInteger() throws Exception {
        assertThatThrownBy(
            () -> assertNormalize("subscript(['Youri', 'Ruben'], 'foo')", isLiteral("Ruben")))
            .isExactlyInstanceOf(ConversionException.class)
                .hasMessage("Cannot cast `'foo'` of type `text` to type `integer`");
    }

    @Test
    public void testLookupByNameWithUnknownName() throws Exception {
        sqlExpressions.setErrorOnUnknownObjectKey(true);
        assertThatThrownBy(() -> assertEvaluate("{}['y']", null))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessageContaining("The object `{}` does not contain the key `y`");
        sqlExpressions.setErrorOnUnknownObjectKey(false);
        assertEvaluateNull("{}['y']");
    }

    @Test
    public void test_unknown_element_of_object_array() {
        sqlExpressions.setErrorOnUnknownObjectKey(true);
        assertThatThrownBy(() -> assertEvaluate("[{}]['y']", null))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessageContaining("The object `{}` does not contain the key `y`");

        sqlExpressions.setErrorOnUnknownObjectKey(false);
        assertNormalize("[{}]['y']", isLiteral(Arrays.stream(new Object[1]).toList()));
    }

    @Test
    public void test_lookup_by_name_with_missing_key_returns_null_if_type_information_are_available() throws Exception {
        assertEvaluateNull("{}::object(strict) as (y int)['y']");
    }

    @Test
    public void test_lookup_by_name_depends_on_column_policy_and_error_on_unknown_object_key_settings() throws Exception {
        sqlExpressions.setErrorOnUnknownObjectKey(true);
        assertThatThrownBy(() -> assertEvaluate("{}::object(strict)['missing_key']", null))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessageContaining("The object `{}` does not contain the key `missing_key`");
        assertThatThrownBy(() -> assertEvaluate("{}::object(dynamic)['missing_key']", null))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessageContaining("The object `{}` does not contain the key `missing_key`");
        assertEvaluateNull("{}::object(ignored)['missing_key']");
        sqlExpressions.setErrorOnUnknownObjectKey(false);
        assertThatThrownBy(() -> assertEvaluate("{}::object(strict)['missing_key']", null))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessageContaining("The object `{}` does not contain the key `missing_key`");
        assertEvaluateNull("{}::object(dynamic)['missing_key']");
        assertEvaluateNull("{}::object(ignored)['missing_key']");
    }

    @Test
    public void test_nested_subscript_on_ignored_column_returns_null() {
        assertNormalize("{}::OBJECT(IGNORED)['a']['b']", isLiteral(null, DataTypes.UNDEFINED));
    }

    @Test
    public void test_return_type_of_subscript_on_literals() {
        assertNormalize("{a = {b = 1}}['a']['b']", isLiteral(1, DataTypes.INTEGER));
        assertNormalize("subscript({a = {b = 1}}['a'], 'b')", isLiteral(1, DataTypes.INTEGER));
    }

    @Test
    public void test_return_type_of_subscript_on_expressions() {
        Symbol symbol = sqlExpressions.asSymbol("(obj_typed || {c=1})['a']['b']");
        assertThat(symbol).isFunction("subscript", DataTypes.INTEGER);
    }
}

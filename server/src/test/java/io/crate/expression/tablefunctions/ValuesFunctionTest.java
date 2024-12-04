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

package io.crate.expression.tablefunctions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.symbol.Function;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.types.DataTypes;
import io.crate.types.RowType;

public class ValuesFunctionTest extends AbstractTableFunctionsTest {

    @Test
    public void test_one_col_one_row() {
        assertExecute("_values([1])", "1\n");
    }

    @Test
    public void test_two_mixed_cols() {
        assertExecute("_values([1, 2], ['a', 'b'])",
                      "1| a\n" +
                      "2| b\n");
    }

    @Test
    public void test_first_array_shorter() {
        assertExecute("_values([1, 2], [1, 2, 3])",
                      "1| 1\n" +
                      "2| 2\n" +
                      "NULL| 3\n");
    }

    @Test
    public void test_second_array_shorter() {
        assertExecute("_values([1, 2, 3], [1, 2])",
                      "1| 1\n" +
                      "2| 2\n" +
                      "3| NULL\n");
    }

    @Test
    public void test_single_null_argument_returns_zero_rows() {
        assertExecute("_values(null)", "");
    }

    @Test
    public void test_null_and_array_with_null() {
        assertExecute("_values(null, [1, null])",
                      "NULL| 1\n" +
                      "NULL| NULL\n");
    }

    @Test
    public void test_does_not_flatten_nested_arrays() {
        assertExecute("_values([[1, 2], [3, 4, 5]])",
                      "[1, 2]\n" +
                      "[3, 4, 5]\n"
        );
    }

    @Test
    public void test_does_not_flatten_nested_arrays_with_mixed_depth() {
        assertExecute("_values([[1, 2], [3, 4, 5]], ['a', 'b'])",
                      "[1, 2]| a\n" +
                      "[3, 4, 5]| b\n"
        );
    }

    @Test
    public void test_function_return_type_of_the_next_nested_item() {
        Function function = (Function) sqlExpressions.asSymbol("_values([['a', 'b']])");
        var funcImplementation = (TableFunctionImplementation<?>) sqlExpressions.nodeCtx.functions().getQualified(
            function);

        assertThat(funcImplementation.returnType()).isExactlyInstanceOf(RowType.class);
        assertThat(funcImplementation.returnType().fieldTypes()).containsExactly(DataTypes.STRING_ARRAY);

    }

    @Test
    public void test_function_arguments_must_have_array_types() {
        assertThatThrownBy(() -> assertExecute("_values(200)", ""))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: _values(200), " +
                                    "no overload found for matching argument types: (integer).");
    }

    @Test
    public void test_bound_signature_return_type_resolves_correct_row_type_parameters() {
        var function = (Function) sqlExpressions.asSymbol("_values([1], ['a'], [{}])");
        var functionImplementation = (TableFunctionImplementation<?>) sqlExpressions.nodeCtx.functions().getQualified(
            function);
        assertThat(functionImplementation.boundSignature().returnType().getTypeParameters())
            .isEqualTo(List.of(DataTypes.INTEGER, DataTypes.STRING, DataTypes.UNTYPED_OBJECT));
    }
}

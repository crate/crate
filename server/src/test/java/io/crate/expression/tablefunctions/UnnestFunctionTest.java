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

import java.util.List;

import org.junit.Test;

import io.crate.expression.symbol.Function;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.types.DataTypes;


public class UnnestFunctionTest extends AbstractTableFunctionsTest {

    @Test
    public void test1Col1Row() throws Exception {
        assertExecute("unnest([1])", "1\n");
    }

    @Test
    public void test2MixedCols() throws Exception {
        assertExecute("unnest([1, 2], ['a', 'b'])", "1| a\n2| b\n");
    }

    @Test
    public void testFirstArrayShorter() throws Exception {
        assertExecute("unnest([1, 2], [1, 2, 3])", "1| 1\n2| 2\nNULL| 3\n");
    }

    @Test
    public void testSecondArrayShorter() throws Exception {
        assertExecute("unnest([1, 2, 3], [1, 2])", "1| 1\n2| 2\n3| NULL\n");
    }

    @Test
    public void testUnnestWithNullArgReturnsZeroRows() {
        assertExecute("unnest(null)", "");
    }

    @Test
    public void testUnnestWithNullArgAndArrayArg() {
        assertExecute("unnest(null, [1, null])",
            "NULL| 1\n" +
            "NULL| NULL\n");
    }

    @Test
    public void test_unnest_flattens_multi_dimensional_arrays() {
        assertExecute(
            "unnest([[1, 2], [3, 4, 5]])",
            "1\n" +
            "2\n" +
            "3\n" +
            "4\n" +
            "5\n"
        );
    }

    @Test
    public void test_unnest_1st_col_multi_dim_array_and_2nd_col_array() {
        assertExecute(
            "unnest([[1, 2], [3, 4, 5]], ['a', 'b'])",
            "1| a\n" +
            "2| b\n" +
            "3| NULL\n" +
            "4| NULL\n" +
            "5| NULL\n"
        );
    }

    @Test
    public void test_unnest_bound_signature_return_type_resolves_correct_row_type_parameters() {
        var function = (Function) sqlExpressions.asSymbol("unnest([1], ['a'], [{}])");
        var functionImplementation = (TableFunctionImplementation<?>) sqlExpressions.nodeCtx.functions().getQualified(
            function
        );
        assertThat(
            functionImplementation.boundSignature().returnType().getTypeParameters()).isEqualTo(List.of(DataTypes.INTEGER, DataTypes.STRING, DataTypes.UNTYPED_OBJECT));
    }
}

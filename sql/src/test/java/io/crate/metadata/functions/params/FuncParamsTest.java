/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.functions.params;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.FuncArg;
import io.crate.expression.symbol.Literal;
import io.crate.exceptions.ConversionException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Path;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class FuncParamsTest extends CrateUnitTest {

    @Test
    public void testNoParams() {
        FuncParams none = FuncParams.NONE;
        assertThat(none.match(Collections.emptyList()), is(Collections.emptyList()));
    }

    @Test
    public void testOneParam() {
        FuncParams oneArg = FuncParams.builder(Param.ANY).build();
        Literal<Integer> symbol = Literal.of(1);
        List<DataType> match = oneArg.match(Collections.singletonList(symbol));
        assertThat(match, is(Collections.singletonList(symbol.valueType())));
    }

    @Test
    public void testTwoConnectedParams() {
        FuncParams twoArgs = FuncParams.builder(Param.ANY, Param.ANY).build();
        Literal<Integer> symbol1 = Literal.of(1);
        Literal<Long> symbol2 = Literal.of(1L);
        List<DataType> match = twoArgs.match(list(symbol1, symbol2));
        assertThat(match, is(list(symbol2.valueType(), symbol2.valueType())));
    }

    @Test
    public void testTooManyArgs() {
        FuncParams params = FuncParams.builder().build();
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The number of arguments is incorrect");
        params.match(list(Literal.of(1)));
    }

    @Test
    public void testVarArgs() {
        FuncParams onlyVarArgs = FuncParams.builder(Param.ANY).withVarArgs(Param.ANY).build();
        List<DataType> signature = onlyVarArgs.match(
            list(Literal.of(1), Literal.of(1L), Literal.of(1.0)));
        assertThat(signature, is(list(DataTypes.DOUBLE, DataTypes.DOUBLE, DataTypes.DOUBLE)));
    }

    @Test
    public void testVarArgLimit() {
        FuncParams maxOneVarArg = FuncParams.builder(Param.ANY)
            .withVarArgs(Param.ANY).limitVarArgOccurrences(1).build();
        maxOneVarArg.match(list(Literal.of("foo")));
        maxOneVarArg.match(list(Literal.of("bla")));

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Too many variable arguments provided");
        List<DataType> signature = maxOneVarArg.match(
            list(Literal.of(1), Literal.of(1L), Literal.of(1.0)));
        assertThat(signature, is(list(DataTypes.DOUBLE, DataTypes.DOUBLE, DataTypes.DOUBLE)));
    }

    @Test
    public void testIndependentVarArgs() {
        FuncParams independentVarArgs = FuncParams.builder(Param.ANY)
            .withIndependentVarArgs(Param.ANY).build();
        List<DataType> signature = independentVarArgs.match(
            list(Literal.of(1), Literal.of(1L), Literal.of(1.0)));
        assertThat(signature, is(list(DataTypes.INTEGER, DataTypes.LONG, DataTypes.DOUBLE)));
    }

    @Test
    public void testCompositeTypes() {
        FuncParams params = FuncParams.builder(Param.ANY_ARRAY, Param.ANY_ARRAY).build();

        ArrayType longArray = new ArrayType(DataTypes.LONG);
        List<DataType> signature = params.match(
            list(
                Literal.of(new Object[]{1L, 2L, 3L}, longArray),
                Literal.of(new Object[]{4L, 5L, 6L}, longArray)
            ));
        assertThat(signature, is(list(longArray, longArray)));

        ArrayType integerArray = new ArrayType(DataTypes.INTEGER);
        signature = params.match(
            list(
                Literal.of(new Object[]{1L, 2L, 3L}, longArray),
                Literal.of(new Object[]{4, 5, 6}, integerArray)
            ));
        assertThat(signature, is(list(longArray, longArray)));
    }

    @Test
    public void testAllowedTypes() {
        FuncParams numericParams = FuncParams.builder(Param.STRING).build();
        List<DataType> foo = numericParams.match(list(Literal.of(1)));
        assertThat(foo, is(list(DataTypes.STRING)));

        FuncParams arrayParams = FuncParams.builder(Param.of(DataTypes.OBJECT)).build();
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast 1");
        arrayParams.match(list(Literal.of(1)));
    }

    @Test
    public void testFieldsAreNotCastable() {
        Path path = new ColumnIdent("test");
        Field field = new Field(Mockito.mock(AnalyzedRelation.class), path, DataTypes.INTEGER);
        FuncParams params = FuncParams.builder(Param.LONG).build();
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast test to type long");
        params.match(list(field));
    }

    @Test
    public void testRespectCastableArguments() {
        FuncArg castableArg = new Arg(DataTypes.INTEGER, true);
        FuncParams params = FuncParams.builder(Param.LONG).build();
        assertThat(params.match(list(castableArg)), is(list(DataTypes.LONG)));
    }

    @Test
    public void testRespectNonCastableArguments() {
        FuncArg castableArg = new Arg(DataTypes.INTEGER, false);
        FuncParams params = FuncParams.builder(Param.LONG).build();
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast testarg to type long");
        params.match(list(castableArg));
    }

    @Test
    public void testRespectCastableArgumentsWithInnerType() {
        FuncArg castableArg = new Arg(DataTypes.LONG, true);
        ArrayType integerArrayType = new ArrayType(DataTypes.INTEGER);
        FuncArg nonCastableArg = new Arg(integerArrayType, false);
        FuncParams params = FuncParams.builder(Param.ANY, Param.ANY_ARRAY.withInnerType(Param.ANY)).build();

        List<DataType> signature = params.match(list(castableArg, nonCastableArg));
        assertThat(signature, is(list(DataTypes.INTEGER, integerArrayType)));
    }

    private static class Arg implements FuncArg {

        private final DataType dataType;
        private final boolean castable;

        Arg(DataType dataType, boolean castable) {
            this.dataType = dataType;
            this.castable = castable;
        }

        @Override
        public DataType valueType() {
            return dataType;
        }

        @Override
        public boolean canBeCasted() {
            return castable;
        }

        @Override
        public String toString() {
            return "testarg";
        }
    }

    @SafeVarargs
    private static <T> List<T> list(T... elements) {
        return Arrays.asList(elements);
    }

}

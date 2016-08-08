/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
package io.crate.operation.operator;

import com.google.common.collect.Sets;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.metadata.StmtCtx;
import io.crate.operation.operator.input.ObjectInput;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class InOperatorTest extends CrateUnitTest{

    private final StmtCtx stmtCtx = new StmtCtx();

    private static final DataType INTEGER_SET_TYPE = new SetType(DataTypes.INTEGER);
    private static final DataType STRING_SET_TYPE = new SetType(DataTypes.STRING);

    @Test
    public void testNormalizeSymbolSetLiteralIntegerIncluded() {
        Literal<Integer> inValue = Literal.newLiteral(1);
        Literal inListValues = Literal.newLiteral(INTEGER_SET_TYPE, Sets.newHashSet(1, 2, 4, 8));

        List<Symbol> arguments = new ArrayList<>();
        arguments.add(inValue);
        arguments.add(inListValues);

        InOperator op = new InOperator(Operator.generateInfo(InOperator.NAME, DataTypes.INTEGER));
        Function function = new Function(op.info(), arguments);
        Symbol result = op.normalizeSymbol(function, stmtCtx);

        assertThat(result, isLiteral(true));
    }

    @Test
    public void testNormalizeSymbolSetLiteralIntegerNotIncluded() {
        Literal<Integer> inValue = Literal.newLiteral(128);

        Literal inListValues = Literal.newLiteral(INTEGER_SET_TYPE, Sets.newHashSet(1, 2, 4, 8));

        List<Symbol> arguments = new ArrayList<>();
        arguments.add(inValue);
        arguments.add(inListValues);

        InOperator op = new InOperator(Operator.generateInfo(InOperator.NAME, DataTypes.INTEGER));
        Function function = new Function(op.info(), arguments);
        Symbol result = op.normalizeSymbol(function, stmtCtx);

        assertThat(result, isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolSetLiteralDifferentDataTypeValue() {
        Literal<Double> value = Literal.newLiteral(2.0);
        Literal inListValues = Literal.newLiteral(INTEGER_SET_TYPE, Sets.newHashSet(1, 2, 4, 8));

        List<Symbol> arguments = new ArrayList<>();
        arguments.add(value);
        arguments.add(inListValues);

        InOperator op = new InOperator(Operator.generateInfo(InOperator.NAME, DataTypes.INTEGER));
        Function function = new Function(op.info(), arguments);
        Symbol result = op.normalizeSymbol(function, stmtCtx);

        assertThat(result, isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolSetLiteralReference() {
        Reference reference = new Reference();
        Literal inListValues = Literal.newLiteral(INTEGER_SET_TYPE, Sets.newHashSet(1, 2, 4, 8));

        List<Symbol> arguments = new ArrayList<>();
        arguments.add(reference);
        arguments.add(inListValues);

        InOperator op = new InOperator(Operator.generateInfo(InOperator.NAME, DataTypes.INTEGER));
        Function function = new Function(op.info(), arguments);
        Symbol result = op.normalizeSymbol(function, stmtCtx);

        assertThat(result, instanceOf(Function.class));
        assertThat(((Function) result).info().ident().name(), is(InOperator.NAME));
    }

    @Test
    public void testNormalizeSymbolSetLiteralStringIncluded() {
        Literal inValue = Literal.newLiteral("charlie");
        Literal inListValues = Literal.newLiteral(
                STRING_SET_TYPE,
                Sets.newHashSet(
                        new BytesRef("alpha"),
                        new BytesRef("bravo"),
                        new BytesRef("charlie"),
                        new BytesRef("delta")
                )
        );

        List<Symbol> arguments = new ArrayList<>();
        arguments.add(inValue);
        arguments.add(inListValues);

        InOperator op = new InOperator(Operator.generateInfo(InOperator.NAME, DataTypes.INTEGER));
        Function function = new Function(op.info(), arguments);
        Symbol result = op.normalizeSymbol(function, stmtCtx);

        assertThat(result, isLiteral(true));
    }

    @Test
    public void testNormalizeSymbolSetLiteralStringNotIncluded() {
        Literal inValue = Literal.newLiteral("not included");
        Literal inListValues = Literal.newLiteral(
                STRING_SET_TYPE,
                Sets.newHashSet(
                        new BytesRef("alpha"),
                        new BytesRef("bravo"),
                        new BytesRef("charlie"),
                        new BytesRef("delta")
                )
        );

        List<Symbol> arguments = new ArrayList<>();
        arguments.add(inValue);
        arguments.add(inListValues);

        InOperator op = new InOperator(Operator.generateInfo(InOperator.NAME, DataTypes.INTEGER));
        Function function = new Function(op.info(), arguments);
        Symbol result = op.normalizeSymbol(function, stmtCtx);

        assertThat(result, isLiteral(false));
    }


    private Boolean in(Object inValue, Object... inList) {
        Set<Object> inListValues = new HashSet<>();
        for (Object o : inList) {
            inListValues.add(o);
        }
        InOperator op = new InOperator(Operator.generateInfo(InOperator.NAME, DataTypes.INTEGER));
        return op.evaluate(new ObjectInput(inValue), new ObjectInput(inListValues));
    }

    @Test
    public void testEvaluateInOperator() {
        assertTrue(in(1, 1,2,4,8));
        assertFalse(in(128, 1, 2, 4, 8));
        assertTrue(in("charlie", "alpha", "bravo", "charlie", "delta"));
        assertFalse(in("not included", "alpha", "bravo", "charlie", "delta"));
        assertNull(in(null, "alpha", "bravo", "charlie", "delta"));

        // "where 'something' in (null)"
        assertNull(in("something", new Object[]{null}));

        // "where 'something' in null"
        InOperator op = new InOperator(Operator.generateInfo(InOperator.NAME, DataTypes.STRING));
        assertNull(op.evaluate(new ObjectInput("something"), new ObjectInput(null)));
    }

}

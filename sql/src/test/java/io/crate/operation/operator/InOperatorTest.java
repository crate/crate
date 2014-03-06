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

import io.crate.operation.operator.input.ObjectInput;
import io.crate.planner.symbol.*;
import io.crate.DataType;
import org.junit.Test;

import java.util.*;

import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class InOperatorTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Test
    public void testNormalizeSymbolSetLiteralIntegerIncluded() {
        IntegerLiteral inValue = new IntegerLiteral(1);
        SetLiteral inListValues = SetLiteral.fromLiterals(
                DataType.INTEGER,
                new HashSet<Literal>(
                        Arrays.asList(
                                new IntegerLiteral(1),
                                new IntegerLiteral(2),
                                new IntegerLiteral(4),
                                new IntegerLiteral(8)
                        )
                )
        );

        List<Symbol> arguments = new ArrayList<>();
        arguments.add(inValue);
        arguments.add(inListValues);

        InOperator op = new InOperator(Operator.generateInfo(InOperator.NAME, DataType.INTEGER));
        Function function = new Function(op.info(), arguments);
        Symbol result = op.normalizeSymbol(function);

        assertThat(result, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral) result).value(), is(true));
    }

    @Test
    public void testNormalizeSymbolSetLiteralIntegerNotIncluded() {
        IntegerLiteral inValue = new IntegerLiteral(128);

        SetLiteral inListValues = SetLiteral.fromLiterals(
                DataType.INTEGER,
                new HashSet<Literal>(
                        Arrays.asList(
                                new IntegerLiteral(1),
                                new IntegerLiteral(2),
                                new IntegerLiteral(4),
                                new IntegerLiteral(8)
                        )
                )
        );

        List<Symbol> arguments = new ArrayList<>();
        arguments.add(inValue);
        arguments.add(inListValues);

        InOperator op = new InOperator(Operator.generateInfo(InOperator.NAME, DataType.INTEGER));
        Function function = new Function(op.info(), arguments);
        Symbol result = op.normalizeSymbol(function);

        assertThat(result, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral) result).value(), is(false));
    }

    @Test
    public void testNormalizeSymbolSetLiteralDifferentDataTypeValue() {
        DoubleLiteral value = new DoubleLiteral(2.0);

        SetLiteral inListValues = SetLiteral.fromLiterals(
                DataType.INTEGER,
                new HashSet<Literal>(
                        Arrays.asList(
                                new IntegerLiteral(1),
                                new IntegerLiteral(2),
                                new IntegerLiteral(4),
                                new IntegerLiteral(8)
                        )
                )
        );

        List<Symbol> arguments = new ArrayList<>();
        arguments.add(value);
        arguments.add(inListValues);

        InOperator op = new InOperator(Operator.generateInfo(InOperator.NAME, DataType.INTEGER));
        Function function = new Function(op.info(), arguments);
        Symbol result = op.normalizeSymbol(function);

        assertThat(result, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral) result).value(), is(false));
    }

    @Test
    public void testNormalizeSymbolSetLiteralReference() {
        Reference reference = new Reference();

        SetLiteral inListValues = SetLiteral.fromLiterals(
                DataType.INTEGER,
                new HashSet<Literal>(
                        Arrays.asList(
                                new IntegerLiteral(1),
                                new IntegerLiteral(2),
                                new IntegerLiteral(4),
                                new IntegerLiteral(8)
                        )
                )
        );

        List<Symbol> arguments = new ArrayList<>();
        arguments.add(reference);
        arguments.add(inListValues);

        InOperator op = new InOperator(Operator.generateInfo(InOperator.NAME, DataType.INTEGER));
        Function function = new Function(op.info(), arguments);
        Symbol result = op.normalizeSymbol(function);

        assertThat(result, instanceOf(Function.class));
        assertThat(((Function) result).info().ident().name(), is(InOperator.NAME));
    }

    @Test
    public void testNormalizeSymbolSetLiteralStringIncluded() {
        StringLiteral inValue = new StringLiteral("charlie");
        SetLiteral inListValues = SetLiteral.fromLiterals(
                DataType.STRING,
                new HashSet<Literal>(
                        Arrays.asList(
                                new StringLiteral("alpha"),
                                new StringLiteral("bravo"),
                                new StringLiteral("charlie"),
                                new StringLiteral("delta")
                        )
                )
        );

        List<Symbol> arguments = new ArrayList<>();
        arguments.add(inValue);
        arguments.add(inListValues);

        InOperator op = new InOperator(Operator.generateInfo(InOperator.NAME, DataType.INTEGER));
        Function function = new Function(op.info(), arguments);
        Symbol result = op.normalizeSymbol(function);

        assertThat(result, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral) result).value(), is(true));
    }

    @Test
    public void testNormalizeSymbolSetLiteralStringNotIncluded() {
        StringLiteral inValue = new StringLiteral("not included");
        SetLiteral inListValues = SetLiteral.fromLiterals(
                DataType.STRING,
                new HashSet<Literal>(
                        Arrays.asList(
                                new StringLiteral("alpha"),
                                new StringLiteral("bravo"),
                                new StringLiteral("charlie"),
                                new StringLiteral("delta")
                        )
                )
        );

        List<Symbol> arguments = new ArrayList<>();
        arguments.add(inValue);
        arguments.add(inListValues);

        InOperator op = new InOperator(Operator.generateInfo(InOperator.NAME, DataType.INTEGER));
        Function function = new Function(op.info(), arguments);
        Symbol result = op.normalizeSymbol(function);

        assertThat(result, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral) result).value(), is(false));
    }


    private Boolean in(Object inValue, Object... inList) {
        Set<Object> inListValues = new HashSet<>();
        for (Object o : inList) {
            inListValues.add(o);
        }
        InOperator op = new InOperator(Operator.generateInfo(InOperator.NAME, DataType.INTEGER));
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
        InOperator op = new InOperator(Operator.generateInfo(InOperator.NAME, DataType.STRING));
        assertNull(op.evaluate(new ObjectInput("something"), new ObjectInput(null)));
    }

}

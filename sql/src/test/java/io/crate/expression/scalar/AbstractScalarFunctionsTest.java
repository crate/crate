/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.expression.scalar;

import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.collections.Lists2;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.symbol.FieldReplacer;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.Scalar;
import io.crate.metadata.SearchPath;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.SetType;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public abstract class AbstractScalarFunctionsTest extends CrateUnitTest {

    protected SqlExpressions sqlExpressions;
    protected Functions functions;
    protected Map<QualifiedName, AnalyzedRelation> tableSources;
    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();
    private InputFactory inputFactory;

    @Before
    public void prepareFunctions() throws Exception {
        DocTableInfo tableInfo = TestingTableInfo.builder(new RelationName(DocSchemaInfo.NAME, "users"), null)
            .add("id", DataTypes.INTEGER)
            .add("name", DataTypes.STRING)
            .add("tags", new ArrayType(DataTypes.STRING))
            .add("age", DataTypes.INTEGER)
            .add("a", DataTypes.INTEGER)
            .add("x", DataTypes.LONG)
            .add("shape", DataTypes.GEO_SHAPE)
            .add("timestamp", DataTypes.TIMESTAMP)
            .add("timezone", DataTypes.STRING)
            .add("interval", DataTypes.STRING)
            .add("time_format", DataTypes.STRING)
            .add("long_array", new ArrayType(DataTypes.LONG))
            .add("int_array", new ArrayType(DataTypes.INTEGER))
            .add("array_string_array", new ArrayType(new ArrayType(DataTypes.STRING)))
            .add("long_set", new SetType(DataTypes.LONG))
            .add("regex_pattern", DataTypes.STRING)
            .add("geoshape", DataTypes.GEO_SHAPE)
            .add("geopoint", DataTypes.GEO_POINT)
            .add("geostring", DataTypes.STRING)
            .add("is_awesome", DataTypes.BOOLEAN)
            .add("double_val", DataTypes.DOUBLE)
            .add("float_val", DataTypes.DOUBLE)
            .add("short_val", DataTypes.SHORT)
            .add("obj", ObjectType.untyped())
            .build();
        DocTableRelation tableRelation = new DocTableRelation(tableInfo);
        tableSources = ImmutableMap.of(new QualifiedName("users"), tableRelation);
        sqlExpressions = new SqlExpressions(tableSources);
        functions = sqlExpressions.functions();
        inputFactory = new InputFactory(functions);
    }

    /**
     * Assert that the functionExpression normalizes to the expectedSymbol
     * <p>
     * If the result of normalize is a Literal and all arguments were Literals evaluate is also called and
     * compared to the result of normalize - the resulting value of normalize must match evaluate.
     */
    @SuppressWarnings("unchecked")
    public void assertNormalize(String functionExpression, Matcher<? super Symbol> expectedSymbol) {
        assertNormalize(functionExpression, expectedSymbol, true);
    }

    public void assertNormalize(String functionExpression, Matcher<? super Symbol> expectedSymbol, boolean evaluate) {
        Symbol functionSymbol = sqlExpressions.asSymbol(functionExpression);
        if (functionSymbol instanceof Literal) {
            assertThat(functionSymbol, expectedSymbol);
            return;
        }
        Function function = (Function) functionSymbol;
        FunctionImplementation impl = functions.getQualified(function.info().ident());
        assertThat(impl, Matchers.notNullValue());

        Symbol normalized = sqlExpressions.normalize(function);
        assertThat(
            String.format(Locale.ENGLISH, "expected <%s> to normalize to %s", functionExpression, expectedSymbol),
            normalized,
            expectedSymbol);

        if (evaluate && normalized instanceof Input && allArgsAreInputs(function.arguments())) {
            Input[] inputs = new Input[function.arguments().size()];
            for (int i = 0; i < inputs.length; i++) {
                inputs[i] = ((Input) function.arguments().get(i));
            }
            Object expectedValue = ((Input) normalized).value();
            assertThat(((Scalar) impl).evaluate(txnCtx, inputs), is(expectedValue));
            assertThat(((Scalar) impl).compile(function.arguments()).evaluate(txnCtx, inputs), is(expectedValue));
        }
    }

    /**
     * asserts that the given functionExpression matches the given matcher.
     * If the functionExpression contains references the inputs will be used in the order the references appear.
     * <p>
     * E.g.
     * <code>
     * assertEvaluate("foo(name, age)", anyOf("expectedValue1", "expectedValue2"), inputForName, inputForAge)
     * </code>
     */
    @SuppressWarnings("unchecked")
    public <T> void assertEvaluate(String functionExpression, Matcher<T> expectedValue, Literal<?>... literals) {
        if (expectedValue == null) {
            expectedValue = (Matcher<T>) nullValue();
        }
        Symbol functionSymbol = sqlExpressions.asSymbol(functionExpression);
        functionSymbol = sqlExpressions.normalize(functionSymbol);
        if (functionSymbol instanceof Literal) {
            Object value = ((Literal) functionSymbol).value();
            assertThat((T) value, expectedValue);
            return;
        }
        LinkedList<Literal<?>> unusedLiterals = new LinkedList<>(Arrays.asList(literals));
        Function function = (Function) FieldReplacer.replaceFields(functionSymbol, f -> {
            Literal<?> literal = unusedLiterals.pollFirst();
            if (literal == null) {
                throw new IllegalArgumentException("No value literal for field=" + f + ", please add more literals");
            }
            return literal;
        });
        Scalar scalar = (Scalar) functions.getQualified(function.info().ident());
        AssertMax1ValueCallInput[] arguments = new AssertMax1ValueCallInput[function.arguments().size()];
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns(txnCtx);
        for (int i = 0; i < function.arguments().size(); i++) {
            Symbol arg = function.arguments().get(i);
            Input<?> input = ctx.add(arg);
            arguments[i] = new AssertMax1ValueCallInput(input);
        }
        Object actualValue = scalar.compile(function.arguments()).evaluate(txnCtx, (Input[]) arguments);
        assertThat((T) actualValue, expectedValue);

        // Reset calls
        for (AssertMax1ValueCallInput argument : arguments) {
            argument.calls = 0;
        }

        actualValue = scalar.evaluate(txnCtx, (Input[]) arguments);
        assertThat((T) actualValue, expectedValue);
    }

    /**
     * asserts that the given functionExpression evaluates to the expectedValue.
     * If the functionExpression contains references the inputs will be used in the order the references appear.
     * <p>
     * E.g.
     * <code>
     * assertEvaluate("foo(name, age)", "expectedValue", inputForName, inputForAge)
     * </code>
     * or
     * <code>
     * assertEvaluate("foo('literalName', age)", "expectedValue", inputForAge)
     * </code>
     */
    public void assertEvaluate(String functionExpression, Object expectedValue, Literal<?>... literals) {
        if (expectedValue == null) {
            assertEvaluate(functionExpression, nullValue());
        } else {
            assertEvaluate(functionExpression, is(expectedValue), literals);
        }
    }

    public void assertCompile(String functionExpression, java.util.function.Function<Scalar, Matcher<Scalar>> matcher) {
        Symbol functionSymbol = sqlExpressions.asSymbol(functionExpression);
        functionSymbol = sqlExpressions.normalize(functionSymbol);
        assertThat("function expression was normalized, compile would not be hit", functionSymbol, not(instanceOf(Literal.class)));
        Function function = (Function) functionSymbol;
        Scalar scalar = (Scalar) functions.getQualified(function.info().ident());

        Scalar compiled = scalar.compile(function.arguments());
        assertThat(compiled, matcher.apply(scalar));
    }


    private static boolean allArgsAreInputs(List<Symbol> arguments) {
        for (Symbol argument : arguments) {
            if (!(argument instanceof Input)) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    protected FunctionImplementation getFunction(String functionName, DataType... argTypes) {
        return getFunction(functionName, Arrays.asList(argTypes));
    }

    @SuppressWarnings("unchecked")
    protected FunctionImplementation getFunction(String functionName, List<DataType> argTypes) {
        return functions.get(
            null, functionName, Lists2.map(argTypes, t -> new InputColumn(0, t)), SearchPath.pathWithPGCatalogAndDoc());
    }

    protected Symbol normalize(String functionName, Object value, DataType type) {
        return normalize(functionName, Literal.of(type, value));
    }

    protected Symbol normalize(CoordinatorTxnCtx coordinatorTxnCtx, String functionName, Symbol... args) {
        List<Symbol> argList = Arrays.asList(args);
        FunctionImplementation function = functions.get(null, functionName, argList, SearchPath.pathWithPGCatalogAndDoc());
        return function.normalizeSymbol(new Function(function.info(),
            argList), coordinatorTxnCtx);
    }

    protected Symbol normalize(String functionName, Symbol... args) {
        return normalize(new CoordinatorTxnCtx(SessionContext.systemSessionContext()), functionName, args);
    }

    private static class AssertMax1ValueCallInput implements Input {
        private final Input delegate;
        int calls = 0;

        AssertMax1ValueCallInput(Input delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object value() {
            calls++;
            if (calls == 1) {
                return delegate.value();
            }
            throw new AssertionError("Input.value() should only be called once");
        }

        @Override
        public String toString() {
            return delegate.toString();
        }
    }
}

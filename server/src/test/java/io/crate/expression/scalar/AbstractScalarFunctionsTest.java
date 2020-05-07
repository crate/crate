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

import io.crate.action.sql.SessionContext;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.common.collections.Lists2;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.RefReplacer;
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
import io.crate.metadata.settings.SessionSettings;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataType;
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

public abstract class AbstractScalarFunctionsTest extends CrateDummyClusterServiceUnitTest {

    protected SqlExpressions sqlExpressions;
    protected Functions functions;
    protected Map<RelationName, AnalyzedRelation> tableSources;
    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();
    private InputFactory inputFactory;

    protected static SessionSettings DUMMY_SESSION_INFO = new SessionSettings(
        "dummyUser",
        SearchPath.createSearchPathFrom("dummySchema"));

    @Before
    public void prepareFunctions() {
        String createTableStmt =
            "create table doc.users (" +
            "  id int," +
            "  name text," +
            "  tags array(text)," +
            "  age int," +
            "  a int," +
            "  ip ip," +
            "  c char," +
            "  x bigint," +
            "  shape geo_shape," +
            "  timestamp_tz timestamp with time zone," +
            "  timestamp timestamp without time zone," +
            "  timezone text," +
            "  interval text," +
            "  time_format text," +
            "  long_array array(bigint)," +
            "  int_array array(int)," +
            "  short_array array(short)," +
            "  double_array array(double precision)," +
            "  regex_pattern text," +
            "  geoshape geo_shape," +
            "  geopoint geo_point," +
            "  geostring text," +
            "  is_awesome boolean," +
            "  double_val double precision," +
            "  float_val real," +
            "  short_val smallint," +
            "  obj object," +
            "  obj_ignored object(ignored)" +
            ")";

        DocTableInfo tableInfo = SQLExecutor.tableInfo(
            new RelationName(DocSchemaInfo.NAME, "users"),
            createTableStmt,
            clusterService);

        DocTableRelation tableRelation = new DocTableRelation(tableInfo);
        tableSources = Map.of(tableInfo.ident(), tableRelation);
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
    public void assertNormalize(String functionExpression, Matcher<? super Symbol> expectedSymbol) {
        assertNormalize(functionExpression, expectedSymbol, true);
    }

    public void assertNormalize(String functionExpression, Matcher<? super Symbol> expectedSymbol, boolean evaluate) {
        // Explicit normalization happens further below
        sqlExpressions.context().allowEagerNormalize(false);
        Symbol functionSymbol = sqlExpressions.asSymbol(functionExpression);
        if (functionSymbol instanceof Literal) {
            assertThat(functionSymbol, expectedSymbol);
            return;
        }
        Function function = (Function) functionSymbol;
        var ident = function.info().ident();
        var signature = function.signature();
        FunctionImplementation impl;
        if (signature == null) {
            impl = functions.getQualified(ident);
        } else {
            impl = functions.getQualified(signature, ident.argumentTypes());
        }
        assertThat("Function implementation not found using full qualified lookup", impl, Matchers.notNullValue());

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
        sqlExpressions.context().allowEagerNormalize(true);
        Symbol functionSymbol = sqlExpressions.asSymbol(functionExpression);
        functionSymbol = sqlExpressions.normalize(functionSymbol);
        if (functionSymbol instanceof Literal) {
            Object value = ((Literal) functionSymbol).value();
            assertThat((T) value, expectedValue);
            return;
        }
        LinkedList<Literal<?>> unusedLiterals = new LinkedList<>(Arrays.asList(literals));
        Function function = (Function) RefReplacer.replaceRefs(functionSymbol, f -> {
            Literal<?> literal = unusedLiterals.pollFirst();
            if (literal == null) {
                throw new IllegalArgumentException("No value literal for field=" + f + ", please add more literals");
            }
            return literal;
        });
        var ident = function.info().ident();
        var signature = function.signature();
        Scalar scalar;
        if (signature == null) {
            scalar = (Scalar) functions.getQualified(ident);
        } else {
            scalar = (Scalar) functions.getQualified(signature, ident.argumentTypes());
        }
        assertThat("Function implementation not found using full qualified lookup", scalar, Matchers.notNullValue());

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
        var ident = function.info().ident();
        var signature = function.signature();
        Scalar scalar;
        if (signature == null) {
            scalar = (Scalar) functions.getQualified(ident);
        } else {
            scalar = (Scalar) functions.getQualified(signature, ident.argumentTypes());
        }
        assertThat("Function implementation not found using full qualified lookup", scalar, Matchers.notNullValue());

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

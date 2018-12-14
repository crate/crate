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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.data.Input;
import io.crate.expression.FunctionExpression;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.Scalar;
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
import io.crate.types.SetType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public abstract class AbstractScalarFunctionsTest extends CrateUnitTest {

    private static final InputApplier INPUT_APPLIER = new InputApplier();

    protected SqlExpressions sqlExpressions;
    protected Functions functions;
    protected Map<QualifiedName, AnalyzedRelation> tableSources;

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
            .add("obj", DataTypes.OBJECT, ImmutableList.of())
            .build();
        DocTableRelation tableRelation = new DocTableRelation(tableInfo);
        tableSources = ImmutableMap.of(new QualifiedName("users"), tableRelation);
        sqlExpressions = new SqlExpressions(tableSources);
        functions = sqlExpressions.functions();
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
            assertThat(((Scalar) impl).evaluate(inputs), is(expectedValue));
            assertThat(((Scalar) impl).compile(function.arguments()).evaluate(inputs), is(expectedValue));
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
    public <T> void assertEvaluate(String functionExpression, Matcher<T> expectedValue, Input... inputs) {
        if (expectedValue == null) {
            expectedValue = (Matcher<T>) nullValue();
        }
        Symbol functionSymbol = sqlExpressions.asSymbol(functionExpression);
        functionSymbol = sqlExpressions.normalize(functionSymbol);
        if (functionSymbol instanceof Literal) {
            Object value = ((Literal) functionSymbol).value();
            if (value instanceof BytesRef) {
                value = BytesRefs.toString(value);
            }
            assertThat((T) value, expectedValue);
            return;
        }
        Function function = (Function) functionSymbol;
        Scalar scalar = (Scalar) functions.getQualified(function.info().ident());

        InputApplierContext inputApplierContext = new InputApplierContext(inputs, sqlExpressions);
        AssertingInput[] arguments = new AssertingInput[function.arguments().size()];
        for (int i = 0; i < function.arguments().size(); i++) {
            Symbol arg = function.arguments().get(i);
            if (arg instanceof Input) {
                arguments[i] = new AssertingInput(((Input) arg));
            } else {
                arguments[i] = new AssertingInput(INPUT_APPLIER.process(arg, inputApplierContext));
            }
        }

        Object actualValue = scalar.compile(function.arguments()).evaluate((Input[]) arguments);
        // readable output for the AssertionError
        if (actualValue instanceof BytesRef) {
            actualValue = BytesRefs.toString(actualValue);
        }
        assertThat((T) actualValue, expectedValue);

        // Reset calls
        for (AssertingInput argument : arguments) {
            argument.calls = 0;
        }

        actualValue = scalar.evaluate((Input[]) arguments);
        // readable output for the AssertionError
        if (actualValue instanceof BytesRef) {
            actualValue = BytesRefs.toString(actualValue);
        }
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
    public void assertEvaluate(String functionExpression, Object expectedValue, Input... inputs) {
        if (expectedValue == null) {
            assertEvaluate(functionExpression, nullValue());
        } else if (expectedValue instanceof BytesRef) {
            assertEvaluate(functionExpression, is(BytesRefs.toString(expectedValue)), inputs);
        } else {
            assertEvaluate(functionExpression, is(expectedValue), inputs);
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
        return functions.getBuiltin(functionName, argTypes);
    }

    protected Symbol normalize(String functionName, Object value, DataType type) {
        return normalize(functionName, Literal.of(type, value));
    }

    protected Symbol normalize(TransactionContext transactionContext, String functionName, Symbol... args) {
        List<Symbol> argList = Arrays.asList(args);
        FunctionImplementation function = functions.getBuiltinByArgs(functionName, argList);
        return function.normalizeSymbol(new Function(function.info(),
            argList), transactionContext);
    }

    protected Symbol normalize(String functionName, Symbol... args) {
        return normalize(new TransactionContext(SessionContext.systemSessionContext()), functionName, args);
    }

    private class AssertingInput implements Input {
        private final Input delegate;
        int calls = 0;

        AssertingInput(Input delegate) {
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

    private static class InputApplierContext implements Iterator<Input> {

        private final Iterator<Input> inputsIterator;
        private final SqlExpressions sqlExpressions;

        InputApplierContext(Input[] inputs, SqlExpressions sqlExpressions) {
            this.inputsIterator = Arrays.asList(inputs).iterator();
            this.sqlExpressions = sqlExpressions;
        }

        public Input next() {
            return inputsIterator.next();
        }

        @Override
        public boolean hasNext() {
            return inputsIterator.hasNext();
        }

        @Override
        public void remove() {
        }
    }

    /**
     * Replace {@link Field} symbols with {@link Input} symbols found in the context.
     * This way one can use column identifiers for scalar testing while providing the (literal) inputs the
     * column should result in.
     */
    private static class InputApplier extends SymbolVisitor<InputApplierContext, Input> {

        @Override
        public Input visitLiteral(Literal symbol, InputApplierContext context) {
            return symbol;
        }

        @Override
        public Input visitField(Field field, InputApplierContext context) {
            if (!context.hasNext()) {
                return null;
            }
            return context.next();
        }

        @Override
        public Input visitFunction(Function function, InputApplierContext context) {
            List<Symbol> args = function.arguments();
            Input[] argInputs = new Input[args.size()];
            ArrayList<Symbol> newArgs = new ArrayList<>(args.size());
            for (int j = 0; j < args.size(); j++) {
                Symbol arg = args.get(j);
                Input input = arg.accept(this, context);
                argInputs[j] = input;
                // replace arguments on function for normalization
                if (input instanceof Literal) {
                    newArgs.add((Literal) argInputs[j]);
                } else {
                    newArgs.add(arg);
                }
            }
            function = new Function(function.info(), newArgs);
            try {
                return (Input) context.sqlExpressions.normalize(function);
            } catch (Exception e) {
                FunctionIdent ident = function.info().ident();
                Scalar scalar =
                    (Scalar) context.sqlExpressions.functions().getBuiltin(ident.name(), ident.argumentTypes());
                return new FunctionExpression<>(scalar, argInputs);
            }
        }
    }
}

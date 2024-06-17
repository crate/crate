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
import static io.crate.testing.Asserts.isNull;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.Before;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.common.collections.Lists;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.ParameterBinder;
import io.crate.expression.symbol.RefReplacer;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.RelationName;
import io.crate.metadata.Scalar;
import io.crate.metadata.SearchPath;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.SessionSettings;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.role.metadata.RolesHelper;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataType;

public abstract class ScalarTestCase extends CrateDummyClusterServiceUnitTest {

    protected SqlExpressions sqlExpressions;
    protected Map<RelationName, AnalyzedRelation> tableSources;
    protected TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();
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
            "  c byte," +
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
        inputFactory = new InputFactory(sqlExpressions.nodeCtx);
    }

    /**
     * Assert that the functionExpression normalizes to the expectedSymbol
     * <p>
     * If the result of normalize is a Literal and all arguments were Literals evaluate is also called and
     * compared to the result of normalize - the resulting value of normalize must match evaluate.
     */
    public void assertNormalize(String functionExpression, Consumer<? super Symbol> expectedSymbol) {
        assertNormalize(functionExpression, expectedSymbol, true);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void assertNormalize(String functionExpression, Consumer<? super Symbol> expectedSymbol, boolean evaluate) {
        // Explicit normalization happens further below
        sqlExpressions.context().allowEagerNormalize(false);
        Symbol functionSymbol = sqlExpressions.asSymbol(functionExpression);
        if (functionSymbol instanceof Literal) {
            assertThat(functionSymbol).satisfies(expectedSymbol);
            return;
        }
        Function function = (Function) functionSymbol;
        FunctionImplementation impl = sqlExpressions.nodeCtx.functions().getQualified(function);
        assertThat(impl)
            .as("Function implementation not found using full qualified lookup")
            .isNotNull();

        Symbol normalized = sqlExpressions.normalize(function);
        assertThat(normalized)
            .as(String.format(Locale.ENGLISH, "expected <%s> to normalize to %s", functionExpression, expectedSymbol))
            .satisfies(expectedSymbol);

        if (evaluate && normalized instanceof Input && allArgsAreInputs(function.arguments())) {
            Input<?>[] inputs = new Input[function.arguments().size()];
            for (int i = 0; i < inputs.length; i++) {
                inputs[i] = ((Input<?>) function.arguments().get(i));
            }
            Object expectedValue = ((Input<?>) normalized).value();
            assertThat(((Scalar) impl).evaluate(txnCtx, sqlExpressions.nodeCtx, inputs))
                .isEqualTo(expectedValue);
            assertThat(((Scalar) impl)
                           .compile(function.arguments(), "dummy", () -> List.of(Role.CRATE_USER))
                           .evaluate(txnCtx, sqlExpressions.nodeCtx, inputs))
                .isEqualTo(expectedValue);
        }
    }

    /**
     * asserts that the given functionExpression matches the given matcher.
     * If the functionExpression contains references the inputs will be used in the order the references appear.
     * <p>
     * * E.g.
     * <code>
     * assertEvaluate("foo(name, age)", "expectedValue", inputForName, inputForAge)
     * </code>
     * or
     * <code>
     * assertEvaluate("foo('literalName', age)", "expectedValue", inputForAge)
     * </code>
     */
    public void assertEvaluate(String functionExpression, Object expectedValue, Literal<?>... literals) {
        assertEvaluate(functionExpression, s -> assertThat(s).isEqualTo(expectedValue), literals);
    }

    public void assertEvaluate(String functionExpression, Object expectedValue, String errorMessage, Literal<?>... literals) {
        assertEvaluate(functionExpression, s -> assertThat(s).withFailMessage(errorMessage).isEqualTo(expectedValue), literals);
    }

    /**
     * Same as {@link #assertEvaluate(String, Object, Literal[])}, but expecting a null value.
     * It's needed, because if the above method is called with <code>null</code>, i.e.:
     * <code>
     *     assertEvaluate("myFunction(null), null, Literal.of(null));
     * </code>
     * the compiler is confused with the following method which takes a <code>Consumer<T></code> as argument.
     */
    public void assertEvaluateNull(String functionExpression, Literal<?>... literals) {
        assertEvaluate(functionExpression, isNull(), literals);
    }

    /**
     * Same as {@link #assertEvaluate(String, Object, Literal[])} but tests against a consumer a not a concrete value.
     * <p>
     * E.g.
     * <code>
     * assertEvaluate("foo(name, age)", isLiteral(Map.of("expectedKey", "expectedValue")), inputForName, inputForAge)
     * </code>
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> void assertEvaluate(String functionExpression, Consumer<T> expectedValue, Literal<?>... literals) {
        sqlExpressions.context().allowEagerNormalize(true);
        Symbol functionSymbol = sqlExpressions.asSymbol(functionExpression);
        functionSymbol = sqlExpressions.normalize(functionSymbol);
        if (functionSymbol instanceof Literal) {
            assertThat((T)((Literal<?>) functionSymbol).value()).satisfies(expectedValue);
            return;
        }
        LinkedList<Literal<?>> unusedLiterals = new LinkedList<>(Arrays.asList(literals));
        Function function = (Function) RefReplacer.replaceRefs(functionSymbol, r -> {
            if (unusedLiterals.isEmpty()) {
                throw new IllegalArgumentException("No value literal for reference=" + r + ", please add more literals");
            }
            return unusedLiterals.pollFirst(); //Can be null.
        });
        if (unusedLiterals.size() == literals.length) {
            // Currently it's supposed that literals will be either references or parameters.
            // One of replaceRefs and bindParameters does nothing and doesn't consume unusedLiterals.
            function = (Function) ParameterBinder.bindParameters(function, p -> {
                if (unusedLiterals.isEmpty()) {
                    throw new IllegalArgumentException(
                        "No value literal for parameter=" + p + ", please add more literals");
                }
                return unusedLiterals.pollFirst(); //Can be null.
            });
        }

        Scalar scalar = (Scalar) sqlExpressions.nodeCtx.functions().getQualified(function);
        assertThat(scalar)
            .as("Function implementation not found using full qualified lookup")
            .isNotNull();

        AssertMax1ValueCallInput[] arguments = new AssertMax1ValueCallInput[function.arguments().size()];
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns(txnCtx);
        for (int i = 0; i < function.arguments().size(); i++) {
            Symbol arg = function.arguments().get(i);
            Input<?> input = ctx.add(arg);
            arguments[i] = new AssertMax1ValueCallInput(input);
        }
        Object actualValue = scalar.compile(function.arguments(), "dummy", () -> List.of(Role.CRATE_USER))
            .evaluate(txnCtx, sqlExpressions.nodeCtx, arguments);
        assertThat((T) actualValue).satisfies(expectedValue);

        // Reset calls
        for (AssertMax1ValueCallInput argument : arguments) {
            argument.calls = 0;
        }

        actualValue = scalar.evaluate(txnCtx, sqlExpressions.nodeCtx, arguments);
        assertThat((T) actualValue).satisfies(expectedValue);
        if (scalar.signature().hasFeature(Scalar.Feature.NON_NULLABLE)) {
            assertThat(actualValue).isNotNull();
        }
    }

    @SuppressWarnings("rawtypes")
    public void assertCompile(String functionExpression,
                              java.util.function.Function<Scalar, Consumer<Scalar>> matcher) {
        assertCompile(functionExpression, RolesHelper.userOf("dummy"), () -> List.of(RolesHelper.userOf("dummy")), matcher);
    }

    @SuppressWarnings("rawtypes")
    public void assertCompileAsSuperUser(String functionExpression,
                                         java.util.function.Function<Scalar, Consumer<Scalar>> matcher) {
        assertCompile(functionExpression, Role.CRATE_USER, () -> List.of(Role.CRATE_USER), matcher);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void assertCompile(String functionExpression,
                              Role sessionUser,
                              Roles roles,
                              java.util.function.Function<Scalar, Consumer<Scalar>> matcher) {
        Symbol functionSymbol = sqlExpressions.asSymbol(functionExpression);
        functionSymbol = sqlExpressions.normalize(functionSymbol);
        assertThat(functionSymbol)
            .as("function expression was normalized, compile would not be hit")
            .isNotInstanceOf(Literal.class);
        Function function = (Function) functionSymbol;
        Scalar scalar = (Scalar) sqlExpressions.nodeCtx.functions().getQualified(function);
        assertThat(scalar)
            .as("Function implementation not found using full qualified lookup")
            .isNotNull();

        Scalar compiled = scalar.compile(function.arguments(), sessionUser.name(), roles);
        assertThat(compiled).satisfies(matcher.apply(scalar));
    }

    private static boolean allArgsAreInputs(List<Symbol> arguments) {
        for (Symbol argument : arguments) {
            if (!(argument instanceof Input)) {
                return false;
            }
        }
        return true;
    }

    protected FunctionImplementation getFunction(String functionName, List<DataType<?>> argTypes) {
        return sqlExpressions.nodeCtx.functions().get(
            null, functionName, Lists.map(argTypes, t -> new InputColumn(0, t)), SearchPath.pathWithPGCatalogAndDoc());
    }

    @SuppressWarnings("NewClassNamingConvention")
    private static class AssertMax1ValueCallInput<T> implements Input<T> {
        private final Input<T> delegate;
        int calls = 0;

        AssertMax1ValueCallInput(Input<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public T value() {
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

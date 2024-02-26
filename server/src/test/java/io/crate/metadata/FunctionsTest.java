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

package io.crate.metadata;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

public class FunctionsTest extends ESTestCase {

    private Map<FunctionName, List<FunctionProvider>> implementations = new HashMap<>();

    private void register(Signature signature, FunctionFactory factory) {
        List<FunctionProvider> functions = implementations.computeIfAbsent(
            signature.getName(),
            k -> new ArrayList<>());
        functions.add(new FunctionProvider(signature, factory));
    }

    private Functions createFunctions() {
        return new Functions(implementations);
    }

    private FunctionImplementation resolve(String functionName,
                                           List<Symbol> arguments) {
        return createFunctions().get(null, functionName, arguments, SearchPath.pathWithPGCatalogAndDoc());
    }

    private static class DummyFunction implements FunctionImplementation {

        private final Signature signature;

        public DummyFunction(Signature signature) {
            this.signature = signature;
        }

        @Override
        public Signature signature() {
            return signature;
        }

        @Override
        public BoundSignature boundSignature() {
            return BoundSignature.sameAsUnbound(signature);
        }
    }

    @Test
    public void test_function_name_doesnt_exists() {
        var functions = createFunctions();

        expectedException.expect(UnsupportedFunctionException.class);
        expectedException.expectMessage("Unknown function: does_not_exists()");
        functions.get(null, "does_not_exists", Collections.emptyList(), SearchPath.pathWithPGCatalogAndDoc());
    }

    @Test
    public void test_signature_matches_exact() {
        register(
            Signature.scalar(
                "foo",
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, args) ->
                new DummyFunction(signature)
        );
        var impl = resolve("foo", List.of(Literal.of("hoschi")));
        assertThat(impl.boundSignature().argTypes(), contains(DataTypes.STRING));
    }

    @Test
    public void test_signature_matches_with_coercion() {
        register(
            Signature.scalar(
                "foo",
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, args) ->
                new DummyFunction(signature)
        );
        var impl = resolve("foo", List.of(Literal.of(1L)));
        assertThat(impl.boundSignature().argTypes(), contains(DataTypes.INTEGER));
    }

    @Test
    public void test_signature_matches_with_coercion_and_precedence() {
        register(
            Signature.scalar(
                "foo",
                DataTypes.DOUBLE.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature()
            ),
            (signature, args) ->
                new DummyFunction(signature)
        );
        register(
            Signature.scalar(
                "foo",
                DataTypes.FLOAT.getTypeSignature(),
                DataTypes.FLOAT.getTypeSignature()
            ),
            (signature, args) ->
                new DummyFunction(signature)
        );

        var impl = resolve("foo", List.of(Literal.of(1L)));

        // float is more specific than double
        assertThat(impl.boundSignature().argTypes(), contains(DataTypes.FLOAT));
    }

    @Test
    public void test_multiple_signature_matches_with_same_return_type_results_in_first_selected() {
        register(
            Signature.scalar(
                "foo",
                DataTypes.STRING.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ),
            (signature, args) ->
                new DummyFunction(signature)
        );
        register(
            Signature.scalar(
                "foo",
                TypeSignature.parse("array(E)"),
                DataTypes.INTEGER.getTypeSignature()
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, args) ->
                new DummyFunction(signature)
        );

        var impl = resolve("foo", List.of(Literal.of(DataTypes.UNDEFINED, null)));
        assertThat(impl.boundSignature().argTypes(), contains(DataTypes.STRING));
    }

    @Test
    public void test_checks_built_in_and_udf_per_search_path_schema() throws Exception {
        register(
            Signature.scalar(
                new FunctionName("schema1", "foo"),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ),
            (signature, args) ->
                new DummyFunction(signature)
        );
        Functions functions = createFunctions();
        Signature signature = Signature.scalar(
            new FunctionName("schema2", "foo"),
            DataTypes.STRING.getTypeSignature(),
            DataTypes.INTEGER.getTypeSignature()
        );
        functions.registerUdfFunctionImplementationsForSchema(
            "schema2",
            Map.of(
                new FunctionName("schema2", "foo"),
                List.of(new FunctionProvider(signature, (sig, args) -> new DummyFunction(sig)))
            )
        );

        SearchPath searchPath = SearchPath.createSearchPathFrom("schema2", "schema1");
        var implementation = functions.get(null, "foo", List.of(Literal.of("foo")), searchPath);
        assertThat(implementation.signature()).isEqualTo(signature);
    }

    @Test
    public void test_multiple_signature_matches_with_undefined_arguments_only_result_in_first_selected() {
        register(
            Signature.scalar(
                "foo",
                DataTypes.STRING.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ),
            (signature, args) ->
                new DummyFunction(signature)
        );
        register(
            Signature.scalar(
                "foo",
                TypeSignature.parse("array(E)"),
                TypeSignature.parse("array(E)")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, args) ->
                new DummyFunction(signature)
        );

        var impl = resolve("foo", List.of(Literal.of(DataTypes.UNDEFINED, null)));
        assertThat(impl.boundSignature().argTypes(), contains(DataTypes.STRING));
    }

    @Test
    public void test_signature_with_more_exact_argument_matches_is_more_specific() {
        // We had a regression where this worked for 2 signatures (1 matching, 1 not matching)
        // but not with multiple not matching signatures. So lets use at least 2 not matching.
        register(
            Signature.scalar(
                "foo",
                DataTypes.BOOLEAN.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ),
            (signature, args) ->
                new DummyFunction(signature)
        );
        register(
            Signature.scalar(
                "foo",
                DataTypes.STRING.getTypeSignature(),
                DataTypes.SHORT.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ),
            (signature, args) ->
                new DummyFunction(signature)
        );
        register(
            Signature.scalar(
                "foo",
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ),
            (signature, args) ->
                new DummyFunction(signature)
        );

        var impl = resolve("foo", List.of(Literal.of(1), Literal.of(1L)));
        assertThat(impl.boundSignature().argTypes(), contains(DataTypes.INTEGER, DataTypes.INTEGER));
    }

    @Test
    public void test_bwc_get_qualified_function_without_signature() {
        var signature = Signature.scalar(
            "foo",
            DataTypes.STRING.getTypeSignature(),
            DataTypes.INTEGER.getTypeSignature()
        );
        var dummyFunction = new DummyFunction(signature);

        register(
            signature,
            (s, args) ->
                dummyFunction
        );

        var func = new Function(dummyFunction.signature(), List.of(Literal.of("hoschi")), DataTypes.INTEGER);
        var funcImpl = createFunctions().getQualified(func);

        assertThat(funcImpl, is(dummyFunction));
    }

    @Test
    public void test_bwc_get_qualified_aggregation_without_signature() {
        var signature = Signature.scalar(
            "foo",
            DataTypes.STRING.getTypeSignature(),
            DataTypes.INTEGER.getTypeSignature()
        );
        var dummyFunction = new DummyFunction(signature);

        register(
            signature,
            (s, args) ->
                dummyFunction
        );

        var agg = new Aggregation(dummyFunction.signature, DataTypes.STRING, List.of(Literal.of("hoschi")));
        var funcImpl = createFunctions().getQualified(agg);

        assertThat(funcImpl, is(dummyFunction));
    }

    @Test
    public void test_unknown_function_with_no_arguments_and_candidates() {
        expectedException.expectMessage("Unknown function: foo.bar()");
        Functions.raiseUnknownFunction("foo", "bar", List.of(), List.of());
    }

    @Test
    public void test_unknown_function_with_arguments_no_candidates() {
        expectedException.expectMessage("Unknown function: foo.bar(1, 2)");
        Functions.raiseUnknownFunction("foo", "bar", List.of(Literal.of(1), Literal.of(2)), List.of());
    }

    @Test
    public void test_unknown_function_no_arguments_but_candidates() {
        expectedException.expectMessage("Unknown function: foo.bar()." +
                                        " Possible candidates: foo.bar(text):text, foo.bar(double precision):double precision");
        Functions.raiseUnknownFunction(
            "foo",
            "bar",
            List.of(),
            List.of(
                new FunctionProvider(
                    Signature.scalar(new FunctionName("foo", "bar"),
                                     DataTypes.STRING.getTypeSignature(),
                                     DataTypes.STRING.getTypeSignature()),
                    ((signature, dataTypes) -> null)
                ),
                new FunctionProvider(
                    Signature.scalar(new FunctionName("foo", "bar"),
                                     DataTypes.DOUBLE.getTypeSignature(),
                                     DataTypes.DOUBLE.getTypeSignature()),
                    ((signature, dataTypes) -> null)
                )
            )
        );
    }

    @Test
    public void test_unknown_function_with_arguments_and_candidates() {
        expectedException.expectMessage("Unknown function: foo.bar(1, 2)," +
                                        " no overload found for matching argument types: (integer, integer)." +
                                        " Possible candidates: foo.bar(text):text, foo.bar(double precision):double precision");
        Functions.raiseUnknownFunction(
            "foo",
            "bar",
            List.of(Literal.of(1), Literal.of(2)),
            List.of(
                new FunctionProvider(
                    Signature.scalar(new FunctionName("foo", "bar"),
                                     DataTypes.STRING.getTypeSignature(),
                                     DataTypes.STRING.getTypeSignature()),
                    ((signature, dataTypes) -> null)
                ),
                new FunctionProvider(
                    Signature.scalar(new FunctionName("foo", "bar"),
                                     DataTypes.DOUBLE.getTypeSignature(),
                                     DataTypes.DOUBLE.getTypeSignature()),
                    ((signature, dataTypes) -> null)
                )
            )
        );
    }
}

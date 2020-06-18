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

package io.crate.metadata;

import io.crate.common.collections.Lists2;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.functions.Signature;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class FunctionsTest extends CrateUnitTest {

    private Map<FunctionName, List<FunctionProvider>> implementations = new HashMap<>();

    private void register(Signature signature,
                          BiFunction<Signature, List<DataType>, FunctionImplementation> factory) {
        List<FunctionProvider> functions = implementations.computeIfAbsent(
            signature.getName(),
            k -> new ArrayList<>());
        functions.add(new FunctionProvider(signature, factory));
    }

    private Functions createFunctions() {
        return new Functions(implementations);
    }

    private FunctionImplementation resolve(String functionName,
                                           List<? extends Symbol> arguments) {
        return createFunctions().get(null, functionName, arguments, SearchPath.pathWithPGCatalogAndDoc());
    }

    private static class DummyFunction implements FunctionImplementation {

        private final Signature signature;
        private final FunctionInfo info;

        public DummyFunction(Signature signature) {
            this.signature = signature;
            this.info = new FunctionInfo(
                new FunctionIdent(
                    signature.getName(),
                    Lists2.map(signature.getArgumentTypes(), TypeSignature::createType)
                ),
                signature.getReturnType().createType()
            );
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        @Override
        public Signature signature() {
            return signature;
        }
    }

    @Test
    public void test_function_name_doesnt_exists() {
        var functions = createFunctions();

        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("unknown function: does_not_exists()");
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
        assertThat(impl.info().ident().argumentTypes(), contains(DataTypes.STRING));
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
        assertThat(impl.info().ident().argumentTypes(), contains(DataTypes.INTEGER));
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
        assertThat(impl.info().ident().argumentTypes(), contains(DataTypes.FLOAT));
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
                parseTypeSignature("array(E)"),
                DataTypes.INTEGER.getTypeSignature()
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, args) ->
                new DummyFunction(signature)
        );

        var impl = resolve("foo", List.of(Literal.of(DataTypes.UNDEFINED, null)));
        assertThat(impl.info().ident().argumentTypes(), contains(DataTypes.STRING));
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
                parseTypeSignature("array(E)"),
                parseTypeSignature("array(E)")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, args) ->
                new DummyFunction(signature)
        );

        var impl = resolve("foo", List.of(Literal.of(DataTypes.UNDEFINED, null)));
        assertThat(impl.info().ident().argumentTypes(), contains(DataTypes.STRING));
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
        assertThat(impl.info().ident().argumentTypes(), contains(DataTypes.INTEGER, DataTypes.INTEGER));
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

        var func = new Function(dummyFunction.info, null, List.of(Literal.of("hoschi")));
        var funcImpl = createFunctions().getQualified(func, SearchPath.pathWithPGCatalogAndDoc());

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

        var agg = new Aggregation(dummyFunction.info, null, DataTypes.STRING, List.of(Literal.of("hoschi")));
        var funcImpl = createFunctions().getQualified(agg, SearchPath.pathWithPGCatalogAndDoc());

        assertThat(funcImpl, is(dummyFunction));
    }
}

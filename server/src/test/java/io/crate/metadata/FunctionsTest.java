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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
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
import io.crate.types.UndefinedType;

public class FunctionsTest extends ESTestCase {

    private final Functions.Builder functionsBuilder = new Functions.Builder();

    private FunctionImplementation resolve(String functionName,
                                           List<Symbol> arguments) {
        return functionsBuilder.build().get(null, functionName, arguments, SearchPath.pathWithPGCatalogAndDoc());
    }

    private static class DummyFunction implements FunctionImplementation {

        private final Signature signature;
        private final BoundSignature boundSignature;

        public DummyFunction(Signature signature, BoundSignature boundSignature) {
            this.signature = signature;
            this.boundSignature = boundSignature;
        }

        public DummyFunction(Signature signature) {
            this(signature, BoundSignature.sameAsUnbound(signature));
        }

        @Override
        public Signature signature() {
            return signature;
        }

        @Override
        public BoundSignature boundSignature() {
            return boundSignature;
        }
    }

    @Test
    public void test_function_name_doesnt_exists() {
        var functions = functionsBuilder.build();
        assertThatThrownBy(()
            -> functions.get(null, "does_not_exists", Collections.emptyList(), SearchPath.pathWithPGCatalogAndDoc())
        ).isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessage("Unknown function: does_not_exists()");
    }

    @Test
    public void test_signature_matches_exact() {
        functionsBuilder.add(
            Signature.builder("foo", FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            (signature, args) -> new DummyFunction(signature));
        var impl = resolve("foo", List.of(Literal.of("hoschi")));
        assertThat(impl.boundSignature().argTypes()).containsExactly(DataTypes.STRING);
    }

    @Test
    public void test_signature_matches_with_coercion() {
        functionsBuilder.add(
            Signature.builder("foo", FunctionType.SCALAR)
                .argumentTypes(DataTypes.INTEGER.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            (signature, args) -> new DummyFunction(signature));
        var impl = resolve("foo", List.of(Literal.of(1L)));
        assertThat(impl.boundSignature().argTypes()).containsExactly(DataTypes.INTEGER);
    }

    @Test
    public void test_signature_matches_with_coercion_and_precedence() {
        functionsBuilder.add(
            Signature.builder("foo", FunctionType.SCALAR)
                .argumentTypes(DataTypes.DOUBLE.getTypeSignature())
                .returnType(DataTypes.DOUBLE.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            (signature, args) -> new DummyFunction(signature));
        functionsBuilder.add(
            Signature.builder("foo", FunctionType.SCALAR)
                .argumentTypes(DataTypes.FLOAT.getTypeSignature())
                .returnType(DataTypes.FLOAT.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            (signature, args) -> new DummyFunction(signature));

        var impl = resolve("foo", List.of(Literal.of(1L)));

        // float is more specific than double
        assertThat(impl.boundSignature().argTypes()).containsExactly(DataTypes.FLOAT);
    }

    @Test
    public void test_multiple_signature_matches_with_same_return_type_results_in_first_selected() {
        functionsBuilder.add(
            Signature.builder("foo", FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.INTEGER.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            (signature, args) -> new DummyFunction(signature));
        functionsBuilder.add(
            Signature.builder("foo", FunctionType.SCALAR)
                .argumentTypes(TypeSignature.parse("array(E)"))
                .returnType(DataTypes.INTEGER.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .typeVariableConstraints(typeVariable("E"))
                .build(),
            (signature, args) -> new DummyFunction(signature));

        var impl = resolve("foo", List.of(Literal.of(DataTypes.UNDEFINED, null)));
        assertThat(impl.boundSignature().argTypes()).containsExactly(DataTypes.STRING);
    }

    @Test
    public void test_checks_built_in_and_udf_per_search_path_schema() throws Exception {
        functionsBuilder.add(
            Signature.builder(new FunctionName("schema1", "foo"), FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.INTEGER.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            (signature, args) -> new DummyFunction(signature));
        Functions functions = functionsBuilder.build();
        Signature signature = Signature.builder(new FunctionName("schema2", "foo"), FunctionType.SCALAR)
            .argumentTypes(DataTypes.STRING.getTypeSignature())
            .returnType(DataTypes.INTEGER.getTypeSignature())
            .features(Scalar.Feature.DETERMINISTIC)
            .build();
        functions.setUDFs(
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
        functionsBuilder.add(
            Signature.builder("foo", FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.INTEGER.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            (signature, args) -> new DummyFunction(signature));
        functionsBuilder.add(
            Signature.builder("foo", FunctionType.SCALAR)
                .argumentTypes(TypeSignature.parse("array(E)"))
                .returnType(TypeSignature.parse("array(E)"))
                .features(Scalar.Feature.DETERMINISTIC)
                .typeVariableConstraints(typeVariable("E"))
                .build(),
            (signature, args) -> new DummyFunction(signature));

        var impl = resolve("foo", List.of(Literal.of(DataTypes.UNDEFINED, null)));
        assertThat(impl.boundSignature().argTypes()).containsExactly(DataTypes.STRING);
    }

    @Test
    public void test_signature_with_more_exact_argument_matches_is_more_specific() {
        // We had a regression where this worked for 2 signatures (1 matching, 1 not matching)
        // but not with multiple not matching signatures. So lets use at least 2 not matching.
        functionsBuilder.add(
            Signature.builder("foo", FunctionType.SCALAR)
                .argumentTypes(DataTypes.BOOLEAN.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.INTEGER.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            (signature, args) -> new DummyFunction(signature));
        functionsBuilder.add(
            Signature.builder("foo", FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature(),
                    DataTypes.SHORT.getTypeSignature())
                .returnType(DataTypes.INTEGER.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            (signature, args) -> new DummyFunction(signature));
        functionsBuilder.add(
            Signature.builder("foo", FunctionType.SCALAR)
                .argumentTypes(DataTypes.INTEGER.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature())
                .returnType(DataTypes.INTEGER.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            (signature, args) -> new DummyFunction(signature));

        var impl = resolve("foo", List.of(Literal.of(1), Literal.of(1L)));
        assertThat(impl.boundSignature().argTypes()).containsExactly(DataTypes.INTEGER, DataTypes.INTEGER);
    }

    @Test
    public void test_most_specific_will_not_remove_specific_candidates() {
        functionsBuilder.add(
            Signature.builder("foo", FunctionType.SCALAR)
                .argumentTypes(
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature()
                )
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            DummyFunction::new);
        functionsBuilder.add(
            Signature.builder("foo", FunctionType.SCALAR)
                .argumentTypes(
                    TypeSignature.parse("array(E)"),
                    TypeSignature.parse("array(E)"))
                .returnType(TypeSignature.parse("array(E)"))
                .typeVariableConstraints(typeVariable("E"))
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            DummyFunction::new);
        functionsBuilder.add(
            Signature.builder("foo", FunctionType.SCALAR)
                .argumentTypes(
                    TypeSignature.parse("array(E)"),
                    TypeSignature.parse("E"))
                .returnType(TypeSignature.parse("array(E)"))
                .typeVariableConstraints(typeVariable("E"))
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            DummyFunction::new);

        var impl = resolve("foo", List.of(
            Literal.of(UndefinedType.INSTANCE, null),
            Literal.of(UndefinedType.INSTANCE, null)
        ));

        // the first 2 functions are equally specific, but the first one should be chosen
        assertThat(impl.boundSignature().argTypes()).containsExactly(DataTypes.STRING, DataTypes.STRING);
    }

    @Test
    public void test_bwc_get_qualified_function_without_signature() {
        var signature = Signature.builder("foo", FunctionType.SCALAR)
            .argumentTypes(DataTypes.STRING.getTypeSignature())
            .returnType(DataTypes.INTEGER.getTypeSignature())
            .features(Scalar.Feature.DETERMINISTIC)
            .build();
        var dummyFunction = new DummyFunction(signature);

        functionsBuilder.add(signature, (s, args) -> dummyFunction);

        var func = new Function(dummyFunction.signature(), List.of(Literal.of("hoschi")), DataTypes.INTEGER);
        var funcImpl = functionsBuilder.build().getQualified(func);

        assertThat(funcImpl).isEqualTo(dummyFunction);
    }

    @Test
    public void test_bwc_get_qualified_aggregation_without_signature() {
        var signature = Signature.builder("foo", FunctionType.SCALAR)
            .argumentTypes(DataTypes.STRING.getTypeSignature())
            .returnType(DataTypes.INTEGER.getTypeSignature())
            .features(Scalar.Feature.DETERMINISTIC)
            .build();
        var dummyFunction = new DummyFunction(signature);

        functionsBuilder.add(signature, (s, args) -> dummyFunction);

        var agg = new Aggregation(dummyFunction.signature, DataTypes.STRING, List.of(Literal.of("hoschi")));
        var funcImpl = functionsBuilder.build().getQualified(agg);

        assertThat(funcImpl).isEqualTo(dummyFunction);
    }

    @Test
    public void test_unknown_function_with_no_arguments_and_candidates() {
        assertThatThrownBy(() -> Functions.raiseUnknownFunction("foo", "bar", List.of(), List.of()))
            .hasMessage("Unknown function: foo.bar()");
    }

    @Test
    public void test_unknown_function_with_arguments_no_candidates() {
        assertThatThrownBy(() ->
            Functions.raiseUnknownFunction("foo", "bar", List.of(Literal.of(1), Literal.of(2)), List.of())
        ).hasMessage("Unknown function: foo.bar(1, 2)");
    }

    @Test
    public void test_unknown_function_no_arguments_but_candidates() {
        assertThatThrownBy(() ->
            Functions.raiseUnknownFunction(
                "foo",
                "bar",
                List.of(),
                List.of(
                    new FunctionProvider(
                        Signature.builder(new FunctionName("foo", "bar"), FunctionType.SCALAR)
                            .argumentTypes(DataTypes.STRING.getTypeSignature())
                            .returnType(DataTypes.STRING.getTypeSignature())
                            .features(Scalar.Feature.DETERMINISTIC)
                            .build(),
                        ((_, _) -> null)
                    ),
                    new FunctionProvider(
                        Signature.builder(new FunctionName("foo", "bar"), FunctionType.SCALAR)
                            .argumentTypes(DataTypes.DOUBLE.getTypeSignature())
                            .returnType(DataTypes.DOUBLE.getTypeSignature())
                            .features(Scalar.Feature.DETERMINISTIC)
                            .build(),
                        ((_, _) -> null)
                    )
                )
            )
        ).hasMessage(
            "Invalid arguments in: foo.bar(). Valid types: (text), (double precision)");
    }

    @Test
    public void test_unknown_function_with_arguments_and_candidates() {
        assertThatThrownBy(() ->
            Functions.raiseUnknownFunction(
                "foo",
                "bar",
                List.of(Literal.of(1), Literal.of(2)),
                List.of(
                    new FunctionProvider(
                        Signature.builder(new FunctionName("foo", "bar"), FunctionType.SCALAR)
                            .argumentTypes(DataTypes.STRING.getTypeSignature())
                            .returnType(DataTypes.STRING.getTypeSignature())
                            .features(Scalar.Feature.DETERMINISTIC)
                            .build(),
                        ((_, _) -> null)
                    ),
                    new FunctionProvider(
                        Signature.builder(new FunctionName("foo", "bar"), FunctionType.SCALAR)
                            .argumentTypes(DataTypes.DOUBLE.getTypeSignature())
                            .returnType(DataTypes.DOUBLE.getTypeSignature())
                            .features(Scalar.Feature.DETERMINISTIC)
                            .build(),
                        ((_, _) -> null)
                    )
                )
            )
        ).hasMessage(
            "Invalid arguments in: foo.bar(1, 2) with (integer, integer). Valid types: (text), (double precision)");
    }
}

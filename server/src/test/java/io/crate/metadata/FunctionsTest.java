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

public class FunctionsTest extends ESTestCase {

    private final Functions.Builder functionsBuilder = new Functions.Builder();

    private FunctionImplementation resolve(String functionName,
                                           List<Symbol> arguments) {
        return functionsBuilder.build().get(null, functionName, arguments, SearchPath.pathWithPGCatalogAndDoc());
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
        var functions = functionsBuilder.build();
        assertThatThrownBy(()
            -> functions.get(null, "does_not_exists", Collections.emptyList(), SearchPath.pathWithPGCatalogAndDoc())
        ).isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessage("Unknown function: does_not_exists()");
    }

    @Test
    public void test_signature_matches_exact() {
        functionsBuilder.add(
            Signature.scalar(
                "foo",
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ).withFeature(Scalar.Feature.DETERMINISTIC),
            (signature, args) -> new DummyFunction(signature));
        var impl = resolve("foo", List.of(Literal.of("hoschi")));
        assertThat(impl.boundSignature().argTypes()).containsExactly(DataTypes.STRING);
    }

    @Test
    public void test_signature_matches_with_coercion() {
        functionsBuilder.add(
            Signature.scalar(
                "foo",
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ).withFeature(Scalar.Feature.DETERMINISTIC),
            (signature, args) -> new DummyFunction(signature));
        var impl = resolve("foo", List.of(Literal.of(1L)));
        assertThat(impl.boundSignature().argTypes()).containsExactly(DataTypes.INTEGER);
    }

    @Test
    public void test_signature_matches_with_coercion_and_precedence() {
        functionsBuilder.add(
            Signature.scalar(
                "foo",
                DataTypes.DOUBLE.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature()
            ).withFeature(Scalar.Feature.DETERMINISTIC),
            (signature, args) -> new DummyFunction(signature));
        functionsBuilder.add(
            Signature.scalar(
                "foo",
                DataTypes.FLOAT.getTypeSignature(),
                DataTypes.FLOAT.getTypeSignature()
            ).withFeature(Scalar.Feature.DETERMINISTIC),
            (signature, args) -> new DummyFunction(signature));

        var impl = resolve("foo", List.of(Literal.of(1L)));

        // float is more specific than double
        assertThat(impl.boundSignature().argTypes()).containsExactly(DataTypes.FLOAT);
    }

    @Test
    public void test_multiple_signature_matches_with_same_return_type_results_in_first_selected() {
        functionsBuilder.add(
            Signature.scalar(
                "foo",
                DataTypes.STRING.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ).withFeature(Scalar.Feature.DETERMINISTIC),
            (signature, args) -> new DummyFunction(signature));
        functionsBuilder.add(
            Signature.scalar(
                    "foo",
                    TypeSignature.parse("array(E)"),
                    DataTypes.INTEGER.getTypeSignature()
                ).withFeature(Scalar.Feature.DETERMINISTIC)
                .withTypeVariableConstraints(typeVariable("E")),
            (signature, args) -> new DummyFunction(signature));

        var impl = resolve("foo", List.of(Literal.of(DataTypes.UNDEFINED, null)));
        assertThat(impl.boundSignature().argTypes()).containsExactly(DataTypes.STRING);
    }

    @Test
    public void test_checks_built_in_and_udf_per_search_path_schema() throws Exception {
        functionsBuilder.add(
            Signature.scalar(
                new FunctionName("schema1", "foo"),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ).withFeature(Scalar.Feature.DETERMINISTIC),
            (signature, args) -> new DummyFunction(signature));
        Functions functions = functionsBuilder.build();
        Signature signature = Signature.scalar(
            new FunctionName("schema2", "foo"),
            DataTypes.STRING.getTypeSignature(),
            DataTypes.INTEGER.getTypeSignature()
        ).withFeature(Scalar.Feature.DETERMINISTIC);
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
            Signature.scalar(
                "foo",
                DataTypes.STRING.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ).withFeature(Scalar.Feature.DETERMINISTIC),
            (signature, args) -> new DummyFunction(signature));
        functionsBuilder.add(
            Signature.scalar(
                    "foo",
                    TypeSignature.parse("array(E)"),
                    TypeSignature.parse("array(E)")
                ).withTypeVariableConstraints(typeVariable("E"))
                .withFeature(Scalar.Feature.DETERMINISTIC),
            (signature, args) -> new DummyFunction(signature));

        var impl = resolve("foo", List.of(Literal.of(DataTypes.UNDEFINED, null)));
        assertThat(impl.boundSignature().argTypes()).containsExactly(DataTypes.STRING);
    }

    @Test
    public void test_signature_with_more_exact_argument_matches_is_more_specific() {
        // We had a regression where this worked for 2 signatures (1 matching, 1 not matching)
        // but not with multiple not matching signatures. So lets use at least 2 not matching.
        functionsBuilder.add(
            Signature.scalar(
                "foo",
                DataTypes.BOOLEAN.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ).withFeature(Scalar.Feature.DETERMINISTIC),
            (signature, args) -> new DummyFunction(signature));
        functionsBuilder.add(
            Signature.scalar(
                "foo",
                DataTypes.STRING.getTypeSignature(),
                DataTypes.SHORT.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ).withFeature(Scalar.Feature.DETERMINISTIC),
            (signature, args) -> new DummyFunction(signature));
        functionsBuilder.add(
            Signature.scalar(
                "foo",
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ).withFeature(Scalar.Feature.DETERMINISTIC),
            (signature, args) -> new DummyFunction(signature));

        var impl = resolve("foo", List.of(Literal.of(1), Literal.of(1L)));
        assertThat(impl.boundSignature().argTypes()).containsExactly(DataTypes.INTEGER, DataTypes.INTEGER);
    }

    @Test
    public void test_bwc_get_qualified_function_without_signature() {
        var signature = Signature.scalar(
            "foo",
            DataTypes.STRING.getTypeSignature(),
            DataTypes.INTEGER.getTypeSignature()
        ).withFeature(Scalar.Feature.DETERMINISTIC);
        var dummyFunction = new DummyFunction(signature);

        functionsBuilder.add(signature, (s, args) -> dummyFunction);

        var func = new Function(dummyFunction.signature(), List.of(Literal.of("hoschi")), DataTypes.INTEGER);
        var funcImpl = functionsBuilder.build().getQualified(func);

        assertThat(funcImpl).isEqualTo(dummyFunction);
    }

    @Test
    public void test_bwc_get_qualified_aggregation_without_signature() {
        var signature = Signature.scalar(
            "foo",
            DataTypes.STRING.getTypeSignature(),
            DataTypes.INTEGER.getTypeSignature()
        ).withFeature(Scalar.Feature.DETERMINISTIC);
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
                        Signature.scalar(new FunctionName("foo", "bar"),
                                        DataTypes.STRING.getTypeSignature(),
                                        DataTypes.STRING.getTypeSignature())
                            .withFeature(Scalar.Feature.DETERMINISTIC),
                        ((signature, dataTypes) -> null)
                    ),
                    new FunctionProvider(
                        Signature.scalar(new FunctionName("foo", "bar"),
                                        DataTypes.DOUBLE.getTypeSignature(),
                                        DataTypes.DOUBLE.getTypeSignature())
                            .withFeature(Scalar.Feature.DETERMINISTIC),
                        ((signature, dataTypes) -> null)
                    )
                )
            )
        ).hasMessage(
            "Unknown function: foo.bar()." +
            " Possible candidates: foo.bar(text):text, foo.bar(double precision):double precision");
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
                        Signature.scalar(new FunctionName("foo", "bar"),
                                        DataTypes.STRING.getTypeSignature(),
                                        DataTypes.STRING.getTypeSignature())
                            .withFeature(Scalar.Feature.DETERMINISTIC),
                        ((signature, dataTypes) -> null)
                    ),
                    new FunctionProvider(
                        Signature.scalar(new FunctionName("foo", "bar"),
                                        DataTypes.DOUBLE.getTypeSignature(),
                                        DataTypes.DOUBLE.getTypeSignature())
                            .withFeature(Scalar.Feature.DETERMINISTIC),
                        ((signature, dataTypes) -> null)
                    )
                )
            )
        ).hasMessage(
            "Unknown function: foo.bar(1, 2)," +
            " no overload found for matching argument types: (integer, integer)." +
            " Possible candidates: foo.bar(text):text, foo.bar(double precision):double precision");
    }
}

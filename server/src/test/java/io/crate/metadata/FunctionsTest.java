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
import java.util.Comparator;
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
import io.crate.testing.TestingHelpers;
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
            ),
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
            ),
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
            ),
            (signature, args) -> new DummyFunction(signature));
        functionsBuilder.add(
            Signature.scalar(
                "foo",
                DataTypes.FLOAT.getTypeSignature(),
                DataTypes.FLOAT.getTypeSignature()
            ),
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
            ),
            (signature, args) -> new DummyFunction(signature));
        functionsBuilder.add(
            Signature.scalar(
                "foo",
                TypeSignature.parse("array(E)"),
                DataTypes.INTEGER.getTypeSignature()
            ).withTypeVariableConstraints(typeVariable("E")),
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
            ),
            (signature, args) -> new DummyFunction(signature));
        Functions functions = functionsBuilder.build();
        Signature signature = Signature.scalar(
            new FunctionName("schema2", "foo"),
            DataTypes.STRING.getTypeSignature(),
            DataTypes.INTEGER.getTypeSignature()
        );
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
            ), (signature, args) -> new DummyFunction(signature));
        functionsBuilder.add(
            Signature.scalar(
                "foo",
                TypeSignature.parse("array(E)"),
                TypeSignature.parse("array(E)")
            ).withTypeVariableConstraints(typeVariable("E")),
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
            ),
            (signature, args) -> new DummyFunction(signature));
        functionsBuilder.add(
            Signature.scalar(
                "foo",
                DataTypes.STRING.getTypeSignature(),
                DataTypes.SHORT.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ),
            (signature, args) -> new DummyFunction(signature));
        functionsBuilder.add(
            Signature.scalar(
                "foo",
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ),
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
        );
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
        );
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
            )
        ).hasMessage(
            "Unknown function: foo.bar(1, 2)," +
            " no overload found for matching argument types: (integer, integer)." +
            " Possible candidates: foo.bar(text):text, foo.bar(double precision):double precision");
    }

    @Test
    public void test_all_functions_signatures_and_features_are_consistent() {
        final NodeContext nodeContext = TestingHelpers.createNodeContext();
        final Functions functions = nodeContext.functions();
        Map<FunctionName, List<FunctionProvider>> functionImplementations = functions.getFunctionImplementations();
        var sortedFuncImpls = functionImplementations.entrySet().stream()
            .sorted(Comparator.comparing(e -> e.getKey().name()))
            .map(Map.Entry::getValue).toList();

        StringBuilder sb = new StringBuilder();
        for (List<FunctionProvider> functionProviders : sortedFuncImpls) {
            for (FunctionProvider functionProvider : functionProviders) {
                Signature signature = functionProvider.signature();
                sb.append(signature.toString());
                sb.append(signature.isDeterministic() ? " deterministic" : " non-deterministic");
                if (signature.hasFeature(Scalar.Feature.NON_NULLABLE)) {
                    sb.append(" non-nullable");
                } else if (signature.hasFeature(Scalar.Feature.NULLABLE)) {
                    sb.append(" nullable");
                } else {
                    sb.append(" default");
                }
                if (signature.hasFeature(Scalar.Feature.LAZY_ATTRIBUTES)) {
                    sb.append(" lazy-attributes");
                }
                if (signature.hasFeature(Scalar.Feature.COMPARISON_REPLACEMENT)) {
                    sb.append(" comparison-replacement");
                }
                sb.append("\n");
            }
        }

        final String allFunctionSignatures = """
            FunctionName{schema='null', name='_all_<'}<E>(E,array(E)):boolean deterministic nullable
            FunctionName{schema='null', name='_all_<='}<E>(E,array(E)):boolean deterministic nullable
            FunctionName{schema='null', name='_all_<>'}<E>(E,array(E)):boolean deterministic nullable
            FunctionName{schema='null', name='_all_='}<E>(E,array(E)):boolean deterministic nullable
            FunctionName{schema='null', name='_all_>'}<E>(E,array(E)):boolean deterministic nullable
            FunctionName{schema='null', name='_all_>='}<E>(E,array(E)):boolean deterministic nullable
            FunctionName{schema='null', name='_array'}<E>(E):array(E) deterministic non-nullable
            FunctionName{schema='null', name='_cast'}<E>(E,text):undefined deterministic default
            FunctionName{schema='null', name='_exists'}<E>(array(E)):boolean deterministic nullable
            FunctionName{schema='null', name='_map'}<V>(text,V):object(text,V) deterministic default
            FunctionName{schema='null', name='_negate'}(double precision):double precision deterministic nullable
            FunctionName{schema='null', name='_negate'}(real):real deterministic nullable
            FunctionName{schema='null', name='_negate'}(integer):integer deterministic nullable
            FunctionName{schema='null', name='_negate'}(bigint):bigint deterministic nullable
            FunctionName{schema='null', name='_negate'}(smallint):smallint deterministic nullable
            FunctionName{schema='null', name='_negate'}(numeric):numeric deterministic nullable
            FunctionName{schema='information_schema', name='_pg_expandarray'}<E>(array(E)):record("x" E,"n" integer) deterministic non-nullable
            FunctionName{schema='null', name='_subscript_record'}(record,text):undefined deterministic default
            FunctionName{schema='null', name='_values'}<E>(array(E)):record deterministic default
            FunctionName{schema='null', name='abs'}(double precision):double precision deterministic nullable
            FunctionName{schema='null', name='abs'}(real):real deterministic nullable
            FunctionName{schema='null', name='abs'}(byte):byte deterministic nullable
            FunctionName{schema='null', name='abs'}(smallint):smallint deterministic nullable
            FunctionName{schema='null', name='abs'}(integer):integer deterministic nullable
            FunctionName{schema='null', name='abs'}(bigint):bigint deterministic nullable
            FunctionName{schema='null', name='acos'}(double precision):double precision deterministic nullable
            FunctionName{schema='null', name='add'}(integer,integer):integer deterministic nullable comparison-replacement
            FunctionName{schema='null', name='add'}(bigint,bigint):bigint deterministic nullable comparison-replacement
            FunctionName{schema='null', name='add'}(timestamp without time zone,timestamp without time zone):timestamp without time zone deterministic nullable comparison-replacement
            FunctionName{schema='null', name='add'}(timestamp with time zone,timestamp with time zone):timestamp with time zone deterministic nullable comparison-replacement
            FunctionName{schema='null', name='add'}(real,real):real deterministic nullable comparison-replacement
            FunctionName{schema='null', name='add'}(double precision,double precision):double precision deterministic nullable comparison-replacement
            FunctionName{schema='null', name='add'}(numeric,numeric):numeric deterministic nullable comparison-replacement
            FunctionName{schema='null', name='add'}(interval,timestamp without time zone):timestamp without time zone deterministic default
            FunctionName{schema='null', name='add'}(timestamp without time zone,interval):timestamp without time zone deterministic default
            FunctionName{schema='null', name='add'}(interval,timestamp with time zone):timestamp with time zone deterministic default
            FunctionName{schema='null', name='add'}(timestamp with time zone,interval):timestamp with time zone deterministic default
            FunctionName{schema='null', name='add'}(interval,interval):interval deterministic nullable
            FunctionName{schema='pg_catalog', name='age'}(timestamp without time zone):interval non-deterministic nullable
            FunctionName{schema='pg_catalog', name='age'}(timestamp without time zone,timestamp without time zone):interval non-deterministic nullable
            FunctionName{schema='null', name='and'}(bigint,bigint):bigint deterministic nullable
            FunctionName{schema='null', name='and'}(integer,integer):integer deterministic nullable
            FunctionName{schema='null', name='and'}(smallint,smallint):smallint deterministic nullable
            FunctionName{schema='null', name='and'}(byte,byte):byte deterministic nullable
            FunctionName{schema='null', name='and'}(bit(1),bit(1)):bit(1) deterministic nullable
            FunctionName{schema='null', name='any_<'}<E>(E,array(E)):boolean deterministic default
            FunctionName{schema='null', name='any_<='}<E>(E,array(E)):boolean deterministic default
            FunctionName{schema='null', name='any_<>'}<E>(E,array(E)):boolean deterministic default
            FunctionName{schema='null', name='any_='}<E>(E,array(E)):boolean deterministic default
            FunctionName{schema='null', name='any_>'}<E>(E,array(E)):boolean deterministic default
            FunctionName{schema='null', name='any_>='}<E>(E,array(E)):boolean deterministic default
            FunctionName{schema='null', name='any_ilike'}(text,array(text)):boolean deterministic nullable
            FunctionName{schema='null', name='any_like'}(text,array(text)):boolean deterministic nullable
            FunctionName{schema='null', name='any_not_ilike'}(text,array(text)):boolean deterministic nullable
            FunctionName{schema='null', name='any_not_like'}(text,array(text)):boolean deterministic nullable
            FunctionName{schema='null', name='any_value'}<T>(T):T deterministic default
            FunctionName{schema='null', name='arbitrary'}<T>(T):T deterministic default
            FunctionName{schema='null', name='area'}(geo_shape):double precision deterministic nullable
            FunctionName{schema='null', name='array_agg'}<E>(E):array(E) deterministic default
            FunctionName{schema='null', name='array_append'}<E>(array(E),E):array(E) deterministic non-nullable
            FunctionName{schema='null', name='array_avg'}(array(numeric)):numeric deterministic nullable
            FunctionName{schema='null', name='array_avg'}(array(real)):real deterministic nullable
            FunctionName{schema='null', name='array_avg'}(array(double precision)):double precision deterministic nullable
            FunctionName{schema='null', name='array_avg'}(array(byte)):numeric deterministic nullable
            FunctionName{schema='null', name='array_avg'}(array(smallint)):numeric deterministic nullable
            FunctionName{schema='null', name='array_avg'}(array(integer)):numeric deterministic nullable
            FunctionName{schema='null', name='array_avg'}(array(bigint)):numeric deterministic nullable
            FunctionName{schema='null', name='array_cat'}<E>(array(E),array(E)):array(E) deterministic non-nullable
            FunctionName{schema='null', name='array_difference'}<E>(array(E),array(E)):array(E) deterministic nullable
            FunctionName{schema='null', name='array_length'}<E>(array(E),integer):integer deterministic nullable
            FunctionName{schema='null', name='array_lower'}<E>(array(E),integer):integer deterministic nullable
            FunctionName{schema='null', name='array_max'}(array(numeric)):numeric deterministic nullable
            FunctionName{schema='null', name='array_max'}(array(byte)):byte deterministic nullable
            FunctionName{schema='null', name='array_max'}(array(boolean)):boolean deterministic nullable
            FunctionName{schema='null', name='array_max'}(array(character(1))):character(1) deterministic nullable
            FunctionName{schema='null', name='array_max'}(array(text)):text deterministic nullable
            FunctionName{schema='null', name='array_max'}(array(ip)):ip deterministic nullable
            FunctionName{schema='null', name='array_max'}(array(double precision)):double precision deterministic nullable
            FunctionName{schema='null', name='array_max'}(array(real)):real deterministic nullable
            FunctionName{schema='null', name='array_max'}(array(smallint)):smallint deterministic nullable
            FunctionName{schema='null', name='array_max'}(array(integer)):integer deterministic nullable
            FunctionName{schema='null', name='array_max'}(array(interval)):interval deterministic nullable
            FunctionName{schema='null', name='array_max'}(array(bigint)):bigint deterministic nullable
            FunctionName{schema='null', name='array_max'}(array(timestamp with time zone)):timestamp with time zone deterministic nullable
            FunctionName{schema='null', name='array_max'}(array(timestamp without time zone)):timestamp without time zone deterministic nullable
            FunctionName{schema='null', name='array_max'}(array(date)):date deterministic nullable
            FunctionName{schema='null', name='array_min'}(array(numeric)):numeric deterministic nullable
            FunctionName{schema='null', name='array_min'}(array(byte)):byte deterministic nullable
            FunctionName{schema='null', name='array_min'}(array(boolean)):boolean deterministic nullable
            FunctionName{schema='null', name='array_min'}(array(character(1))):character(1) deterministic nullable
            FunctionName{schema='null', name='array_min'}(array(text)):text deterministic nullable
            FunctionName{schema='null', name='array_min'}(array(ip)):ip deterministic nullable
            FunctionName{schema='null', name='array_min'}(array(double precision)):double precision deterministic nullable
            FunctionName{schema='null', name='array_min'}(array(real)):real deterministic nullable
            FunctionName{schema='null', name='array_min'}(array(smallint)):smallint deterministic nullable
            FunctionName{schema='null', name='array_min'}(array(integer)):integer deterministic nullable
            FunctionName{schema='null', name='array_min'}(array(interval)):interval deterministic nullable
            FunctionName{schema='null', name='array_min'}(array(bigint)):bigint deterministic nullable
            FunctionName{schema='null', name='array_min'}(array(timestamp with time zone)):timestamp with time zone deterministic nullable
            FunctionName{schema='null', name='array_min'}(array(timestamp without time zone)):timestamp without time zone deterministic nullable
            FunctionName{schema='null', name='array_min'}(array(date)):date deterministic nullable
            FunctionName{schema='null', name='array_position'}<T>(array(T),T):integer deterministic nullable
            FunctionName{schema='null', name='array_position'}<T>(array(T),T,integer):integer deterministic nullable
            FunctionName{schema='null', name='array_set'}<E>(array(E),array(integer),array(E)):array(E) deterministic nullable
            FunctionName{schema='null', name='array_set'}<E>(array(E),integer,E):array(E) deterministic nullable
            FunctionName{schema='null', name='array_slice'}<E>(array(E),integer,integer):array(E) deterministic non-nullable
            FunctionName{schema='null', name='array_sum'}(array(numeric)):numeric deterministic nullable
            FunctionName{schema='null', name='array_sum'}(array(double precision)):double precision deterministic nullable
            FunctionName{schema='null', name='array_sum'}(array(real)):real deterministic nullable
            FunctionName{schema='null', name='array_sum'}(array(byte)):bigint deterministic nullable
            FunctionName{schema='null', name='array_sum'}(array(smallint)):bigint deterministic nullable
            FunctionName{schema='null', name='array_sum'}(array(integer)):bigint deterministic nullable
            FunctionName{schema='null', name='array_sum'}(array(bigint)):bigint deterministic nullable
            FunctionName{schema='pg_catalog', name='array_to_string'}<E>(array(E),text):text deterministic nullable
            FunctionName{schema='pg_catalog', name='array_to_string'}<E>(array(E),text,text):text deterministic nullable
            FunctionName{schema='null', name='array_unique'}<E>(array(E)):array(E) deterministic non-nullable
            FunctionName{schema='null', name='array_unique'}<E>(array(E),array(E)):array(E) deterministic non-nullable
            FunctionName{schema='null', name='array_unnest'}<E>(array(array(E))):array(E) deterministic nullable
            FunctionName{schema='null', name='array_upper'}<E>(array(E),integer):integer deterministic nullable
            FunctionName{schema='null', name='ascii'}(text):integer deterministic nullable
            FunctionName{schema='null', name='asin'}(double precision):double precision deterministic nullable
            FunctionName{schema='null', name='atan'}(double precision):double precision deterministic nullable
            FunctionName{schema='null', name='atan2'}(double precision,double precision):double precision deterministic nullable
            FunctionName{schema='null', name='avg'}(double precision):double precision deterministic default
            FunctionName{schema='null', name='avg'}(real):double precision deterministic default
            FunctionName{schema='null', name='avg'}(byte):double precision deterministic default
            FunctionName{schema='null', name='avg'}(smallint):double precision deterministic default
            FunctionName{schema='null', name='avg'}(integer):double precision deterministic default
            FunctionName{schema='null', name='avg'}(bigint):double precision deterministic default
            FunctionName{schema='null', name='avg'}(timestamp with time zone):double precision deterministic default
            FunctionName{schema='null', name='avg'}(numeric):numeric deterministic default
            FunctionName{schema='null', name='avg'}(interval):interval deterministic default
            FunctionName{schema='null', name='bit_length'}(text):integer deterministic nullable
            FunctionName{schema='null', name='btrim'}(text):text deterministic nullable
            FunctionName{schema='null', name='btrim'}(text,text):text deterministic nullable
            FunctionName{schema='null', name='case'}<T>(boolean,T):T deterministic default
            FunctionName{schema='null', name='cast'}<E,V>(E,V):V deterministic default
            FunctionName{schema='null', name='ceil'}(double precision):bigint deterministic nullable
            FunctionName{schema='null', name='ceil'}(real):integer deterministic nullable
            FunctionName{schema='null', name='ceil'}(byte):integer deterministic nullable
            FunctionName{schema='null', name='ceil'}(smallint):integer deterministic nullable
            FunctionName{schema='null', name='ceil'}(integer):integer deterministic nullable
            FunctionName{schema='null', name='ceil'}(bigint):bigint deterministic nullable
            FunctionName{schema='null', name='ceiling'}(double precision):bigint deterministic nullable
            FunctionName{schema='null', name='ceiling'}(real):integer deterministic nullable
            FunctionName{schema='null', name='ceiling'}(byte):integer deterministic nullable
            FunctionName{schema='null', name='ceiling'}(smallint):integer deterministic nullable
            FunctionName{schema='null', name='ceiling'}(integer):integer deterministic nullable
            FunctionName{schema='null', name='ceiling'}(bigint):bigint deterministic nullable
            FunctionName{schema='null', name='char_length'}(text):integer deterministic nullable
            FunctionName{schema='null', name='chr'}(integer):text deterministic nullable
            FunctionName{schema='null', name='coalesce'}<E>(E):E deterministic default
            FunctionName{schema='pg_catalog', name='col_description'}(integer,integer):text deterministic default
            FunctionName{schema='null', name='collect_set'}(byte):array(byte) deterministic default
            FunctionName{schema='null', name='collect_set'}(boolean):array(boolean) deterministic default
            FunctionName{schema='null', name='collect_set'}(character(1)):array(character(1)) deterministic default
            FunctionName{schema='null', name='collect_set'}(text):array(text) deterministic default
            FunctionName{schema='null', name='collect_set'}(ip):array(ip) deterministic default
            FunctionName{schema='null', name='collect_set'}(double precision):array(double precision) deterministic default
            FunctionName{schema='null', name='collect_set'}(real):array(real) deterministic default
            FunctionName{schema='null', name='collect_set'}(smallint):array(smallint) deterministic default
            FunctionName{schema='null', name='collect_set'}(integer):array(integer) deterministic default
            FunctionName{schema='null', name='collect_set'}(interval):array(interval) deterministic default
            FunctionName{schema='null', name='collect_set'}(bigint):array(bigint) deterministic default
            FunctionName{schema='null', name='collect_set'}(timestamp with time zone):array(timestamp with time zone) deterministic default
            FunctionName{schema='null', name='collect_set'}(timestamp without time zone):array(timestamp without time zone) deterministic default
            FunctionName{schema='null', name='collect_set'}(date):array(date) deterministic default
            FunctionName{schema='null', name='collection_avg'}<E>(array(E)):double precision deterministic nullable
            FunctionName{schema='null', name='collection_count'}<E>(array(E)):bigint deterministic nullable
            FunctionName{schema='null', name='concat'}(text,text):text deterministic non-nullable
            FunctionName{schema='null', name='concat'}(text):text deterministic non-nullable
            FunctionName{schema='null', name='concat'}<E>(array(E),array(E)):array(E) deterministic non-nullable
            FunctionName{schema='null', name='concat'}(object,object):object deterministic default
            FunctionName{schema='null', name='concat_ws'}(text):text deterministic default
            FunctionName{schema='null', name='cos'}(double precision):double precision deterministic nullable
            FunctionName{schema='null', name='cot'}(double precision):double precision deterministic nullable
            FunctionName{schema='null', name='count'}<V>(V):bigint deterministic default
            FunctionName{schema='null', name='count'}():bigint deterministic default
            FunctionName{schema='null', name='curdate'}():date non-deterministic non-nullable
            FunctionName{schema='pg_catalog', name='current_database'}():text deterministic non-nullable
            FunctionName{schema='pg_catalog', name='current_schema'}():text non-deterministic non-nullable
            FunctionName{schema='pg_catalog', name='current_schemas'}(boolean):array(text) non-deterministic non-nullable
            FunctionName{schema='pg_catalog', name='current_setting'}(text):text deterministic default
            FunctionName{schema='pg_catalog', name='current_setting'}(text,boolean):text deterministic default
            FunctionName{schema='null', name='current_time'}(integer):time with time zone deterministic non-nullable
            FunctionName{schema='null', name='current_time'}():time with time zone deterministic non-nullable
            FunctionName{schema='null', name='current_timestamp'}():timestamp with time zone non-deterministic non-nullable
            FunctionName{schema='null', name='current_timestamp'}(integer):timestamp with time zone non-deterministic non-nullable
            FunctionName{schema='null', name='current_user'}():text non-deterministic default
            FunctionName{schema='null', name='date_bin'}(interval,timestamp with time zone,timestamp with time zone):timestamp with time zone deterministic nullable comparison-replacement
            FunctionName{schema='null', name='date_bin'}(interval,timestamp without time zone,timestamp without time zone):timestamp without time zone deterministic nullable comparison-replacement
            FunctionName{schema='null', name='date_format'}(timestamp with time zone):text deterministic default
            FunctionName{schema='null', name='date_format'}(text,timestamp with time zone):text deterministic default
            FunctionName{schema='null', name='date_format'}(text,text,timestamp with time zone):text deterministic default
            FunctionName{schema='null', name='date_format'}(timestamp without time zone):text deterministic default
            FunctionName{schema='null', name='date_format'}(text,timestamp without time zone):text deterministic default
            FunctionName{schema='null', name='date_format'}(text,text,timestamp without time zone):text deterministic default
            FunctionName{schema='null', name='date_format'}(bigint):text deterministic default
            FunctionName{schema='null', name='date_format'}(text,bigint):text deterministic default
            FunctionName{schema='null', name='date_format'}(text,text,bigint):text deterministic default
            FunctionName{schema='null', name='date_trunc'}(text,timestamp without time zone):timestamp with time zone deterministic default comparison-replacement
            FunctionName{schema='null', name='date_trunc'}(text,text,timestamp without time zone):timestamp with time zone deterministic default comparison-replacement
            FunctionName{schema='null', name='date_trunc'}(text,timestamp with time zone):timestamp with time zone deterministic default comparison-replacement
            FunctionName{schema='null', name='date_trunc'}(text,text,timestamp with time zone):timestamp with time zone deterministic default comparison-replacement
            FunctionName{schema='null', name='date_trunc'}(text,bigint):timestamp with time zone deterministic default comparison-replacement
            FunctionName{schema='null', name='date_trunc'}(text,text,bigint):timestamp with time zone deterministic default comparison-replacement
            FunctionName{schema='null', name='decode'}(text,text):text deterministic nullable
            FunctionName{schema='null', name='degrees'}(double precision):double precision deterministic nullable
            FunctionName{schema='null', name='distance'}(geo_point,geo_point):double precision deterministic nullable
            FunctionName{schema='null', name='divide'}(integer,integer):integer deterministic nullable
            FunctionName{schema='null', name='divide'}(bigint,bigint):bigint deterministic nullable
            FunctionName{schema='null', name='divide'}(timestamp without time zone,timestamp without time zone):timestamp without time zone deterministic nullable
            FunctionName{schema='null', name='divide'}(timestamp with time zone,timestamp with time zone):timestamp with time zone deterministic nullable
            FunctionName{schema='null', name='divide'}(real,real):real deterministic nullable
            FunctionName{schema='null', name='divide'}(double precision,double precision):double precision deterministic nullable
            FunctionName{schema='null', name='divide'}(numeric,numeric):numeric deterministic nullable
            FunctionName{schema='null', name='empty_row'}():record deterministic non-nullable
            FunctionName{schema='null', name='encode'}(text,text):text deterministic nullable
            FunctionName{schema='null', name='exp'}(double precision):double precision deterministic nullable
            FunctionName{schema='null', name='extract_CENTURY'}(timestamp with time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_CENTURY'}(timestamp without time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_DAY'}(timestamp with time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_DAY'}(timestamp without time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_DAY'}(interval):integer deterministic nullable
            FunctionName{schema='null', name='extract_DAY_OF_MONTH'}(timestamp with time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_DAY_OF_MONTH'}(timestamp without time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_DAY_OF_WEEK'}(timestamp with time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_DAY_OF_WEEK'}(timestamp without time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_DAY_OF_YEAR'}(timestamp with time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_DAY_OF_YEAR'}(timestamp without time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_EPOCH'}(timestamp with time zone):double precision deterministic nullable
            FunctionName{schema='null', name='extract_EPOCH'}(timestamp without time zone):double precision deterministic nullable
            FunctionName{schema='null', name='extract_EPOCH'}(interval):double precision deterministic nullable
            FunctionName{schema='null', name='extract_HOUR'}(timestamp with time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_HOUR'}(timestamp without time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_HOUR'}(interval):integer deterministic nullable
            FunctionName{schema='null', name='extract_MINUTE'}(timestamp with time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_MINUTE'}(timestamp without time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_MINUTE'}(interval):integer deterministic nullable
            FunctionName{schema='null', name='extract_MONTH'}(timestamp with time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_MONTH'}(timestamp without time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_MONTH'}(interval):integer deterministic nullable
            FunctionName{schema='null', name='extract_QUARTER'}(timestamp with time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_QUARTER'}(timestamp without time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_QUARTER'}(interval):integer deterministic nullable
            FunctionName{schema='null', name='extract_SECOND'}(timestamp with time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_SECOND'}(timestamp without time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_SECOND'}(interval):integer deterministic nullable
            FunctionName{schema='null', name='extract_WEEK'}(timestamp with time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_WEEK'}(timestamp without time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_YEAR'}(timestamp with time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_YEAR'}(timestamp without time zone):integer deterministic nullable
            FunctionName{schema='null', name='extract_YEAR'}(interval):integer deterministic nullable
            FunctionName{schema='null', name='floor'}(double precision):bigint deterministic nullable
            FunctionName{schema='null', name='floor'}(real):integer deterministic nullable
            FunctionName{schema='null', name='floor'}(byte):integer deterministic nullable
            FunctionName{schema='null', name='floor'}(smallint):integer deterministic nullable
            FunctionName{schema='null', name='floor'}(integer):integer deterministic nullable
            FunctionName{schema='null', name='floor'}(bigint):bigint deterministic nullable
            FunctionName{schema='null', name='format'}<E>(text,E):text deterministic non-nullable
            FunctionName{schema='pg_catalog', name='format_type'}(integer,integer):text deterministic nullable
            FunctionName{schema='null', name='gen_random_text_uuid'}():text non-deterministic non-nullable
            FunctionName{schema='pg_catalog', name='generate_series'}(bigint,bigint):bigint deterministic non-nullable
            FunctionName{schema='pg_catalog', name='generate_series'}(integer,integer):integer deterministic non-nullable
            FunctionName{schema='pg_catalog', name='generate_series'}(bigint,bigint,bigint):bigint deterministic non-nullable
            FunctionName{schema='pg_catalog', name='generate_series'}(integer,integer,integer):integer deterministic non-nullable
            FunctionName{schema='pg_catalog', name='generate_series'}(timestamp without time zone,timestamp without time zone,interval):timestamp without time zone deterministic non-nullable
            FunctionName{schema='pg_catalog', name='generate_series'}(timestamp without time zone,timestamp without time zone):timestamp without time zone deterministic default
            FunctionName{schema='pg_catalog', name='generate_series'}(timestamp with time zone,timestamp with time zone,interval):timestamp with time zone deterministic non-nullable
            FunctionName{schema='pg_catalog', name='generate_series'}(timestamp with time zone,timestamp with time zone):timestamp with time zone deterministic default
            FunctionName{schema='pg_catalog', name='generate_subscripts'}<E>(array(E),integer):integer deterministic non-nullable
            FunctionName{schema='pg_catalog', name='generate_subscripts'}<E>(array(E),integer,boolean):integer deterministic non-nullable
            FunctionName{schema='null', name='geohash'}(geo_point):text deterministic nullable
            FunctionName{schema='null', name='geometric_mean'}(double precision):double precision deterministic default
            FunctionName{schema='null', name='geometric_mean'}(real):double precision deterministic default
            FunctionName{schema='null', name='geometric_mean'}(byte):double precision deterministic default
            FunctionName{schema='null', name='geometric_mean'}(smallint):double precision deterministic default
            FunctionName{schema='null', name='geometric_mean'}(integer):double precision deterministic default
            FunctionName{schema='null', name='geometric_mean'}(bigint):double precision deterministic default
            FunctionName{schema='null', name='geometric_mean'}(timestamp with time zone):double precision deterministic default
            FunctionName{schema='null', name='greatest'}<E>(E):E deterministic default
            FunctionName{schema='pg_catalog', name='has_database_privilege'}(text,text):boolean deterministic nullable
            FunctionName{schema='pg_catalog', name='has_database_privilege'}(integer,text):boolean deterministic nullable
            FunctionName{schema='pg_catalog', name='has_database_privilege'}(text,text,text):boolean deterministic nullable
            FunctionName{schema='pg_catalog', name='has_database_privilege'}(text,integer,text):boolean deterministic nullable
            FunctionName{schema='pg_catalog', name='has_database_privilege'}(integer,text,text):boolean deterministic nullable
            FunctionName{schema='pg_catalog', name='has_database_privilege'}(integer,integer,text):boolean deterministic nullable
            FunctionName{schema='pg_catalog', name='has_schema_privilege'}(text,text):boolean deterministic nullable
            FunctionName{schema='pg_catalog', name='has_schema_privilege'}(integer,text):boolean deterministic nullable
            FunctionName{schema='pg_catalog', name='has_schema_privilege'}(text,text,text):boolean deterministic nullable
            FunctionName{schema='pg_catalog', name='has_schema_privilege'}(text,integer,text):boolean deterministic nullable
            FunctionName{schema='pg_catalog', name='has_schema_privilege'}(integer,text,text):boolean deterministic nullable
            FunctionName{schema='pg_catalog', name='has_schema_privilege'}(integer,integer,text):boolean deterministic nullable
            FunctionName{schema='pg_catalog', name='has_table_privilege'}(text,text):boolean deterministic nullable
            FunctionName{schema='pg_catalog', name='has_table_privilege'}(integer,text):boolean deterministic nullable
            FunctionName{schema='pg_catalog', name='has_table_privilege'}(text,text,text):boolean deterministic nullable
            FunctionName{schema='pg_catalog', name='has_table_privilege'}(text,integer,text):boolean deterministic nullable
            FunctionName{schema='pg_catalog', name='has_table_privilege'}(integer,text,text):boolean deterministic nullable
            FunctionName{schema='pg_catalog', name='has_table_privilege'}(integer,integer,text):boolean deterministic nullable
            FunctionName{schema='null', name='if'}<E>(boolean,E):E deterministic default
            FunctionName{schema='null', name='if'}<E>(boolean,E,E):E deterministic default
            FunctionName{schema='null', name='ignore3vl'}(boolean):boolean deterministic non-nullable
            FunctionName{schema='null', name='initcap'}(text):text deterministic nullable
            FunctionName{schema='null', name='intersects'}(geo_shape,geo_shape):boolean deterministic nullable
            FunctionName{schema='null', name='knn_match'}(float_vector,float_vector,integer):boolean deterministic default
            FunctionName{schema='null', name='latitude'}(geo_point):double precision deterministic nullable
            FunctionName{schema='null', name='least'}<E>(E):E deterministic default
            FunctionName{schema='null', name='left'}(text,integer):text deterministic default
            FunctionName{schema='null', name='length'}(text):integer deterministic nullable
            FunctionName{schema='null', name='ln'}(double precision):double precision deterministic nullable
            FunctionName{schema='null', name='log'}(double precision,double precision):double precision deterministic nullable
            FunctionName{schema='null', name='log'}(double precision):double precision deterministic nullable
            FunctionName{schema='null', name='longitude'}(geo_point):double precision deterministic nullable
            FunctionName{schema='null', name='lower'}(text):text deterministic nullable
            FunctionName{schema='null', name='lpad'}(text,integer):text deterministic default
            FunctionName{schema='null', name='lpad'}(text,integer,text):text deterministic default
            FunctionName{schema='null', name='ltrim'}(text):text deterministic nullable
            FunctionName{schema='null', name='ltrim'}(text,text):text deterministic nullable
            FunctionName{schema='null', name='match'}(object,geo_shape,text,object):boolean deterministic non-nullable
            FunctionName{schema='null', name='match'}(object,text,text,object):boolean deterministic non-nullable
            FunctionName{schema='null', name='max'}(byte):byte deterministic default
            FunctionName{schema='null', name='max'}(boolean):boolean deterministic default
            FunctionName{schema='null', name='max'}(character(1)):character(1) deterministic default
            FunctionName{schema='null', name='max'}(text):text deterministic default
            FunctionName{schema='null', name='max'}(ip):ip deterministic default
            FunctionName{schema='null', name='max'}(double precision):double precision deterministic default
            FunctionName{schema='null', name='max'}(real):real deterministic default
            FunctionName{schema='null', name='max'}(smallint):smallint deterministic default
            FunctionName{schema='null', name='max'}(integer):integer deterministic default
            FunctionName{schema='null', name='max'}(interval):interval deterministic default
            FunctionName{schema='null', name='max'}(bigint):bigint deterministic default
            FunctionName{schema='null', name='max'}(timestamp with time zone):timestamp with time zone deterministic default
            FunctionName{schema='null', name='max'}(timestamp without time zone):timestamp without time zone deterministic default
            FunctionName{schema='null', name='max'}(date):date deterministic default
            FunctionName{schema='null', name='max_by'}<A,B>(A,B):A deterministic default
            FunctionName{schema='null', name='md5'}(text):text deterministic nullable
            FunctionName{schema='null', name='mean'}(double precision):double precision deterministic default
            FunctionName{schema='null', name='mean'}(real):double precision deterministic default
            FunctionName{schema='null', name='mean'}(byte):double precision deterministic default
            FunctionName{schema='null', name='mean'}(smallint):double precision deterministic default
            FunctionName{schema='null', name='mean'}(integer):double precision deterministic default
            FunctionName{schema='null', name='mean'}(bigint):double precision deterministic default
            FunctionName{schema='null', name='mean'}(timestamp with time zone):double precision deterministic default
            FunctionName{schema='null', name='mean'}(numeric):numeric deterministic default
            FunctionName{schema='null', name='mean'}(interval):interval deterministic default
            FunctionName{schema='null', name='min'}(byte):byte deterministic default
            FunctionName{schema='null', name='min'}(boolean):boolean deterministic default
            FunctionName{schema='null', name='min'}(character(1)):character(1) deterministic default
            FunctionName{schema='null', name='min'}(text):text deterministic default
            FunctionName{schema='null', name='min'}(ip):ip deterministic default
            FunctionName{schema='null', name='min'}(double precision):double precision deterministic default
            FunctionName{schema='null', name='min'}(real):real deterministic default
            FunctionName{schema='null', name='min'}(smallint):smallint deterministic default
            FunctionName{schema='null', name='min'}(integer):integer deterministic default
            FunctionName{schema='null', name='min'}(interval):interval deterministic default
            FunctionName{schema='null', name='min'}(bigint):bigint deterministic default
            FunctionName{schema='null', name='min'}(timestamp with time zone):timestamp with time zone deterministic default
            FunctionName{schema='null', name='min'}(timestamp without time zone):timestamp without time zone deterministic default
            FunctionName{schema='null', name='min'}(date):date deterministic default
            FunctionName{schema='null', name='min_by'}<A,B>(A,B):A deterministic default
            FunctionName{schema='null', name='mod'}(integer,integer):integer deterministic nullable
            FunctionName{schema='null', name='mod'}(bigint,bigint):bigint deterministic nullable
            FunctionName{schema='null', name='mod'}(timestamp without time zone,timestamp without time zone):timestamp without time zone deterministic nullable
            FunctionName{schema='null', name='mod'}(timestamp with time zone,timestamp with time zone):timestamp with time zone deterministic nullable
            FunctionName{schema='null', name='mod'}(real,real):real deterministic nullable
            FunctionName{schema='null', name='mod'}(double precision,double precision):double precision deterministic nullable
            FunctionName{schema='null', name='mod'}(numeric,numeric):numeric deterministic nullable
            FunctionName{schema='null', name='modulus'}(integer,integer):integer deterministic nullable
            FunctionName{schema='null', name='modulus'}(bigint,bigint):bigint deterministic nullable
            FunctionName{schema='null', name='modulus'}(timestamp without time zone,timestamp without time zone):timestamp without time zone deterministic nullable
            FunctionName{schema='null', name='modulus'}(timestamp with time zone,timestamp with time zone):timestamp with time zone deterministic nullable
            FunctionName{schema='null', name='modulus'}(real,real):real deterministic nullable
            FunctionName{schema='null', name='modulus'}(double precision,double precision):double precision deterministic nullable
            FunctionName{schema='null', name='modulus'}(numeric,numeric):numeric deterministic nullable
            FunctionName{schema='null', name='multiply'}(integer,integer):integer deterministic nullable
            FunctionName{schema='null', name='multiply'}(bigint,bigint):bigint deterministic nullable
            FunctionName{schema='null', name='multiply'}(timestamp without time zone,timestamp without time zone):timestamp without time zone deterministic nullable
            FunctionName{schema='null', name='multiply'}(timestamp with time zone,timestamp with time zone):timestamp with time zone deterministic nullable
            FunctionName{schema='null', name='multiply'}(real,real):real deterministic nullable
            FunctionName{schema='null', name='multiply'}(double precision,double precision):double precision deterministic nullable
            FunctionName{schema='null', name='multiply'}(numeric,numeric):numeric deterministic nullable
            FunctionName{schema='null', name='multiply'}(integer,interval):interval deterministic nullable
            FunctionName{schema='null', name='multiply'}(interval,integer):interval deterministic nullable
            FunctionName{schema='null', name='now'}():timestamp with time zone non-deterministic non-nullable
            FunctionName{schema='null', name='null_or_empty'}(object):boolean deterministic non-nullable
            FunctionName{schema='null', name='null_or_empty'}<E>(array(E)):boolean deterministic non-nullable
            FunctionName{schema='null', name='nullif'}<E>(E,E):E deterministic default
            FunctionName{schema='pg_catalog', name='obj_description'}(integer,text):text deterministic default
            FunctionName{schema='null', name='object_keys'}(object):array(text) deterministic nullable
            FunctionName{schema='null', name='octet_length'}(text):integer deterministic nullable
            FunctionName{schema='null', name='op_<'}(byte,byte):boolean deterministic nullable
            FunctionName{schema='null', name='op_<'}(boolean,boolean):boolean deterministic nullable
            FunctionName{schema='null', name='op_<'}(character(1),character(1)):boolean deterministic nullable
            FunctionName{schema='null', name='op_<'}(text,text):boolean deterministic nullable
            FunctionName{schema='null', name='op_<'}(ip,ip):boolean deterministic nullable
            FunctionName{schema='null', name='op_<'}(double precision,double precision):boolean deterministic nullable
            FunctionName{schema='null', name='op_<'}(real,real):boolean deterministic nullable
            FunctionName{schema='null', name='op_<'}(smallint,smallint):boolean deterministic nullable
            FunctionName{schema='null', name='op_<'}(integer,integer):boolean deterministic nullable
            FunctionName{schema='null', name='op_<'}(interval,interval):boolean deterministic nullable
            FunctionName{schema='null', name='op_<'}(bigint,bigint):boolean deterministic nullable
            FunctionName{schema='null', name='op_<'}(timestamp with time zone,timestamp with time zone):boolean deterministic nullable
            FunctionName{schema='null', name='op_<'}(timestamp without time zone,timestamp without time zone):boolean deterministic nullable
            FunctionName{schema='null', name='op_<'}(date,date):boolean deterministic nullable
            FunctionName{schema='null', name='op_<<'}(ip,text):boolean deterministic nullable
            FunctionName{schema='null', name='op_<='}(byte,byte):boolean deterministic nullable
            FunctionName{schema='null', name='op_<='}(boolean,boolean):boolean deterministic nullable
            FunctionName{schema='null', name='op_<='}(character(1),character(1)):boolean deterministic nullable
            FunctionName{schema='null', name='op_<='}(text,text):boolean deterministic nullable
            FunctionName{schema='null', name='op_<='}(ip,ip):boolean deterministic nullable
            FunctionName{schema='null', name='op_<='}(double precision,double precision):boolean deterministic nullable
            FunctionName{schema='null', name='op_<='}(real,real):boolean deterministic nullable
            FunctionName{schema='null', name='op_<='}(smallint,smallint):boolean deterministic nullable
            FunctionName{schema='null', name='op_<='}(integer,integer):boolean deterministic nullable
            FunctionName{schema='null', name='op_<='}(interval,interval):boolean deterministic nullable
            FunctionName{schema='null', name='op_<='}(bigint,bigint):boolean deterministic nullable
            FunctionName{schema='null', name='op_<='}(timestamp with time zone,timestamp with time zone):boolean deterministic nullable
            FunctionName{schema='null', name='op_<='}(timestamp without time zone,timestamp without time zone):boolean deterministic nullable
            FunctionName{schema='null', name='op_<='}(date,date):boolean deterministic nullable
            FunctionName{schema='null', name='op_='}<E>(E,E):boolean deterministic nullable
            FunctionName{schema='null', name='op_>'}(byte,byte):boolean deterministic nullable
            FunctionName{schema='null', name='op_>'}(boolean,boolean):boolean deterministic nullable
            FunctionName{schema='null', name='op_>'}(character(1),character(1)):boolean deterministic nullable
            FunctionName{schema='null', name='op_>'}(text,text):boolean deterministic nullable
            FunctionName{schema='null', name='op_>'}(ip,ip):boolean deterministic nullable
            FunctionName{schema='null', name='op_>'}(double precision,double precision):boolean deterministic nullable
            FunctionName{schema='null', name='op_>'}(real,real):boolean deterministic nullable
            FunctionName{schema='null', name='op_>'}(smallint,smallint):boolean deterministic nullable
            FunctionName{schema='null', name='op_>'}(integer,integer):boolean deterministic nullable
            FunctionName{schema='null', name='op_>'}(interval,interval):boolean deterministic nullable
            FunctionName{schema='null', name='op_>'}(bigint,bigint):boolean deterministic nullable
            FunctionName{schema='null', name='op_>'}(timestamp with time zone,timestamp with time zone):boolean deterministic nullable
            FunctionName{schema='null', name='op_>'}(timestamp without time zone,timestamp without time zone):boolean deterministic nullable
            FunctionName{schema='null', name='op_>'}(date,date):boolean deterministic nullable
            FunctionName{schema='null', name='op_>='}(byte,byte):boolean deterministic nullable
            FunctionName{schema='null', name='op_>='}(boolean,boolean):boolean deterministic nullable
            FunctionName{schema='null', name='op_>='}(character(1),character(1)):boolean deterministic nullable
            FunctionName{schema='null', name='op_>='}(text,text):boolean deterministic nullable
            FunctionName{schema='null', name='op_>='}(ip,ip):boolean deterministic nullable
            FunctionName{schema='null', name='op_>='}(double precision,double precision):boolean deterministic nullable
            FunctionName{schema='null', name='op_>='}(real,real):boolean deterministic nullable
            FunctionName{schema='null', name='op_>='}(smallint,smallint):boolean deterministic nullable
            FunctionName{schema='null', name='op_>='}(integer,integer):boolean deterministic nullable
            FunctionName{schema='null', name='op_>='}(interval,interval):boolean deterministic nullable
            FunctionName{schema='null', name='op_>='}(bigint,bigint):boolean deterministic nullable
            FunctionName{schema='null', name='op_>='}(timestamp with time zone,timestamp with time zone):boolean deterministic nullable
            FunctionName{schema='null', name='op_>='}(timestamp without time zone,timestamp without time zone):boolean deterministic nullable
            FunctionName{schema='null', name='op_>='}(date,date):boolean deterministic nullable
            FunctionName{schema='null', name='op_and'}(boolean,boolean):boolean deterministic default
            FunctionName{schema='null', name='op_ilike'}(text,text):boolean deterministic nullable
            FunctionName{schema='null', name='op_ilike'}(text,text,text):boolean deterministic default
            FunctionName{schema='null', name='op_isnull'}<E>(E):boolean deterministic non-nullable
            FunctionName{schema='null', name='op_like'}(text,text):boolean deterministic nullable
            FunctionName{schema='null', name='op_like'}(text,text,text):boolean deterministic default
            FunctionName{schema='null', name='op_not'}(boolean):boolean deterministic nullable
            FunctionName{schema='null', name='op_or'}(boolean,boolean):boolean deterministic default
            FunctionName{schema='null', name='op_~'}(text,text):boolean deterministic default
            FunctionName{schema='null', name='op_~*'}(text,text):boolean deterministic default
            FunctionName{schema='null', name='or'}(bigint,bigint):bigint deterministic nullable
            FunctionName{schema='null', name='or'}(integer,integer):integer deterministic nullable
            FunctionName{schema='null', name='or'}(smallint,smallint):smallint deterministic nullable
            FunctionName{schema='null', name='or'}(byte,byte):byte deterministic nullable
            FunctionName{schema='null', name='or'}(bit(1),bit(1)):bit(1) deterministic nullable
            FunctionName{schema='null', name='parse_uri'}(text):object deterministic nullable
            FunctionName{schema='null', name='parse_url'}(text):object deterministic nullable
            FunctionName{schema='null', name='percentile'}(double precision,double precision):double precision deterministic default
            FunctionName{schema='null', name='percentile'}(double precision,array(double precision)):array(double precision) deterministic default
            FunctionName{schema='null', name='percentile'}(real,double precision):double precision deterministic default
            FunctionName{schema='null', name='percentile'}(real,array(double precision)):array(double precision) deterministic default
            FunctionName{schema='null', name='percentile'}(byte,double precision):double precision deterministic default
            FunctionName{schema='null', name='percentile'}(byte,array(double precision)):array(double precision) deterministic default
            FunctionName{schema='null', name='percentile'}(smallint,double precision):double precision deterministic default
            FunctionName{schema='null', name='percentile'}(smallint,array(double precision)):array(double precision) deterministic default
            FunctionName{schema='null', name='percentile'}(integer,double precision):double precision deterministic default
            FunctionName{schema='null', name='percentile'}(integer,array(double precision)):array(double precision) deterministic default
            FunctionName{schema='null', name='percentile'}(bigint,double precision):double precision deterministic default
            FunctionName{schema='null', name='percentile'}(bigint,array(double precision)):array(double precision) deterministic default
            FunctionName{schema='pg_catalog', name='pg_backend_pid'}():integer non-deterministic non-nullable
            FunctionName{schema='pg_catalog', name='pg_encoding_to_char'}(integer):text deterministic default
            FunctionName{schema='null', name='pg_function_is_visible'}(integer):boolean deterministic nullable
            FunctionName{schema='pg_catalog', name='pg_get_expr'}(text,integer):text deterministic default
            FunctionName{schema='pg_catalog', name='pg_get_expr'}(text,integer,boolean):text deterministic default
            FunctionName{schema='null', name='pg_get_function_result'}(integer):text deterministic default
            FunctionName{schema='pg_catalog', name='pg_get_keywords'}():record("word" text,"catcode" text,"catdesc" text) deterministic non-nullable
            FunctionName{schema='pg_catalog', name='pg_get_partkeydef'}(integer):text deterministic nullable
            FunctionName{schema='pg_catalog', name='pg_get_serial_sequence'}(text,text):text deterministic nullable
            FunctionName{schema='pg_catalog', name='pg_get_userbyid'}(integer):text deterministic nullable
            FunctionName{schema='pg_catalog', name='pg_postmaster_start_time'}():timestamp with time zone non-deterministic non-nullable
            FunctionName{schema='pg_catalog', name='pg_table_is_visible'}(integer):boolean deterministic nullable
            FunctionName{schema='pg_catalog', name='pg_typeof'}<E>(E):text deterministic non-nullable
            FunctionName{schema='null', name='pi'}():double precision deterministic non-nullable
            FunctionName{schema='null', name='power'}(double precision,double precision):double precision deterministic nullable
            FunctionName{schema='pg_catalog', name='quote_ident'}(text):text deterministic nullable
            FunctionName{schema='null', name='radians'}(double precision):double precision deterministic nullable
            FunctionName{schema='null', name='random'}():double precision non-deterministic non-nullable
            FunctionName{schema='null', name='regexp_matches'}(text,text):array(text) deterministic non-nullable
            FunctionName{schema='null', name='regexp_matches'}(text,text,text):array(text) deterministic non-nullable
            FunctionName{schema='null', name='regexp_replace'}(text,text,text):text deterministic default
            FunctionName{schema='null', name='regexp_replace'}(text,text,text,text):text deterministic default
            FunctionName{schema='null', name='repeat'}(text,integer):text deterministic default
            FunctionName{schema='null', name='replace'}(text,text,text):text deterministic nullable
            FunctionName{schema='null', name='reverse'}(text):text deterministic nullable
            FunctionName{schema='null', name='right'}(text,integer):text deterministic default
            FunctionName{schema='null', name='round'}(double precision):bigint deterministic nullable
            FunctionName{schema='null', name='round'}(real):integer deterministic nullable
            FunctionName{schema='null', name='round'}(byte):integer deterministic nullable
            FunctionName{schema='null', name='round'}(smallint):integer deterministic nullable
            FunctionName{schema='null', name='round'}(integer):integer deterministic nullable
            FunctionName{schema='null', name='round'}(bigint):bigint deterministic nullable
            FunctionName{schema='null', name='row_number'}():integer deterministic default
            FunctionName{schema='null', name='rpad'}(text,integer):text deterministic default
            FunctionName{schema='null', name='rpad'}(text,integer,text):text deterministic default
            FunctionName{schema='null', name='rtrim'}(text):text deterministic nullable
            FunctionName{schema='null', name='rtrim'}(text,text):text deterministic nullable
            FunctionName{schema='null', name='session_user'}():text non-deterministic default
            FunctionName{schema='null', name='sha1'}(text):text deterministic nullable
            FunctionName{schema='null', name='sign'}(numeric):numeric deterministic nullable
            FunctionName{schema='null', name='sign'}(double precision):double precision deterministic nullable
            FunctionName{schema='null', name='sin'}(double precision):double precision deterministic nullable
            FunctionName{schema='null', name='sleep'}(bigint):boolean non-deterministic default
            FunctionName{schema='null', name='split_part'}(text,text,integer):text deterministic default
            FunctionName{schema='null', name='sqrt'}(double precision):double precision deterministic nullable
            FunctionName{schema='null', name='sqrt'}(real):double precision deterministic nullable
            FunctionName{schema='null', name='sqrt'}(byte):double precision deterministic nullable
            FunctionName{schema='null', name='sqrt'}(smallint):double precision deterministic nullable
            FunctionName{schema='null', name='sqrt'}(integer):double precision deterministic nullable
            FunctionName{schema='null', name='sqrt'}(bigint):double precision deterministic nullable
            FunctionName{schema='null', name='stddev'}(double precision):double precision deterministic default
            FunctionName{schema='null', name='stddev'}(real):double precision deterministic default
            FunctionName{schema='null', name='stddev'}(byte):double precision deterministic default
            FunctionName{schema='null', name='stddev'}(smallint):double precision deterministic default
            FunctionName{schema='null', name='stddev'}(integer):double precision deterministic default
            FunctionName{schema='null', name='stddev'}(bigint):double precision deterministic default
            FunctionName{schema='null', name='stddev'}(timestamp with time zone):double precision deterministic default
            FunctionName{schema='null', name='string_agg'}(text,text):text deterministic default
            FunctionName{schema='null', name='string_to_array'}(text,text):array(text) deterministic default
            FunctionName{schema='null', name='string_to_array'}(text,text,text):array(text) deterministic default
            FunctionName{schema='null', name='strpos'}(text,text):integer deterministic default
            FunctionName{schema='null', name='subscript'}(array(object),text):array(undefined) deterministic default
            FunctionName{schema='null', name='subscript'}<E>(array(E),integer):E deterministic default
            FunctionName{schema='null', name='subscript'}(object,text):undefined deterministic default
            FunctionName{schema='null', name='subscript'}(undefined,text):undefined deterministic default
            FunctionName{schema='null', name='subscript_obj'}(object,text):undefined deterministic default
            FunctionName{schema='null', name='substr'}(text,integer):text deterministic default
            FunctionName{schema='null', name='substr'}(text,integer,integer):text deterministic default
            FunctionName{schema='null', name='substr'}(text,text):text deterministic default
            FunctionName{schema='null', name='substring'}(text,integer):text deterministic default
            FunctionName{schema='null', name='substring'}(text,integer,integer):text deterministic default
            FunctionName{schema='null', name='substring'}(text,text):text deterministic default
            FunctionName{schema='null', name='subtract'}(integer,integer):integer deterministic nullable
            FunctionName{schema='null', name='subtract'}(bigint,bigint):bigint deterministic nullable
            FunctionName{schema='null', name='subtract'}(real,real):real deterministic nullable
            FunctionName{schema='null', name='subtract'}(double precision,double precision):double precision deterministic nullable
            FunctionName{schema='null', name='subtract'}(numeric,numeric):numeric deterministic nullable
            FunctionName{schema='null', name='subtract'}(timestamp without time zone,timestamp without time zone):interval deterministic default
            FunctionName{schema='null', name='subtract'}(timestamp with time zone,timestamp with time zone):interval deterministic default
            FunctionName{schema='null', name='subtract'}(timestamp without time zone,interval):timestamp without time zone deterministic default
            FunctionName{schema='null', name='subtract'}(timestamp with time zone,interval):timestamp with time zone deterministic default
            FunctionName{schema='null', name='subtract'}(interval,interval):interval deterministic nullable
            FunctionName{schema='null', name='sum'}(interval):interval deterministic default
            FunctionName{schema='null', name='sum'}(real):real deterministic default
            FunctionName{schema='null', name='sum'}(double precision):double precision deterministic default
            FunctionName{schema='null', name='sum'}(byte):bigint deterministic default
            FunctionName{schema='null', name='sum'}(smallint):bigint deterministic default
            FunctionName{schema='null', name='sum'}(integer):bigint deterministic default
            FunctionName{schema='null', name='sum'}(bigint):bigint deterministic default
            FunctionName{schema='null', name='sum'}(numeric):numeric deterministic default
            FunctionName{schema='null', name='tan'}(double precision):double precision deterministic nullable
            FunctionName{schema='null', name='timezone'}(text,timestamp with time zone):timestamp without time zone deterministic default
            FunctionName{schema='null', name='timezone'}(text,timestamp without time zone):timestamp with time zone deterministic default
            FunctionName{schema='null', name='timezone'}(text,bigint):timestamp with time zone deterministic default
            FunctionName{schema='null', name='to_char'}(timestamp without time zone,text):text deterministic default
            FunctionName{schema='null', name='to_char'}(timestamp with time zone,text):text deterministic default
            FunctionName{schema='null', name='to_char'}(interval,text):text deterministic default
            FunctionName{schema='null', name='translate'}(text,text,text):text deterministic default
            FunctionName{schema='null', name='trim'}(text):text deterministic nullable
            FunctionName{schema='null', name='trim'}(text,text,text):text deterministic nullable
            FunctionName{schema='null', name='trunc'}(double precision):bigint deterministic nullable
            FunctionName{schema='null', name='trunc'}(real):integer deterministic nullable
            FunctionName{schema='null', name='trunc'}(byte):integer deterministic nullable
            FunctionName{schema='null', name='trunc'}(smallint):integer deterministic nullable
            FunctionName{schema='null', name='trunc'}(integer):integer deterministic nullable
            FunctionName{schema='null', name='trunc'}(bigint):bigint deterministic nullable
            FunctionName{schema='null', name='trunc'}(double precision,integer):double precision deterministic nullable
            FunctionName{schema='null', name='try_cast'}<E,V>(E,V):V deterministic default
            FunctionName{schema='null', name='unnest'}<N>(array(N)):record deterministic non-nullable
            FunctionName{schema='null', name='unnest'}():object deterministic non-nullable
            FunctionName{schema='null', name='upper'}(text):text deterministic nullable
            FunctionName{schema='null', name='variance'}(double precision):double precision deterministic default
            FunctionName{schema='null', name='variance'}(real):double precision deterministic default
            FunctionName{schema='null', name='variance'}(byte):double precision deterministic default
            FunctionName{schema='null', name='variance'}(smallint):double precision deterministic default
            FunctionName{schema='null', name='variance'}(integer):double precision deterministic default
            FunctionName{schema='null', name='variance'}(bigint):double precision deterministic default
            FunctionName{schema='null', name='variance'}(timestamp with time zone):double precision deterministic default
            FunctionName{schema='null', name='vector_similarity'}(float_vector(1),float_vector(1)):real deterministic nullable
            FunctionName{schema='pg_catalog', name='version'}():text non-deterministic non-nullable
            FunctionName{schema='null', name='within'}(geo_shape,geo_shape):boolean deterministic default
            FunctionName{schema='null', name='within'}(geo_point,geo_shape):boolean deterministic default
            FunctionName{schema='null', name='within'}(geo_point,text):boolean deterministic default
            FunctionName{schema='null', name='within'}(geo_point,object):boolean deterministic default
            FunctionName{schema='null', name='within'}(geo_point,undefined):boolean deterministic default
            FunctionName{schema='null', name='xor'}(bigint,bigint):bigint deterministic nullable
            FunctionName{schema='null', name='xor'}(integer,integer):integer deterministic nullable
            FunctionName{schema='null', name='xor'}(smallint,smallint):smallint deterministic nullable
            FunctionName{schema='null', name='xor'}(byte,byte):byte deterministic nullable
            FunctionName{schema='null', name='xor'}(bit(1),bit(1)):bit(1) deterministic nullable
            """;

        assertThat(sb.toString()).isEqualTo(allFunctionSignatures);
    }
}

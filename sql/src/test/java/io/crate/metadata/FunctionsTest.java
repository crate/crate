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

import io.crate.expression.symbol.FuncArg;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.functions.Signature;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;
import static org.hamcrest.Matchers.contains;

public class FunctionsTest extends CrateUnitTest {

    private Map<FunctionName, List<FuncResolver>> implementations = new HashMap<>();

    private void register(Signature signature, Function<List<DataType>, FunctionImplementation> factory) {
        List<FuncResolver> functions = implementations.computeIfAbsent(
            signature.getName(),
            k -> new ArrayList<>());
        functions.add(new FuncResolver(signature, factory));
    }

    private Functions createFunctions() {
        return new Functions(Collections.emptyMap(), Collections.emptyMap(), implementations);
    }

    private FunctionImplementation resolve(String functionName,
                                           List<? extends FuncArg> arguments) {
        return createFunctions().get(null, functionName, arguments, SearchPath.pathWithPGCatalogAndDoc());
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
            args -> () -> new FunctionInfo(new FunctionIdent("foo", args), DataTypes.STRING)
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
            args -> () -> new FunctionInfo(new FunctionIdent("foo", args), DataTypes.INTEGER)
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
            args -> () -> new FunctionInfo(new FunctionIdent("foo", args), DataTypes.DOUBLE)
        );
        register(
            Signature.scalar(
                "foo",
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ),
            args -> () -> new FunctionInfo(new FunctionIdent("foo", args), DataTypes.INTEGER)
        );

        var impl = resolve("foo", List.of(Literal.of(1L)));

        // integer is more specific than double
        assertThat(impl.info().ident().argumentTypes(), contains(DataTypes.INTEGER));
    }

    @Test
    public void test_multiple_signature_matches_with_same_return_type_results_in_first_selected() {
        register(
            Signature.scalar(
                "foo",
                DataTypes.STRING.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ),
            args -> () -> new FunctionInfo(new FunctionIdent("foo", args), DataTypes.INTEGER)
        );
        register(
            Signature.scalar(
                "foo",
                parseTypeSignature("array(E)"),
                DataTypes.INTEGER.getTypeSignature()
            ).withTypeVariableConstraints(typeVariable("E")),
            args -> () -> new FunctionInfo(new FunctionIdent("foo", args), DataTypes.INTEGER)
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
            args -> () -> new FunctionInfo(new FunctionIdent("foo", args), DataTypes.INTEGER)
        );
        register(
            Signature.scalar(
                "foo",
                parseTypeSignature("array(E)"),
                parseTypeSignature("array(E)")
            ).withTypeVariableConstraints(typeVariable("E")),
            args -> () -> new FunctionInfo(new FunctionIdent("foo", args), DataTypes.UNDEFINED)
        );

        var impl = resolve("foo", List.of(Literal.of(DataTypes.UNDEFINED, null)));
        assertThat(impl.info().ident().argumentTypes(), contains(DataTypes.STRING));
    }
}

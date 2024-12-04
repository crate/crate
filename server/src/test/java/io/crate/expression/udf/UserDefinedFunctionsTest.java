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

package io.crate.expression.udf;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.FunctionArgumentDefinition;
import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.SearchPath;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class UserDefinedFunctionsTest extends UdfUnitTest {


    @Before
    public void prepare() throws Exception {
        udfService.registerLanguage(DUMMY_LANG);
    }

    private void registerUserDefinedFunction(
        String lang,
        String schema,
        String name,
        DataType<?> returnType,
        List<DataType<?>> types,
        String definition) {

        var udf = new UserDefinedFunctionMetadata(
            schema,
            name,
            types.stream().map(FunctionArgumentDefinition::of).toList(),
            returnType,
            lang,
            definition);

        var functionName = new FunctionName(udf.schema(), udf.name());
        Map<FunctionName, List<FunctionProvider>> functionImplementations = new HashMap<>();
        var resolvers = functionImplementations.computeIfAbsent(
            functionName, k -> new ArrayList<>());
        resolvers.add(udfService.buildFunctionResolver(udf));
        sqlExecutor.nodeCtx.functions().setUDFs(functionImplementations);
    }

    @Test
    public void testOverloadingBuiltinFunctions() {
        registerUserDefinedFunction(
            DUMMY_LANG.name(),
            "test",
            "subtract",
            DataTypes.INTEGER,
            List.of(DataTypes.INTEGER, DataTypes.INTEGER),
            "function subtract(a, b) { return a + b; }");
        assertThat(sqlExecutor.asSymbol("test.subtract(2::integer, 1::integer)"))
            .isLiteral(DummyFunction.RESULT);
    }

    @Test
    public void test_function_name_and_schema_with_camel_case_strings() throws IOException {
        // camelCase Schema and name
        registerUserDefinedFunction(
            DUMMY_LANG.name(),
            "MyScHEmA",
            "mYFunCTioN",
            DataTypes.INTEGER,
            List.of(DataTypes.INTEGER, DataTypes.INTEGER),
            "function mYFunCTioN(a, b) { return a + b; }");

        assertThatThrownBy(() -> sqlExecutor.asSymbol("MyScHEmA.mYFunCTioN(1::integer, 2::integer)"))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessage("Unknown function: myschema.myfunction(1, 2)");

        Function f = (Function) sqlExecutor.asSymbol("\"MyScHEmA\".\"mYFunCTioN\"(ints, 10)");
        assertThat(f.toString(Style.QUALIFIED)).isEqualTo("\"MyScHEmA\".\"mYFunCTioN\"(doc.users.ints, 10)");
        assertThat(f.toString(Style.UNQUALIFIED)).isEqualTo("\"MyScHEmA\".\"mYFunCTioN\"(ints, 10)");

        // null schema and camelCase name is considered as builtin -> no quoting
        // a UDF cannot be created with a null schema, if not defined, the schema from `search_path` is used
        registerUserDefinedFunction(
            DUMMY_LANG.name(),
            null,
            "myOTHerFUncTIOn",
            DataTypes.INTEGER,
            List.of(DataTypes.INTEGER, DataTypes.INTEGER),
            "function myOTHerFUncTIOn(a, b) { return a + b; }");

        assertThatThrownBy(() -> sqlExecutor.asSymbol("myOTHerFUncTIOn(1::integer, 2::integer)"))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessage("Unknown function: myotherfunction(1, 2)");

        f = (Function) sqlExecutor.asSymbol("\"myOTHerFUncTIOn\"(ints, 10)");
        assertThat(f.toString(Style.QUALIFIED)).isEqualTo("myOTHerFUncTIOn(doc.users.ints, 10)");
        assertThat(f.toString(Style.UNQUALIFIED)).isEqualTo("myOTHerFUncTIOn(ints, 10)");
    }

    @Test
    public void test_cannot_create_invalid_function() throws Exception {
        UserDefinedFunctionMetadata invalid = new UserDefinedFunctionMetadata(
            "my_schema",
            "invalid",
            List.of(),
            DataTypes.INTEGER,
            "burlesque",
            "this is not valid burlesque code"
        );
        UserDefinedFunctionMetadata valid = new UserDefinedFunctionMetadata(
            "my_schema",
            "valid",
            List.of(),
            DataTypes.INTEGER,
            DUMMY_LANG.name(),
            "function valid() { return 42; }"
        );
        // if a functionImpl can't be created, it won't be registered
        udfService.updateImplementations(List.of(invalid, valid));

        Functions functions = sqlExecutor.nodeCtx.functions();
        SearchPath searchPath = SearchPath.pathWithPGCatalogAndDoc();
        FunctionImplementation function = functions.get("my_schema", "valid", List.of(), searchPath);
        assertThat(function).isNotNull();

        assertThatThrownBy(() -> functions.get("my_schema", "invalid", List.of(), searchPath))
            .hasMessage("Unknown function: my_schema.invalid()");
    }
}

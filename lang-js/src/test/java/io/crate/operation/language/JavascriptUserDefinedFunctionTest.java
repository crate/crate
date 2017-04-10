/*
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate.io licenses
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
 * To enable or use any of the enterprise features, Crate.io must have given
 * you permission to enable and use the Enterprise Edition of CrateDB and you
 * must have a valid Enterprise or Subscription Agreement with Crate.io.  If
 * you enable or use features that are part of the Enterprise Edition, you
 * represent and warrant that you have a valid Enterprise or Subscription
 * Agreement with Crate.io.  Your use of features of the Enterprise Edition
 * is governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.operation.language;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.FunctionArgumentDefinition;
import io.crate.analyze.symbol.Literal;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Schemas;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.operation.udf.UserDefinedFunctionMetaData;
import io.crate.operation.udf.UserDefinedFunctionService;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.script.ScriptException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;

public class JavascriptUserDefinedFunctionTest extends AbstractScalarFunctionsTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private static final String JS = "javascript";
    private UserDefinedFunctionService udfService;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder()
            .put("udf.enabled", true)
            .build();
        udfService = new UserDefinedFunctionService(mock(ClusterService.class));
        udfService.registerLanguage(new JavaScriptLanguage(udfService, settings));
    }

    private Map<FunctionIdent, FunctionImplementation> functionImplementations = new HashMap<>();

    private void registerUserDefinedFunction(String name, DataType returnType, List<DataType> types, String definition) throws ScriptException {
        UserDefinedFunctionMetaData udfMeta = new UserDefinedFunctionMetaData(
            Schemas.DEFAULT_SCHEMA_NAME,
            name,
            types.stream().map(FunctionArgumentDefinition::of).collect(Collectors.toList()),
            returnType,
            JS,
            definition
        );

        functionImplementations.put(new FunctionIdent(Schemas.DEFAULT_SCHEMA_NAME, name, types), udfService.getLanguage(JS).createFunctionImplementation(udfMeta));
        functions.registerUdfResolversForSchema(Schemas.DEFAULT_SCHEMA_NAME, functionImplementations);
    }

    @After
    public void after() {
        functionImplementations.clear();
    }

    @Test
    public void testObjectReturnType() throws Exception {
        registerUserDefinedFunction("f", DataTypes.OBJECT, ImmutableList.of(),
            "function f() { return JSON.parse('{\"foo\": \"bar\"}'); }");
        assertEvaluate("f()", ImmutableMap.of("foo", "bar"));
    }

    @Test
    public void testInvalidJavascript() throws ScriptException{
        List<DataType> types = ImmutableList.of(DataTypes.DOUBLE);
        UserDefinedFunctionMetaData udfMeta = new UserDefinedFunctionMetaData(
            Schemas.DEFAULT_SCHEMA_NAME,
            "f",
            types.stream().map(FunctionArgumentDefinition::of).collect(Collectors.toList()),
            DataTypes.DOUBLE_ARRAY,
            JS,
            "function f(a) { return a[0]1*#?; }"
        );

        String validation = udfService.getLanguage(JS).validate(udfMeta);
        assertEquals(validation, "Invalid JavaScript in function 'f(double)'");
    }

    @Test
    public void testValidJavascript() throws Exception {
        List<DataType> types = ImmutableList.of(DataTypes.DOUBLE_ARRAY);
        UserDefinedFunctionMetaData udfMeta = new UserDefinedFunctionMetaData(
            Schemas.DEFAULT_SCHEMA_NAME,
            "f",
            types.stream().map(FunctionArgumentDefinition::of).collect(Collectors.toList()),
            DataTypes.DOUBLE,
            JS,
            "function f(a) { return a[0]; }"
        );

        String validation = udfService.getLanguage(JS).validate(udfMeta);
        assertEquals(validation, null);
    }

    @Test
    public void testArrayReturnType() throws Exception {
        registerUserDefinedFunction("f", DataTypes.DOUBLE_ARRAY, ImmutableList.of(), "function f() { return [1, 2]; }");
        assertEvaluate("f()", new double[]{1.0, 2.0});
    }

    @Test
    public void testTimestampReturnType() throws Exception {
        registerUserDefinedFunction("f", DataTypes.TIMESTAMP, ImmutableList.of(),
            "function f() { return \"1990-01-01T00:00:00\"; }");
        assertEvaluate("f()", 631152000000L);
    }

    @Test
    public void testIpReturnType() throws Exception {
        registerUserDefinedFunction("f", DataTypes.IP, ImmutableList.of(), "function f() { return \"127.0.0.1\"; }");
        assertEvaluate("f()", DataTypes.IP.value("127.0.0.1"));
    }

    @Test
    public void testPrimitiveReturnType() throws Exception {
        registerUserDefinedFunction("f", DataTypes.INTEGER, ImmutableList.of(), "function f() { return 10; }");
        assertEvaluate("f()", 10);
    }

    @Test
    public void testObjectReturnTypeAndInputArguments() throws Exception {
        registerUserDefinedFunction("f", DataTypes.FLOAT, ImmutableList.of(DataTypes.DOUBLE, DataTypes.SHORT),
            "function f(x, y) { return x + y; }");
        assertEvaluate("f(double_val, short_val)", 3.0f, Literal.of(1), Literal.of(2));
    }

    @Test
    public void testPrimitiveReturnTypeAndInputArguments() throws Exception {
        registerUserDefinedFunction("f", DataTypes.FLOAT, ImmutableList.of(DataTypes.DOUBLE, DataTypes.SHORT),
            "function f(x, y) { return x + y; }");
        assertEvaluate("f(double_val, short_val)", 3.0f, Literal.of(1), Literal.of(2));
    }

    @Test
    public void testGeoTypeReturnTypeWithDoubleArray() throws Exception {
        registerUserDefinedFunction("f", DataTypes.GEO_POINT, ImmutableList.of(), "function f() { return [1, 1]; }");
        assertEvaluate("f()", new double[]{1.0, 1.0});
    }

    @Test
    public void testGeoTypeReturnTypeWithWKT() throws Exception {
        registerUserDefinedFunction("f", DataTypes.GEO_POINT, ImmutableList.of(),
            "function f() { return \"POINT (1.0 2.0)\"; }");
        assertEvaluate("f()", new double[]{1.0, 2.0});
    }

    @Test
    public void testOverloadingUserDefinedFunctions() throws Exception {
        registerUserDefinedFunction("f", DataTypes.LONG, ImmutableList.of(), "function f() { return 1; }");
        registerUserDefinedFunction("f", DataTypes.LONG, ImmutableList.of(DataTypes.LONG), "function f(x) { return x; }");
        registerUserDefinedFunction("f", DataTypes.LONG, ImmutableList.of(DataTypes.LONG, DataTypes.INTEGER),
            "function f(x, y) { return x + y; }");
        assertEvaluate("f()", 1L);
        assertEvaluate("f(x)", 2L, Literal.of(2));
        assertEvaluate("f(x, a)", 3L, Literal.of(2), Literal.of(1));
    }

    @Test
    public void testFunctionWrongNameInFunctionBody() throws Exception {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage(containsString("The name [test] of the function signature doesn't"));
        registerUserDefinedFunction("test", DataTypes.LONG, ImmutableList.of(), "function f() { return 1; }");
        assertEvaluate("test()", 1L);
    }

    @Test
    public void testCompilableButIncorrectFunctionBody() throws Exception {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(containsString("The function definition cannot be evaluated"));
        registerUserDefinedFunction("f", DataTypes.OBJECT, ImmutableList.of(),
            "function f() { return JSON.parse('{\"foo\": a}'); }");
        assertEvaluate("f()", 1L);
    }

    @Test
    public void testNormalizeOnObjectInput() throws Exception {
        registerUserDefinedFunction("f", DataTypes.OBJECT, ImmutableList.of(DataTypes.OBJECT),
            "function f(x) { return x; }");
        assertNormalize("f({})", isLiteral(new HashMap<>()));
    }

    @Test
    public void testNormalizeOnArrayInput() throws Exception {
        registerUserDefinedFunction("f", DataTypes.LONG, ImmutableList.of(DataTypes.DOUBLE_ARRAY),
            "function f(x) { return x[1]; }");
        assertNormalize("f([1.0, 2.0])", isLiteral(2L));
    }

    @Test
    public void testNormalizeOnStringInputs() throws Exception {
        registerUserDefinedFunction("f", DataTypes.STRING, ImmutableList.of(DataTypes.STRING),
            "function f(x) { return x; }");
        assertNormalize("f('bar')", isLiteral("bar"));
    }

    @Test
    public void testAccessJavaClasses() throws Exception {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(containsString("has no such function \"type\""));
        registerUserDefinedFunction("f", DataTypes.LONG, ImmutableList.of(DataTypes.LONG),
            "function f(x) { var File = Java.type(\"java.io.File\"); return x; }");
        assertEvaluate("f(x)", 1L, Literal.of(1L));
    }

    @Test
    public void testEvaluateBytesRefConvertedToString() throws Exception {
        registerUserDefinedFunction("f", DataTypes.STRING, ImmutableList.of(DataTypes.STRING),
            "function f(name) { return 'foo' + name; }");
        assertEvaluate("f(name)", "foobar", Literal.of("bar"));
    }

    @Test
    public void testJavaScriptFunctionReturnsUndefined() throws Exception {
        registerUserDefinedFunction("f", DataTypes.STRING, ImmutableList.of(DataTypes.STRING),
            "function f(name) { }");
        assertEvaluate("f(name)", null, Literal.of("bar"));
    }

    @Test
    public void testJavaScriptFunctionReturnsNull() throws Exception {
        registerUserDefinedFunction("f", DataTypes.STRING, ImmutableList.of(), "function f() { return null; }");
        assertEvaluate("f()", null);
    }

    @Test
    public void testEvaluateBytesRefInObjectIsConvertedToString() throws Exception {
        registerUserDefinedFunction("f", DataTypes.OBJECT, ImmutableList.of(DataTypes.OBJECT),
            "function f(o) { return {'key1' : o['inner']['key1'][0], 'key2': o['inner']['key2']}; }");
        // the map will be modified
        Map<String, Object> inner = new HashMap<>();
        inner.put("key1", new Object[]{new BytesRef("bar")});
        inner.put("key2", new BytesRef("foo"));
        assertEvaluate("f(obj)", ImmutableMap.of("key1", "bar", "key2", "foo"), Literal.of(ImmutableMap.of("inner", inner)));
    }

    @Test
    public void testEvaluateBytesRefInArrayIsConvertedToString() throws Exception {
        registerUserDefinedFunction("f", DataTypes.STRING, ImmutableList.of(new ArrayType(new ArrayType(DataTypes.STRING))),
            "function f(arr) { return arr[0][0]; }");
        assertEvaluate("f(array_string_array)", "foo",
            Literal.of(new Object[][]{new Object[]{new BytesRef("foo")}}, new ArrayType(new ArrayType(DataTypes.STRING))));
    }
}

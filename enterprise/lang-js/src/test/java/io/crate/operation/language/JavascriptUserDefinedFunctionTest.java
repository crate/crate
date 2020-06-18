/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.operation.language;

import io.crate.analyze.FunctionArgumentDefinition;
import io.crate.expression.scalar.AbstractScalarFunctionsTest;
import io.crate.expression.symbol.Literal;
import io.crate.expression.udf.UserDefinedFunctionMetaData;
import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.metadata.FunctionProvider;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Schemas;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.impl.PointImpl;

import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;

public class JavascriptUserDefinedFunctionTest extends AbstractScalarFunctionsTest {

    private static final String JS = JavaScriptLanguage.NAME;

    private Map<FunctionName, List<FunctionProvider>> functionImplementations = new HashMap<>();
    private UserDefinedFunctionService udfService;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        udfService = new UserDefinedFunctionService(
            mock(ClusterService.class),
            functions);
        udfService.registerLanguage(new JavaScriptLanguage(udfService));
    }

    private void registerUserDefinedFunction(String name,
                                             DataType returnType,
                                             List<DataType> types,
                                             String definition) throws ScriptException {
        UserDefinedFunctionMetaData udf = new UserDefinedFunctionMetaData(
            Schemas.DOC_SCHEMA_NAME,
            name,
            types.stream().map(FunctionArgumentDefinition::of).collect(Collectors.toList()),
            returnType,
            JS,
            definition);

        String validation = udfService.getLanguage(JS).validate(udf);
        if (validation == null) {
            var functionName = new FunctionName(Schemas.DOC_SCHEMA_NAME, udf.name());
            var resolvers = functionImplementations.computeIfAbsent(
                functionName, k -> new ArrayList<>());
            resolvers.add(udfService.buildFunctionResolver(udf));
            functions.registerUdfFunctionImplementationsForSchema(
                Schemas.DOC_SCHEMA_NAME,
                functionImplementations);
        } else {
            throw new ScriptException(validation);
        }
    }

    @After
    public void afterTest() {
        functionImplementations.clear();
    }

    @Test
    public void testObjectReturnType() throws Exception {
        registerUserDefinedFunction(
            "f",
            DataTypes.UNTYPED_OBJECT,
            List.of(),
            "function f() { return JSON.parse('{\"foo\": \"bar\"}'); }");
        assertEvaluate("f()", Map.of("foo", "bar"));
    }

    @Test
    public void testValidateCatchesScriptException() {
        var udfMeta = new UserDefinedFunctionMetaData(
            Schemas.DOC_SCHEMA_NAME,
            "f",
            Collections.singletonList(FunctionArgumentDefinition.of(DataTypes.DOUBLE)),
            DataTypes.DOUBLE_ARRAY,
            JS,
            "function f(a) { return a[0]1*#?; }");

        assertThat(
            udfService.getLanguage(JS).validate(udfMeta),
            containsString(
                "Invalid JavaScript in function 'doc.f(double precision)' AS 'function f(a) " +
                "{ return a[0]1*#?; }': SyntaxError: f:1:27 Expected ; but found 1"));
    }

    @Test
    public void testValidateCatchesAssertionError() {
        var udfMeta = new UserDefinedFunctionMetaData(
            Schemas.DOC_SCHEMA_NAME,
            "f",
            Collections.singletonList(FunctionArgumentDefinition.of(DataTypes.DOUBLE)),
            DataTypes.DOUBLE_ARRAY,
            JS,
            "var f = (a) => a * a;");

        String validation = udfService.getLanguage(JS).validate(udfMeta);
        String javaVersion = System.getProperty("java.specification.version");
        try {
            if (Integer.parseInt(javaVersion) >= 9) {
                assertThat(validation, is(nullValue()));
            }
        } catch (NumberFormatException e) {
            assertThat(validation, startsWith("Invalid JavaScript in function 'doc.f(double)'"));
            assertThat(validation, endsWith("Failed generating bytecode for <eval>:1"));
        }
    }

    @Test
    public void testValidJavascript() {
        var udfMeta = new UserDefinedFunctionMetaData(
            Schemas.DOC_SCHEMA_NAME,
            "f",
            Collections.singletonList(FunctionArgumentDefinition.of(DataTypes.DOUBLE_ARRAY)),
            DataTypes.DOUBLE,
            JS,
            "function f(a) { return a[0]; }");
        assertThat(udfService.getLanguage(JS).validate(udfMeta), is(nullValue()));
    }

    @Test
    public void testArrayReturnType() throws Exception {
        registerUserDefinedFunction(
            "f",
            DataTypes.DOUBLE_ARRAY,
            List.of(),
            "function f() { return [1, 2]; }");
        assertEvaluate("f()", List.of(1.0, 2.0));
    }

    @Test
    public void testTimestampReturnType() throws Exception {
        registerUserDefinedFunction(
            "f",
            DataTypes.TIMESTAMPZ,
            List.of(),
            "function f() { return \"1990-01-01T00:00:00\"; }");
        assertEvaluate("f()", 631152000000L);
    }

    @Test
    public void testIpReturnType() throws Exception {
        registerUserDefinedFunction(
            "f",
            DataTypes.IP,
            List.of(),
            "function f() { return \"127.0.0.1\"; }");
        assertEvaluate("f()", DataTypes.IP.value("127.0.0.1"));
    }

    @Test
    public void testPrimitiveReturnType() throws Exception {
        registerUserDefinedFunction(
            "f",
            DataTypes.INTEGER,
            List.of(),
            "function f() { return 10; }");
        assertEvaluate("f()", 10);
    }

    @Test
    public void testObjectReturnTypeAndInputArguments() throws Exception {
        registerUserDefinedFunction(
            "f",
            DataTypes.FLOAT,
            List.of(DataTypes.DOUBLE, DataTypes.SHORT),
            "function f(x, y) { return x + y; }");
        assertEvaluate("f(double_val, short_val)", 3.0f, Literal.of(1), Literal.of(2));
    }

    @Test
    public void testPrimitiveReturnTypeAndInputArguments() throws Exception {
        registerUserDefinedFunction(
            "f",
            DataTypes.FLOAT,
            List.of(DataTypes.DOUBLE, DataTypes.SHORT),
            "function f(x, y) { return x + y; }");
        assertEvaluate("f(double_val, short_val)", 3.0f, Literal.of(1), Literal.of(2));
    }

    @Test
    public void testGeoTypeReturnTypeWithDoubleArray() throws Exception {
        registerUserDefinedFunction(
            "f",
            DataTypes.GEO_POINT,
            List.of(),
            "function f() { return [1, 1]; }");
        assertEvaluate("f()", new PointImpl(1.0, 1.0, JtsSpatialContext.GEO));
    }

    @Test
    public void testGeoTypeReturnTypeWithWKT() throws Exception {
        registerUserDefinedFunction(
            "f",
            DataTypes.GEO_POINT,
            List.of(),
            "function f() { return \"POINT (1.0 2.0)\"; }");
        assertEvaluate("f()", new PointImpl(1.0, 2.0, JtsSpatialContext.GEO));
    }

    @Test
    public void testOverloadingUserDefinedFunctions() throws Exception {
        registerUserDefinedFunction("f", DataTypes.LONG, List.of(), "function f() { return 1; }");
        registerUserDefinedFunction("f", DataTypes.LONG, List.of(DataTypes.LONG), "function f(x) { return x; }");
        registerUserDefinedFunction("f", DataTypes.LONG, List.of(DataTypes.LONG, DataTypes.INTEGER),
            "function f(x, y) { return x + y; }");
        assertEvaluate("f()", 1L);
        assertEvaluate("f(x)", 2L, Literal.of(2));
        assertEvaluate("f(x, a)", 3L, Literal.of(2), Literal.of(1));
    }

    @Test
    public void testFunctionWrongNameInFunctionBody() {
        var udfMeta = new UserDefinedFunctionMetaData(
            Schemas.DOC_SCHEMA_NAME,
            "f",
            Collections.singletonList(FunctionArgumentDefinition.of(DataTypes.DOUBLE)),
            DataTypes.DOUBLE_ARRAY,
            JS,
            "function test() { return 1; }");

        assertThat(
            udfService.getLanguage(JS).validate(udfMeta),
            containsString(
                "The name of the function signature 'f' doesn't " +
                "match the function name in the function definition"));
    }

    @Test
    public void testNormalizeOnObjectInput() throws Exception {
        registerUserDefinedFunction(
            "f",
            DataTypes.UNTYPED_OBJECT,
            List.of(DataTypes.UNTYPED_OBJECT),
            "function f(x) { return x; }");
        assertNormalize("f({})", isLiteral(Map.of()));
    }

    @Test
    public void testNormalizeOnArrayInput() throws Exception {
        registerUserDefinedFunction(
            "f",
            DataTypes.LONG,
            List.of(DataTypes.DOUBLE_ARRAY),
            "function f(x) { return x[1]; }");
        assertNormalize("f([1.0, 2.0])", isLiteral(2L));
    }

    @Test
    public void testNormalizeOnStringInputs() throws Exception {
        registerUserDefinedFunction(
            "f",
            DataTypes.STRING,
            List.of(DataTypes.STRING),
            "function f(x) { return x; }");
        assertNormalize("f('bar')", isLiteral("bar"));
    }

    @Test
    public void testAccessJavaClasses() throws Exception {
        expectedException.expect(io.crate.exceptions.ScriptException.class);
        expectedException.expectMessage(containsString("Java is not defined"));
        registerUserDefinedFunction(
            "f",
            DataTypes.LONG,
            List.of(DataTypes.LONG),
            "function f(x) { var File = Java.type(\"java.io.File\"); return x; }");
        assertEvaluate("f(x)", 1L, Literal.of(1L));
    }

    @Test
    public void testEvaluateBytesRefConvertedToString() throws Exception {
        registerUserDefinedFunction(
            "f",
            DataTypes.STRING,
            List.of(DataTypes.STRING),
            "function f(name) { return 'foo' + name; }");
        assertEvaluate("f(name)", "foobar", Literal.of("bar"));
    }

    @Test
    public void testJavaScriptFunctionReturnsUndefined() throws Exception {
        registerUserDefinedFunction(
            "f",
            DataTypes.STRING,
            List.of(DataTypes.STRING),
            "function f(name) { }");
        assertEvaluate("f(name)", null, Literal.of("bar"));
    }

    @Test
    public void testJavaScriptFunctionReturnsNull() throws Exception {
        registerUserDefinedFunction(
            "f",
            DataTypes.STRING,
            List.of(),
            "function f() { return null; }");
        assertEvaluate("f()", null);
    }

    @Test
    public void testStringArrayTypeArgument() throws Exception {
        registerUserDefinedFunction(
            "f",
            DataTypes.STRING,
            List.of(DataTypes.STRING_ARRAY),
            "function f(a) { return Array.prototype.join.call(a, '.'); }");
        assertEvaluate("f(['a', 'b'])", is("a.b"));
        assertEvaluate("f(['a', 'b'])", is("a.b"), Literal.of(List.of("a", "b"), DataTypes.STRING_ARRAY));
    }
}

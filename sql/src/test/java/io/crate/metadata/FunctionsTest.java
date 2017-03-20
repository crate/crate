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

package io.crate.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.Option;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.FunctionArgumentDefinition;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.data.Row;
import io.crate.operation.udf.UserDefinedFunctionFactory;
import io.crate.operation.udf.UserDefinedFunctionMetaData;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static io.crate.metadata.Schemas.*;
import static io.crate.testing.SymbolMatchers.isLiteral;

public class FunctionsTest extends CrateUnitTest {

    private SQLExecutor e;

    private final FunctionImplementation customFoo = UserDefinedFunctionFactory.of(
        new UserDefinedFunctionMetaData("custom", "foo",
            ImmutableList.of(FunctionArgumentDefinition.of(DataTypes.STRING)),
            DataTypes.STRING,
            "javascript",
            "function foo(x) { return x; }")
    );

    private final FunctionImplementation docLower = UserDefinedFunctionFactory.of(
        new UserDefinedFunctionMetaData("doc", "lower",
            ImmutableList.of(FunctionArgumentDefinition.of(DataTypes.STRING)),
            DataTypes.STRING,
            "javascript",
            "function lower(x) { return x; }")
    );

    @Before
    public void before() {
        e = SQLExecutor.builder(new NoopClusterService()).enableDefaultTables().build();
        e.functions().registerSchemaFunctionResolvers(
            customFoo.info().ident().schema(),
            e.functions().generateFunctionResolvers(ImmutableMap.of(customFoo.info().ident(), customFoo))
        );
        e.functions().registerSchemaFunctionResolvers(
            docLower.info().ident().schema(),
            e.functions().generateFunctionResolvers(ImmutableMap.of(docLower.info().ident(), docLower))
        );
    }

    @Test
    public void testUserDefinedFunctionIgnoresSessionSchema() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        e.analyzer.boundAnalyze(SqlParser.createStatement("select foo('foo');"),
            new SessionContext(0, Option.NONE, "custom"),
            new ParameterContext(Row.EMPTY, Collections.emptyList())).analyzedStatement();
    }

    @Test
    public void testBuiltinFunctionIgnoresSessionSchema() throws Exception {
        SelectAnalyzedStatement analysis = (SelectAnalyzedStatement) e.analyzer.boundAnalyze(
            SqlParser.createStatement("select abs(1);"),
            new SessionContext(0, Option.NONE, "bar"),
            new ParameterContext(Row.EMPTY, Collections.emptyList())).analyzedStatement();
        assertThat(analysis.relation().querySpec().outputs().get(0), isLiteral(1L));
    }

    @Test
    public void testUserDefinedFunctionInCustomSchemaAccessibleViaSchemaName() throws Exception {
        SelectAnalyzedStatement analysis = (SelectAnalyzedStatement) e.analyzer.boundAnalyze(
            SqlParser.createStatement("select custom.foo('foo');"),
            new SessionContext(0, Option.NONE, null),
            new ParameterContext(Row.EMPTY, Collections.emptyList())).analyzedStatement();
        assertThat(analysis.relation().querySpec().outputs().get(0), isLiteral("foo"));
    }

    @Test
    public void testBuiltinAndUserDefinedFunctionHaveSameNameThenBuiltinUsed() throws Exception {
        SelectAnalyzedStatement analysis = (SelectAnalyzedStatement) e.analyzer.boundAnalyze(
            SqlParser.createStatement("select lower('Foo');"),
            new SessionContext(0, Option.NONE, null),
            new ParameterContext(Row.EMPTY, Collections.emptyList())).analyzedStatement();
        assertThat(analysis.relation().querySpec().outputs().get(0), isLiteral("foo"));
    }

    @Test
    public void testBuiltinAndUserDefinedFunctionHaveSameNameUDFAccessibleViaSchemaName() throws Exception {
        SelectAnalyzedStatement analysis = (SelectAnalyzedStatement) e.analyzer.boundAnalyze(
            SqlParser.createStatement("select doc.lower('Foo');"),
            new SessionContext(0, Option.NONE, null),
            new ParameterContext(Row.EMPTY, Collections.emptyList())).analyzedStatement();
        assertThat(analysis.relation().querySpec().outputs().get(0), isLiteral("Foo"));
    }

    @Test
    public void testBuiltinAndUserDefinedFunctionHaveSameNameSessionSchemaIsDefaultThenBuiltFunctionUsed()
        throws Exception {
        SelectAnalyzedStatement analysis = (SelectAnalyzedStatement) e.analyzer.boundAnalyze(
            SqlParser.createStatement("select lower('Foo');"),
            new SessionContext(0, Option.NONE, DEFAULT_SCHEMA_NAME),
            new ParameterContext(Row.EMPTY, Collections.emptyList())).analyzedStatement();
        assertThat(analysis.relation().querySpec().outputs().get(0), isLiteral("foo"));
    }
}
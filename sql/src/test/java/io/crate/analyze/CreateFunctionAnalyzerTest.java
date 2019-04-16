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

package io.crate.analyze;

import io.crate.action.sql.Option;
import io.crate.action.sql.SessionContext;
import io.crate.auth.user.User;
import io.crate.data.Row;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Literal;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CreateFunctionAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void initExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService).build();
    }

    @Test
    public void testCreateFunctionSimple() {
        AnalyzedStatement analyzedStatement = e.analyze("CREATE FUNCTION bar(long, long)" +
            " RETURNS long LANGUAGE dummy_lang AS 'function(a, b) { return a + b; }'");
        assertThat(analyzedStatement, instanceOf(CreateFunctionAnalyzedStatement.class));

        CreateFunctionAnalyzedStatement analysis = (CreateFunctionAnalyzedStatement) analyzedStatement;
        assertThat(analysis.schema(), is("doc"));
        assertThat(analysis.name(), is("bar"));
        assertThat(analysis.replace(), is(false));
        assertThat(analysis.returnType(), is(DataTypes.LONG));
        assertThat(analysis.arguments().get(0), is(FunctionArgumentDefinition.of(DataTypes.LONG)));
        assertThat(analysis.arguments().get(1), is(FunctionArgumentDefinition.of(DataTypes.LONG)));
        assertThat(analysis.language(), is(Literal.fromObject("dummy_lang")));
        assertThat(analysis.definition(), is(Literal.fromObject("function(a, b) { return a + b; }")));
    }

    @Test
    public void testCreateFunctionWithSchemaName() {
        CreateFunctionAnalyzedStatement analyzedStatement = e.analyze("CREATE FUNCTION foo.bar(long, long)" +
            " RETURNS long LANGUAGE dummy_lang AS 'function(a, b) { return a + b; }'");
        assertThat(analyzedStatement.schema(), is("foo"));
        assertThat(analyzedStatement.name(), is("bar"));
    }

    @Test
    public void testCreateFunctionWithSessionSetSchema() throws Exception {
        CreateFunctionAnalyzedStatement analysis = (CreateFunctionAnalyzedStatement) e.analyzer.boundAnalyze(
            SqlParser.createStatement("CREATE FUNCTION bar(long, long)" +
                " RETURNS long LANGUAGE dummy_lang AS 'function(a, b) { return a + b; }'"),
            new CoordinatorTxnCtx(new SessionContext(0, Option.NONE, User.CRATE_USER, s -> {}, t -> {}, "my_schema")),
            new ParameterContext(Row.EMPTY, Collections.emptyList())).analyzedStatement();

        assertThat(analysis.schema(), is("my_schema"));
        assertThat(analysis.name(), is("bar"));
    }

    @Test
    public void testCreateFunctionExplicitSchemaSupersedesSessionSchema() throws Exception {
        CreateFunctionAnalyzedStatement analysis = (CreateFunctionAnalyzedStatement) e.analyzer.boundAnalyze(
            SqlParser.createStatement("CREATE FUNCTION my_other_schema.bar(long, long)" +
                " RETURNS long LANGUAGE dummy_lang AS 'function(a, b) { return a + b; }'"),
            new CoordinatorTxnCtx(new SessionContext(0, Option.NONE, User.CRATE_USER, s -> {}, t -> {}, "my_schema")),
            new ParameterContext(Row.EMPTY, Collections.emptyList())).analyzedStatement();

        assertThat(analysis.schema(), is("my_other_schema"));
        assertThat(analysis.name(), is("bar"));
    }

    @Test
    public void testCreateFunctionOrReplace() {
        AnalyzedStatement analyzedStatement = e.analyze("CREATE OR REPLACE FUNCTION bar()" +
            " RETURNS long LANGUAGE dummy_lang AS 'function() { return 1; }'");
        assertThat(analyzedStatement, instanceOf(CreateFunctionAnalyzedStatement.class));

        CreateFunctionAnalyzedStatement analysis = (CreateFunctionAnalyzedStatement) analyzedStatement;
        assertThat(analysis.name(), is("bar"));
        assertThat(analysis.replace(), is(true));
        assertThat(analysis.returnType(), is(DataTypes.LONG));
        assertThat(analysis.language(), is(Literal.fromObject("dummy_lang")));
        assertThat(analysis.definition(), is(Literal.fromObject("function() { return 1; }")));
    }

    @Test
    public void testCreateFunctionWithComplexGeoDataTypes() {
        AnalyzedStatement analyzedStatement = e.analyze("CREATE FUNCTION bar(geo_point, geo_shape)" +
            " RETURNS geo_point LANGUAGE dummy_lang AS 'function() { return 1; }'");
        assertThat(analyzedStatement, instanceOf(CreateFunctionAnalyzedStatement.class));

        CreateFunctionAnalyzedStatement analysis = (CreateFunctionAnalyzedStatement) analyzedStatement;
        assertThat(analysis.name(), is("bar"));
        assertThat(analysis.replace(), is(false));
        assertThat(analysis.returnType(), is(DataTypes.GEO_POINT));
        assertThat(analysis.arguments().get(0), is(FunctionArgumentDefinition.of(DataTypes.GEO_POINT)));
        assertThat(analysis.arguments().get(1), is(FunctionArgumentDefinition.of(DataTypes.GEO_SHAPE)));
        assertThat(analysis.language(), is(Literal.fromObject("dummy_lang")));
    }

    @Test
    public void testCreateFunctionWithComplexComplexTypes() {
        AnalyzedStatement analyzedStatement = e.analyze(
            "CREATE FUNCTION" +
            "   bar(array(integer)," +
            "   object, ip," +
            "   timestamp with time zone" +
            ") RETURNS array(geo_point) LANGUAGE dummy_lang AS 'function() { return 1; }'");
        assertThat(analyzedStatement, instanceOf(CreateFunctionAnalyzedStatement.class));

        CreateFunctionAnalyzedStatement analysis = (CreateFunctionAnalyzedStatement) analyzedStatement;
        assertThat(analysis.name(), is("bar"));
        assertThat(analysis.replace(), is(false));
        assertThat(analysis.returnType(), is(new ArrayType(DataTypes.GEO_POINT)));
        assertThat(analysis.arguments().get(0), is(FunctionArgumentDefinition.of(new ArrayType(DataTypes.INTEGER))));
        assertThat(analysis.arguments().get(1), is(FunctionArgumentDefinition.of(ObjectType.untyped())));
        assertThat(analysis.arguments().get(2), is(FunctionArgumentDefinition.of(DataTypes.IP)));
        assertThat(analysis.arguments().get(3), is(FunctionArgumentDefinition.of(DataTypes.TIMESTAMPZ)));
        assertThat(analysis.language(), is(Literal.fromObject("dummy_lang")));
    }
}

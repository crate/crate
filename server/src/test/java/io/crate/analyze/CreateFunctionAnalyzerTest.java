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

package io.crate.analyze;

import static io.crate.testing.Asserts.assertThat;

import org.junit.Before;
import org.junit.Test;

import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.role.Role;

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
        assertThat(analyzedStatement).isExactlyInstanceOf(AnalyzedCreateFunction.class);

        AnalyzedCreateFunction analysis = (AnalyzedCreateFunction) analyzedStatement;
        assertThat(analysis.schema()).isEqualTo("doc");
        assertThat(analysis.name()).isEqualTo("bar");
        assertThat(analysis.replace()).isEqualTo(false);
        assertThat(analysis.returnType()).isEqualTo(DataTypes.LONG);
        assertThat(analysis.arguments().get(0)).isEqualTo(FunctionArgumentDefinition.of(DataTypes.LONG));
        assertThat(analysis.arguments().get(1)).isEqualTo(FunctionArgumentDefinition.of(DataTypes.LONG));
        assertThat(analysis.language()).isLiteral("dummy_lang");
        assertThat(analysis.definition()).isLiteral("function(a, b) { return a + b; }");
    }

    @Test
    public void testCreateFunctionWithSchemaName() {
        AnalyzedCreateFunction analyzedStatement = e.analyze(
            "CREATE FUNCTION foo.bar(long, long)" +
            " RETURNS long LANGUAGE dummy_lang AS 'function(a, b) { return a + b; }'");
        assertThat(analyzedStatement.schema()).isEqualTo("foo");
        assertThat(analyzedStatement.name()).isEqualTo("bar");
    }

    @Test
    public void testCreateFunctionWithSessionSetSchema() throws Exception {
        AnalyzedCreateFunction analysis = (AnalyzedCreateFunction) e.analyzer.analyze(
            SqlParser.createStatement(
                "CREATE FUNCTION bar(long, long)" +
                " RETURNS long LANGUAGE dummy_lang AS 'function(a, b) { return a + b; }'"),
            new CoordinatorSessionSettings(Role.CRATE_USER, "my_schema"),
            ParamTypeHints.EMPTY,
            e.cursors
        );

        assertThat(analysis.schema()).isEqualTo("my_schema");
        assertThat(analysis.name()).isEqualTo("bar");
    }

    @Test
    public void testCreateFunctionExplicitSchemaSupersedesSessionSchema() throws Exception {
        AnalyzedCreateFunction analysis = (AnalyzedCreateFunction) e.analyzer.analyze(
            SqlParser.createStatement("CREATE FUNCTION my_other_schema.bar(long, long)" +
                " RETURNS long LANGUAGE dummy_lang AS 'function(a, b) { return a + b; }'"),
            new CoordinatorSessionSettings(Role.CRATE_USER, "my_schema"),
            ParamTypeHints.EMPTY,
            e.cursors
        );

        assertThat(analysis.schema()).isEqualTo("my_other_schema");
        assertThat(analysis.name()).isEqualTo("bar");
    }

    @Test
    public void testCreateFunctionOrReplace() {
        AnalyzedStatement analyzedStatement = e.analyze("CREATE OR REPLACE FUNCTION bar()" +
            " RETURNS long LANGUAGE dummy_lang AS 'function() { return 1; }'");
        assertThat(analyzedStatement).isExactlyInstanceOf(AnalyzedCreateFunction.class);

        AnalyzedCreateFunction analysis = (AnalyzedCreateFunction) analyzedStatement;
        assertThat(analysis.name()).isEqualTo("bar");
        assertThat(analysis.replace()).isEqualTo(true);
        assertThat(analysis.returnType()).isEqualTo(DataTypes.LONG);
        assertThat(analysis.language()).isLiteral("dummy_lang");
        assertThat(analysis.definition()).isLiteral("function() { return 1; }");
    }

    @Test
    public void testCreateFunctionWithComplexGeoDataTypes() {
        AnalyzedStatement analyzedStatement = e.analyze("CREATE FUNCTION bar(geo_point, geo_shape)" +
            " RETURNS geo_point LANGUAGE dummy_lang AS 'function() { return 1; }'");
        assertThat(analyzedStatement).isExactlyInstanceOf(AnalyzedCreateFunction.class);

        AnalyzedCreateFunction analysis = (AnalyzedCreateFunction) analyzedStatement;
        assertThat(analysis.name()).isEqualTo("bar");
        assertThat(analysis.replace()).isEqualTo(false);
        assertThat(analysis.returnType()).isEqualTo(DataTypes.GEO_POINT);
        assertThat(analysis.arguments().get(0)).isEqualTo(FunctionArgumentDefinition.of(DataTypes.GEO_POINT));
        assertThat(analysis.arguments().get(1)).isEqualTo(FunctionArgumentDefinition.of(DataTypes.GEO_SHAPE));
        assertThat(analysis.language()).isLiteral("dummy_lang");
    }

    @Test
    public void testCreateFunctionWithComplexComplexTypes() {
        AnalyzedStatement analyzedStatement = e.analyze(
            "CREATE FUNCTION" +
            "   bar(array(integer)," +
            "   object, ip," +
            "   timestamp with time zone" +
            ") RETURNS array(geo_point) LANGUAGE dummy_lang AS 'function() { return 1; }'");
        assertThat(analyzedStatement).isExactlyInstanceOf(AnalyzedCreateFunction.class);

        AnalyzedCreateFunction analysis = (AnalyzedCreateFunction) analyzedStatement;
        assertThat(analysis.name()).isEqualTo("bar");
        assertThat(analysis.replace()).isEqualTo(false);
        assertThat(analysis.returnType()).isEqualTo(new ArrayType<>(DataTypes.GEO_POINT));
        assertThat(analysis.arguments().get(0)).isEqualTo(FunctionArgumentDefinition.of(new ArrayType<>(DataTypes.INTEGER)));
        assertThat(analysis.arguments().get(1)).isEqualTo(FunctionArgumentDefinition.of(DataTypes.UNTYPED_OBJECT));
        assertThat(analysis.arguments().get(2)).isEqualTo(FunctionArgumentDefinition.of(DataTypes.IP));
        assertThat(analysis.arguments().get(3)).isEqualTo(FunctionArgumentDefinition.of(DataTypes.TIMESTAMPZ));
        assertThat(analysis.language()).isLiteral("dummy_lang");
    }
}

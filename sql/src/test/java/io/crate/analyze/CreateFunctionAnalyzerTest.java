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

import io.crate.sql.tree.Literal;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CreateFunctionAnalyzerTest extends CrateUnitTest {

    private SQLExecutor e = SQLExecutor.builder(new NoopClusterService()).build();

    @Test
    public void testCreateFunctionSimple() throws Exception {
        AnalyzedStatement analyzedStatement = e.analyze("CREATE FUNCTION bar(long, long)" +
            " RETURNS long LANGUAGE javascript AS 'function(a, b) { return a + b; }'");
        assertThat(analyzedStatement, instanceOf(CreateFunctionAnalyzedStatement.class));

        CreateFunctionAnalyzedStatement analysis = (CreateFunctionAnalyzedStatement) analyzedStatement;
        assertThat(analysis.name(), is("bar"));
        assertThat(analysis.replace(), is(false));
        assertThat(analysis.returnType(), is(DataTypes.LONG));
        assertThat(analysis.arguments().get(0), is(FunctionArgumentDefinition.of(DataTypes.LONG)));
        assertThat(analysis.arguments().get(1), is(FunctionArgumentDefinition.of(DataTypes.LONG)));
        assertThat(analysis.language(), is(Literal.fromObject("javascript")));
        assertThat(analysis.body(), is(Literal.fromObject("function(a, b) { return a + b }")));
        assertThat(analysis.options().size(), is(0));
    }

    @Test
    public void testCreateFunctionWithOptions() throws Exception {
        AnalyzedStatement analyzedStatement = e.analyze("CREATE FUNCTION bar()" +
            " RETURNS long LANGUAGE javascript STRICT AS 'function() { return 1; }'");
        assertThat(analyzedStatement, instanceOf(CreateFunctionAnalyzedStatement.class));

        CreateFunctionAnalyzedStatement analysis = (CreateFunctionAnalyzedStatement) analyzedStatement;
        assertThat(analysis.name(), is("bar"));
        assertThat(analysis.replace(), is(false));
        assertThat(analysis.returnType(), is(DataTypes.LONG));
        assertThat(analysis.arguments().size(), is(0));
        assertThat(analysis.language(), is(Literal.fromObject("javascript")));
        assertThat(analysis.body(), is(Literal.fromObject("function() { return 1; }")));
        assertThat(analysis.options().size(), is(1));
        assertThat(analysis.options().contains("STRICT"), is(true));
    }

    @Test
    public void testCreateFunctionOrReplace() throws Exception {
        AnalyzedStatement analyzedStatement = e.analyze("CREATE OR REPLACE FUNCTION bar()" +
            " RETURNS long LANGUAGE javascript STRICT AS 'function() { return 1; }'");
        assertThat(analyzedStatement, instanceOf(CreateFunctionAnalyzedStatement.class));

        CreateFunctionAnalyzedStatement analysis = (CreateFunctionAnalyzedStatement) analyzedStatement;
        assertThat(analysis.name(), is("bar"));
        assertThat(analysis.replace(), is(true));
        assertThat(analysis.returnType(), is(DataTypes.LONG));
        assertThat(analysis.language(), is(Literal.fromObject("javascript")));
        assertThat(analysis.body(), is(Literal.fromObject("function() { return 1 }")));
        assertThat(analysis.options().size(), is(1));
        assertThat(analysis.options().contains("STRICT"), is(true));
    }

    @Test
    public void testCreateFunctionWithComplexGeoDataTypes() throws Exception {
        AnalyzedStatement analyzedStatement = e.analyze("CREATE FUNCTION foo.bar(geo_point, geo_shape)" +
            " RETURNS geo_point LANGUAGE javascript AS 'function() { return 1; }'");
        assertThat(analyzedStatement, instanceOf(CreateFunctionAnalyzedStatement.class));

        CreateFunctionAnalyzedStatement analysis = (CreateFunctionAnalyzedStatement) analyzedStatement;
        assertThat(analysis.name(), is("foo.bar"));
        assertThat(analysis.replace(), is(false));
        assertThat(analysis.returnType(), is(DataTypes.GEO_POINT));
        assertThat(analysis.arguments().get(0), is(FunctionArgumentDefinition.of(DataTypes.GEO_POINT)));
        assertThat(analysis.arguments().get(1), is(FunctionArgumentDefinition.of(DataTypes.GEO_SHAPE)));
        assertThat(analysis.language(), is(Literal.fromObject("javascript")));
    }

    @Test
    public void testCreateFunctionWithComplexComplexTypes() throws Exception {
        AnalyzedStatement analyzedStatement = e.analyze("CREATE FUNCTION" +
            " foo.bar(array(integer), object, ip, timestamp)" +
            " RETURNS array(geo_point) LANGUAGE javascript AS 'function() { return 1; }'");
        assertThat(analyzedStatement, instanceOf(CreateFunctionAnalyzedStatement.class));

        CreateFunctionAnalyzedStatement analysis = (CreateFunctionAnalyzedStatement) analyzedStatement;
        assertThat(analysis.name(), is("foo.bar"));
        assertThat(analysis.replace(), is(false));
        assertThat(analysis.returnType(), is(new ArrayType(DataTypes.GEO_POINT)));
        assertThat(analysis.arguments().get(0), is(FunctionArgumentDefinition.of(new ArrayType(DataTypes.INTEGER))));
        assertThat(analysis.arguments().get(1), is(FunctionArgumentDefinition.of(DataTypes.OBJECT)));
        assertThat(analysis.arguments().get(2), is(FunctionArgumentDefinition.of(DataTypes.IP)));
        assertThat(analysis.arguments().get(3), is(FunctionArgumentDefinition.of(DataTypes.TIMESTAMP)));
        assertThat(analysis.language(), is(Literal.fromObject("javascript")));
    }
}

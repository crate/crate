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

import io.crate.action.sql.SessionContext;
import io.crate.user.User;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class DropFunctionAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void initExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService).build();
    }

    @Test
    public void testDropFunctionSimple() throws Exception {
        AnalyzedStatement analyzedStatement = e.analyze("DROP FUNCTION bar(long, object)");
        assertThat(analyzedStatement, instanceOf(AnalyzedDropFunction.class));

        AnalyzedDropFunction analysis = (AnalyzedDropFunction) analyzedStatement;
        assertThat(analysis.schema(), is("doc"));
        assertThat(analysis.name(), is("bar"));
        assertThat(analysis.ifExists(), is(false));
        assertThat(analysis.argumentTypes().get(0), is(DataTypes.LONG));
        assertThat(analysis.argumentTypes().get(1).id(), is(ObjectType.ID));
    }

    @Test
    public void testDropFunctionWithSessionSetSchema() throws Exception {
        AnalyzedDropFunction analysis = (AnalyzedDropFunction) e.analyzer.analyze(
            SqlParser.createStatement("DROP FUNCTION bar(long, object)"),
            new SessionContext(User.CRATE_USER, "my_schema"),
            ParamTypeHints.EMPTY);

        assertThat(analysis.schema(), is("my_schema"));
        assertThat(analysis.name(), is("bar"));
    }

    @Test
    public void testDropFunctionExplicitSchemaSupersedesSessionSchema() throws Exception {
        AnalyzedDropFunction analysis = (AnalyzedDropFunction) e.analyzer.analyze(
            SqlParser.createStatement("DROP FUNCTION my_other_schema.bar(long, object)"),
            new SessionContext(User.CRATE_USER, "my_schema"),
            ParamTypeHints.EMPTY);

        assertThat(analysis.schema(), is("my_other_schema"));
        assertThat(analysis.name(), is("bar"));
    }

    @Test
    public void testDropFunctionIfExists() throws Exception {
        AnalyzedStatement analyzedStatement = e.analyze("DROP FUNCTION IF EXISTS bar(arg_long long, arg_obj object)");
        assertThat(analyzedStatement, instanceOf(AnalyzedDropFunction.class));

        AnalyzedDropFunction analysis = (AnalyzedDropFunction) analyzedStatement;
        assertThat(analysis.name(), is("bar"));
        assertThat(analysis.ifExists(), is(true));
        assertThat(analysis.argumentTypes().get(0), is(DataTypes.LONG));
        assertThat(analysis.argumentTypes().get(1).id(), is(ObjectType.ID));
    }
}

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

import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class DropFunctionAnalyzerTest extends CrateUnitTest {

    private SQLExecutor e = SQLExecutor.builder(new NoopClusterService()).build();

    @Test
    public void testDropFunctionSimple() throws Exception {
        AnalyzedStatement analyzedStatement = e.analyze("DROP FUNCTION bar(long, object)");
        assertThat(analyzedStatement, instanceOf(DropFunctionAnalyzedStatement.class));

        DropFunctionAnalyzedStatement analysis = (DropFunctionAnalyzedStatement) analyzedStatement;
        assertThat(analysis.name(), is("bar"));
        assertThat(analysis.ifExists(), is(false));
        assertThat(analysis.arguments().get(0), is(FunctionArgumentDefinition.of(DataTypes.LONG)));
        assertThat(analysis.arguments().get(1), is(FunctionArgumentDefinition.of(DataTypes.OBJECT)));
    }

    @Test
    public void testDropFunctionIfExists() throws Exception {
        AnalyzedStatement analyzedStatement = e.analyze("DROP FUNCTION IF EXISTS foo.bar(arg_long long, arg_obj object)");
        assertThat(analyzedStatement, instanceOf(DropFunctionAnalyzedStatement.class));

        DropFunctionAnalyzedStatement analysis = (DropFunctionAnalyzedStatement) analyzedStatement;
        assertThat(analysis.name(), is("foo.bar"));
        assertThat(analysis.ifExists(), is(true));
        assertThat(analysis.arguments().get(0), is(FunctionArgumentDefinition.of("arg_long", DataTypes.LONG)));
        assertThat(analysis.arguments().get(1), is(FunctionArgumentDefinition.of("arg_obj", DataTypes.OBJECT)));
    }
}

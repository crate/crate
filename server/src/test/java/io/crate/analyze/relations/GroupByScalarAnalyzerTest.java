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

package io.crate.analyze.relations;

import io.crate.expression.symbol.Symbols;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.core.Is.is;


public class GroupByScalarAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor executor;

    @Before
    public void prepare() throws IOException {
        executor = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    @Test
    public void testScalarFunctionArgumentsNotAllInGroupByThrowsException() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("'(id * other_id)' must appear in the GROUP BY");
        executor.analyze("select id * other_id from users group by id");
    }

    @Test
    public void testValidGroupByWithScalarAndMultipleColumns() throws Exception {
        AnalyzedRelation relation = executor.analyze("select id * other_id from users group by id, other_id");
        assertThat(Symbols.pathFromSymbol(relation.outputs().get(0)).sqlFqn(), is("(id * other_id)"));
    }

    @Test
    public void testValidGroupByWithScalar() throws Exception {
        AnalyzedRelation relation = executor.analyze("select id * 2 from users group by id");
        assertThat(Symbols.pathFromSymbol(relation.outputs().get(0)).sqlFqn(), is("(id * 2::bigint)"));
    }

    @Test
    public void testValidGroupByWithMultipleScalarFunctions() throws Exception {
        AnalyzedRelation relation = executor.analyze("select abs(id * 2) from users group by id");
        assertThat(Symbols.pathFromSymbol(relation.outputs().get(0)).sqlFqn(), is("abs((id * 2::bigint))"));
    }
}

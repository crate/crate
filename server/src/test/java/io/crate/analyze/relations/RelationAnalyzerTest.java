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

import io.crate.exceptions.RelationValidationException;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.tablefunctions.ValuesFunction;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class RelationAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor executor;

    @Before
    public void prepare() throws IOException {
        executor = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    @Test
    public void testValidateUsedRelationsInJoinConditions() throws Exception {
        expectedException.expect(RelationValidationException.class);
        expectedException.expectMessage("missing FROM-clause entry for relation '[doc.t3]'");
        executor.analyze("select * from t1 join t2 on t1.a = t3.c join t3 on t2.b = t3.c");
    }

    @Test
    public void testColumnNameFromArrayComparisonExpression() throws Exception {
        AnalyzedRelation relation = executor.analyze("select 'foo' = any(partitioned_by) " +
                                                     "from information_schema.tables");
        assertThat(Symbols.pathFromSymbol(relation.outputs().get(0)).sqlFqn(), is("('foo' = ANY(partitioned_by))"));
    }

    @Test
    public void test_process_values_result_in_table_function_with_values_name() {
        AnalyzedRelation relation = executor.analyze("VALUES ([1, 2], 'a')");
        assertThat(relation, instanceOf(TableFunctionRelation.class));
        assertThat(relation.relationName().toString(), is(ValuesFunction.NAME));
    }
}

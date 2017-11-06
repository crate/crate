/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.symbol.Function;
import io.crate.exceptions.RelationUnknownException;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.operator.EqOperator;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import static io.crate.analyze.TableDefinitions.USER_TABLE_IDENT;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DeleteAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    @Test
    public void testDeleteWhere() throws Exception {
        DeleteAnalyzedStatement statement = e.analyze("delete from users where name='Trillian'");
        DocTableRelation tableRelation = statement.analyzedRelation;
        TableInfo tableInfo = tableRelation.tableInfo();
        assertThat(USER_TABLE_IDENT, equalTo(tableInfo.ident()));

        assertThat(tableInfo.rowGranularity(), is(RowGranularity.DOC));

        Function whereClause = (Function) statement.whereClauses.get(0).query();
        assertEquals(EqOperator.NAME, whereClause.info().ident().name());
        assertFalse(whereClause.info().type() == FunctionInfo.Type.AGGREGATE);

        assertThat(whereClause.arguments().get(0), isReference("name"));
        assertThat(whereClause.arguments().get(1), isLiteral("Trillian"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDeleteSystemTable() throws Exception {
        e.analyze("delete from sys.nodes where name='Trillian'");
    }

    @Test
    public void testDeleteWhereSysColumn() throws Exception {
        expectedException.expect(RelationUnknownException.class);
        expectedException.expectMessage("Cannot resolve relation 'sys.nodes'");
        e.analyze("delete from users where sys.nodes.id = 'node_1'");
    }

    @Test
    public void testDeleteWherePartitionedByColumn() throws Exception {
        DeleteAnalyzedStatement statement = e.analyze("delete from parted where date = 1395874800000");
        assertThat(statement.whereClauses().get(0).hasQuery(), is(false));
        assertThat(statement.whereClauses().get(0).noMatch(), is(false));
        assertThat(statement.whereClauses().get(0).partitions().size(), is(1));
        assertThat(statement.whereClauses().get(0).partitions().get(0),
            is(".partitioned.parted.04732cpp6ks3ed1o60o30c1g"));
    }

    @Test
    public void testDeleteTableAlias() throws Exception {
        DeleteAnalyzedStatement expectedStatement = e.analyze("delete from users where name='Trillian'");
        DeleteAnalyzedStatement actualStatement = e.analyze("delete from users as u where u.name='Trillian'");

        assertThat(actualStatement.analyzedRelation.tableInfo(), equalTo(expectedStatement.analyzedRelation().tableInfo()));
        assertThat(actualStatement.whereClauses().get(0), equalTo(expectedStatement.whereClauses().get(0)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWhereClauseObjectArrayField() throws Exception {
        e.analyze("delete from users where friends['id'] = 5");
    }

    @Test
    public void testBulkDelete() throws Exception {
        DeleteAnalyzedStatement analysis = e.analyze("delete from users where id = ?", new Object[][]{
            new Object[]{1},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4},
        });
        assertThat(analysis.whereClauses().size(), is(4));
    }

    @Test
    public void testDeleteWhereVersionIsNullPredicate() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
            "Filtering \"_version\" in WHERE clause only works using the \"=\" operator, checking for a numeric value");
        e.analyze("delete from users where _version is null", new Object[]{1});
    }

    @Test
    public void testSensibleErrorOnDeleteComplexRelation() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("DELETE only works on base-table relations");
        e.analyze("delete from (select * from users) u");
    }
}

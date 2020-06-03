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
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.RelationUnknown;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.table.TableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static io.crate.analyze.TableDefinitions.USER_TABLE_IDENT;
import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class DeleteAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    @Test
    public void testDeleteWhere() throws Exception {
        AnalyzedDeleteStatement delete = e.analyze("delete from users where name='Trillian'");
        DocTableRelation tableRelation = delete.relation();
        TableInfo tableInfo = tableRelation.tableInfo();
        assertThat(USER_TABLE_IDENT, equalTo(tableInfo.ident()));
        assertThat(tableInfo.rowGranularity(), is(RowGranularity.DOC));

        assertThat(delete.query(), isFunction(EqOperator.NAME, isReference("name"), isLiteral("Trillian")));
    }

    @Test
    public void testDeleteSystemTable() throws Exception {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"sys.nodes\" doesn't support or allow DELETE "+
                                        "operations, as it is read-only.");
        e.analyze("delete from sys.nodes where name='Trillian'");
    }

    @Test
    public void testDeleteWhereSysColumn() throws Exception {
        expectedException.expect(RelationUnknown.class);
        expectedException.expectMessage("Relation 'sys.nodes' unknown");
        e.analyze("delete from users where sys.nodes.id = 'node_1'");
    }

    @Test
    public void testDeleteWherePartitionedByColumn() throws Exception {
        AnalyzedDeleteStatement delete = e.analyze("delete from parted where date = 1395874800000::timestamp");
        assertThat(delete.query(), isFunction(EqOperator.NAME, isReference("date"), isLiteral(1395874800000L)));
    }

    @Test
    public void testDeleteTableAlias() throws Exception {
        AnalyzedDeleteStatement expectedStatement = e.analyze("delete from users where name='Trillian'");
        AnalyzedDeleteStatement actualStatement = e.analyze("delete from users as u where u.name='Trillian'");

        assertThat(actualStatement.relation().tableInfo(), equalTo(expectedStatement.relation().tableInfo()));
        assertThat(actualStatement.query(), equalTo(expectedStatement.query()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWhereClauseObjectArrayField() throws Exception {
        e.analyze("delete from users where friends['id'] = 5");
    }

    @Test
    public void testBulkDelete() throws Exception {
        AnalyzedDeleteStatement delete = e.analyze(
            "delete from users where id = ?",
            new ParamTypeHints(List.of(DataTypes.INTEGER, DataTypes.INTEGER)));
        assertThat(delete.query(), isFunction(EqOperator.NAME, isReference("id"), instanceOf(ParameterSymbol.class)));
    }

    @Test
    public void testSensibleErrorOnDeleteComplexRelation() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot delete from relations other than base tables");
        e.analyze("delete from (select * from users) u");
    }
}

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

import static io.crate.analyze.TableDefinitions.USER_TABLE_IDENT;
import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.exactlyInstanceOf;
import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isLiteral;
import static io.crate.testing.Asserts.isReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.relations.DocTableRelation;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.scalar.timestamp.NowFunction;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.table.TableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class DeleteAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addPartitionedTable(
                TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION,
                TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS);
    }

    @Test
    public void testDeleteWhere() throws Exception {
        AnalyzedDeleteStatement delete = e.analyze("delete from users where name='Trillian'");
        DocTableRelation tableRelation = delete.relation();
        TableInfo tableInfo = tableRelation.tableInfo();
        assertThat(USER_TABLE_IDENT).isEqualTo(tableInfo.ident());
        assertThat(tableInfo.rowGranularity()).isEqualTo(RowGranularity.DOC);

        assertThat(delete.query()).isFunction(EqOperator.NAME, isReference("name"), isLiteral("Trillian"));
    }

    @Test
    public void testDeleteSystemTable() throws Exception {
        assertThatThrownBy(() -> e.analyze("delete from sys.nodes where name='Trillian'"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage("The relation \"sys.nodes\" doesn't support or allow DELETE " +
                        "operations");
    }

    @Test
    public void testDeleteWhereSysColumn() throws Exception {
        assertThatThrownBy(() -> e.analyze("delete from users where sys.nodes.id = 'node_1'"))
            .isExactlyInstanceOf(RelationUnknown.class)
            .hasMessage("Relation 'sys.nodes' unknown");
    }

    @Test
    public void testDeleteWherePartitionedByColumn() throws Exception {
        AnalyzedDeleteStatement delete = e.analyze("delete from parted where date = 1395874800000::timestamptz");
        assertThat(delete.query()).isFunction(EqOperator.NAME, isReference("date"), isLiteral(1395874800000L));
    }

    @Test
    public void testDeleteTableAlias() throws Exception {
        AnalyzedDeleteStatement expectedStatement = e.analyze("delete from users where name='Trillian'");
        AnalyzedDeleteStatement actualStatement = e.analyze("delete from users as u where u.name='Trillian'");

        assertThat(actualStatement.relation().tableInfo()).isEqualTo(expectedStatement.relation().tableInfo());
        assertThat(actualStatement.query()).isEqualTo(expectedStatement.query());
    }

    @Test
    public void testWhereClauseObjectArrayField() throws Exception {
        assertThatThrownBy(() -> e.analyze("delete from users where friends['id'] = 5"))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: (doc.users.friends['id'] = 5)," +
                                    " no overload found for matching argument types: (bigint_array, integer).");
    }

    @Test
    public void testBulkDelete() throws Exception {
        AnalyzedDeleteStatement delete = e.analyze(
            "delete from users where id = ?",
            new ParamTypeHints(List.of(DataTypes.INTEGER, DataTypes.INTEGER)));
        assertThat(delete.query())
            .isFunction(EqOperator.NAME, isReference("id"), exactlyInstanceOf(ParameterSymbol.class));
    }

    @Test
    public void testSensibleErrorOnDeleteComplexRelation() throws Exception {
        assertThatThrownBy(() -> e.analyze("delete from (select * from users) u"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Cannot delete from relations other than base tables");
    }

    @Test
    public void test_non_deterministic_function_is_not_normalized() {
        AnalyzedDeleteStatement analyzedDeleteStatement = e.analyze("delete from parted where date = now()");
        assertThat(analyzedDeleteStatement.query()).isFunction(
            EqOperator.NAME,
            isReference("date"),
            isFunction(NowFunction.NAME));
    }
}

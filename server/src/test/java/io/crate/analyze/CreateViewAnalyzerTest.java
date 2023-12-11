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

import static io.crate.testing.Asserts.assertList;
import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.isAlias;
import static io.crate.testing.Asserts.isReference;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Locale;

import org.junit.Before;
import org.junit.Test;

import io.crate.exceptions.InvalidRelationName;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.role.Role;

public class CreateViewAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private static final Role TEST_USER = Role.userOf("test_user");
    private SQLExecutor e;

    @Before
    public void setUpExecutor() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .setUser(TEST_USER)
            .addTable("create table t1 (x int)")
            .build();
    }

    @Test
    public void testCreateViewCreatesStatementWithNameAndAnalyzedRelation() {
        CreateViewStmt createView = e.analyze("create view v1 as select * from t1");

        assertThat(createView.name()).isEqualTo(new RelationName(e.getSessionSettings().searchPath().currentSchema(), "v1"));
        assertThat(createView.analyzedQuery()).isSQL("SELECT doc.t1.x");
        assertThat(createView.owner()).isEqualTo(TEST_USER);
    }

    @Test
    public void testCreateViewIncludingParamPlaceholder() {
        CreateViewStmt createView = e.analyze("create view v1 as select * from t1 where x = ?");
        assertThat(createView.analyzedQuery()).isSQL("SELECT doc.t1.x WHERE (doc.t1.x = $1)");
    }

    @Test
    public void testCreateViewCreatesViewInDefaultSchema() {
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService)
            .setSearchPath("firstSchema", "secondSchema")
            .build();
        CreateViewStmt createView = sqlExecutor.analyze("create view v1 as select * from sys.nodes");

        assertThat(createView.name()).isEqualTo(new RelationName(sqlExecutor.getSessionSettings().searchPath().currentSchema(), "v1"));
    }

    @Test
    public void testCreateOrReplaceViewCreatesStatementWithNameAndAnalyzedRelation() {
        CreateViewStmt createView = e.analyze("create or replace view v1 as select x from t1");
        assertThat(createView.replaceExisting()).isTrue();
    }

    @Test
    public void testViewNameWithDotsAreNotAllowed() {
        assertThatThrownBy(() -> e.analyze("create view \"v.1\" as select 1"))
            .isExactlyInstanceOf(InvalidRelationName.class)
            .hasMessage("Relation name \"doc.v.1\" is invalid.");
    }

    @Test
    public void testDuplicateColumnNamesMustNotBeAllowedInQuery() {
        assertThatThrownBy(() -> e.analyze("create view v1 as select x, x from t1"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Query in CREATE VIEW must not have duplicate column names");
    }

    @Test
    public void test_create_view_with_query_with_invalid_column_results_in_userfriendly_error_message() {
        assertThatThrownBy(() -> e.analyze("create view v1 as select invalid from t1"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Invalid query used in CREATE VIEW. Column invalid unknown. Query: SELECT \"invalid\"\nFROM \"t1\"\n");
    }

    @Test
    public void testAliasCanBeUsedToAvoidDuplicateColumnNamesInQuery() {
        CreateViewStmt createView = e.analyze("create view v1 as select x, x as y from t1");
        assertThat(createView.analyzedQuery().outputs())
            .satisfiesExactly(isReference("x"), isAlias("y", isReference("x")));
    }

    @Test
    public void testCreatingAViewInReadOnlySchemaIsProhibited() {
        for (String schema : Schemas.READ_ONLY_SYSTEM_SCHEMAS) {
            assertThatThrownBy(() ->
                e.analyze(String.format(Locale.ENGLISH, "create view %s.v1 as select 1", schema)))
                .as("creating a view in read-only schema must fail")
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot create relation in read-only schema: " + schema);
        }
    }

    @Test
    public void testCreatingAViewInBlobSchemaIsProhibited() {
        assertThatThrownBy(() -> e.analyze("create view blob.v1 as select 1"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Creating a view in the \"blob\" schema is not supported");
    }

    @Test
    public void test_create_view_with_any_select() {
        CreateViewStmt stmt = e.analyze(
            "CREATE OR REPLACE VIEW subselect_view_any AS SELECT (SELECT 1) = ANY([5])");
        assertList(stmt.analyzedQuery().outputs())
            .isSQL("((SELECT 1 FROM (empty_row)) = ANY([5]))");
    }
}

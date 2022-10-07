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

import static io.crate.testing.Asserts.isAlias;
import static io.crate.testing.Asserts.isReference;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Locale;

import org.junit.Before;
import org.junit.Test;

import io.crate.exceptions.InvalidRelationName;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.Asserts;
import io.crate.testing.SQLExecutor;
import io.crate.user.User;

public class CreateViewAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private User testUser = User.of("test_user");

    @Before
    public void setUpExecutor() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .setUser(testUser)
            .addTable("create table t1 (x int)")
            .build();
    }

    @Test
    public void testCreateViewCreatesStatementWithNameAndAnalyzedRelation() {
        CreateViewStmt createView = e.analyze("create view v1 as select * from t1");

        assertThat(createView.name(), is(new RelationName(e.getSessionContext().searchPath().currentSchema(), "v1")));
        assertThat(createView.analyzedQuery(), isSQL("SELECT doc.t1.x"));
        assertThat(createView.owner(), is(testUser));
    }

    @Test
    public void testCreateViewIncludingParamPlaceholder() {
        CreateViewStmt createView = e.analyze("create view v1 as select * from t1 where x = ?");
        assertThat(createView.analyzedQuery(), isSQL("SELECT doc.t1.x WHERE (doc.t1.x = $1)"));
    }

    @Test
    public void testCreateViewCreatesViewInDefaultSchema() {
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService)
            .setSearchPath("firstSchema", "secondSchema")
            .build();
        CreateViewStmt createView = sqlExecutor.analyze("create view v1 as select * from sys.nodes");

        assertThat(createView.name(), is(new RelationName(sqlExecutor.getSessionContext().searchPath().currentSchema(), "v1")));
    }

    @Test
    public void testCreateOrReplaceViewCreatesStatementWithNameAndAnalyzedRelation() {
        CreateViewStmt createView = e.analyze("create or replace view v1 as select x from t1");
        assertTrue(createView.replaceExisting());
    }

    @Test
    public void testViewNameWithDotsAreNotAllowed() {
        expectedException.expect(InvalidRelationName.class);
        e.analyze("create view \"v.1\" as select 1");
    }

    @Test
    public void testDuplicateColumnNamesMustNotBeAllowedInQuery() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Query in CREATE VIEW must not have duplicate column names");
        e.analyze("create view v1 as select x, x from t1");
    }

    @Test
    public void test_create_view_with_query_with_invalid_column_results_in_userfriendly_error_message() {
        expectedException.expectMessage("Invalid query used in CREATE VIEW. Column invalid unknown. Query: SELECT \"invalid\"");
        e.analyze("create view v1 as select invalid from t1");
    }

    @Test
    public void testAliasCanBeUsedToAvoidDuplicateColumnNamesInQuery() {
        CreateViewStmt createView = e.analyze("create view v1 as select x, x as y from t1");
        Asserts.assertThat(createView.analyzedQuery().outputs())
            .satisfiesExactly(isReference("x"), isAlias("y", isReference("x")));
    }

    @Test
    public void testCreatingAViewInReadOnlySchemaIsProhibited() {
        for (String schema : Schemas.READ_ONLY_SYSTEM_SCHEMAS) {
            try {
                e.analyze(String.format(Locale.ENGLISH, "create view %s.v1 as select 1", schema));
                fail("creating a view in read-only schema must fail");
            } catch (Exception e) {
                assertThat(e.getMessage(), is("Cannot create relation in read-only schema: " + schema));
            }
        }
    }

    @Test
    public void testCreatingAViewInBlobSchemaIsProhibited() {
        expectedException.expect(UnsupportedOperationException.class);
        e.analyze("create view blob.v1 as select 1");
    }

    @Test
    public void test_create_view_with_any_select() {
        CreateViewStmt stmt = e.analyze(
            "CREATE OR REPLACE VIEW subselect_view_any AS SELECT (SELECT 1) = ANY([5])");
        assertThat(
            stmt.analyzedQuery().outputs(), contains(
                isSQL("((SELECT 1 FROM (empty_row)) = ANY([5]))")
            )
        );
    }
}

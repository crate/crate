/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import io.crate.auth.user.User;
import io.crate.exceptions.InvalidRelationName;
import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static io.crate.testing.SymbolMatchers.isField;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

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

        assertThat(createView.name(), is(new RelationName(e.getSessionContext().defaultSchema(), "v1")));
        assertThat(createView.query(), isSQL("QueriedTable{DocTableRelation{doc.t1}}"));
        assertThat(createView.owner(), is(testUser));
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
    public void testAliasCanBeUsedToAvoidDuplicateColumnNamesInQuery() {
        CreateViewStmt createView = e.analyze("create view v1 as select x, x as y from t1");
        assertThat(createView.query().fields(), contains(isField("x"), isField("y")));
    }

    @Test
    public void testErrorIsThrownForUnsupportedQueries() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Query cannot be used in a VIEW");
        e.analyze("create view v1 as select t1.x, t1.x as y from t1, t1 as t2");
    }
}

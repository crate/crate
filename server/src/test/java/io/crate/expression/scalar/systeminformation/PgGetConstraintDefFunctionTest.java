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

package io.crate.expression.scalar.systeminformation;

import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.pgcatalog.OidHash;
import io.crate.metadata.table.ConstraintInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.role.Role;
import io.crate.role.metadata.RolesHelper;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;

public class PgGetConstraintDefFunctionTest extends ScalarTestCase {

    private static final RelationName TABLE = new RelationName("my_schema", "t");

    private Schemas schemas;
    private int pkOid;
    private int checkOid;

    @Before
    public void setUpSchemas() throws IOException {
        // The default ScalarTestCase wiring has no Schemas; build a real one so the
        // function can resolve constraint oids (same pattern as PgTableIsVisibleFunctionTest).
        schemas = SQLExecutor.of(clusterService)
            .addTable("create table my_schema.t (" +
                      "  id int," +
                      "  x int," +
                      "  constraint x_positive check (x > 0)," +
                      "  primary key (id)" +
                      ")")
            .schemas();
        sqlExpressions = new SqlExpressions(tableSources, null, Role.CRATE_USER, List.of(), schemas);
        TableInfo table = schemas.getTableInfo(TABLE);
        pkOid = OidHash.constraintOid(
            TABLE.fqn(), table.pkConstraintNameOrDefault(), ConstraintInfo.Type.PRIMARY_KEY.toString());
        checkOid = OidHash.constraintOid(
            TABLE.fqn(), "x_positive", ConstraintInfo.Type.CHECK.toString());
    }

    @Test
    public void test_returns_null_for_null_oid() {
        assertEvaluateNull("pg_get_constraintdef(null)");
    }

    @Test
    public void test_returns_null_for_unknown_oid() {
        assertEvaluateNull("pg_get_constraintdef(-1)");
    }

    @Test
    public void test_renders_primary_key_definition() {
        assertEvaluate("pg_get_constraintdef(" + pkOid + ")", "PRIMARY KEY (id)");
    }

    @Test
    public void test_renders_check_definition() {
        assertEvaluate("pg_get_constraintdef(" + checkOid + ")", "CHECK (\"x\" > 0)");
    }

    @Test
    public void test_pretty_flag_is_accepted_and_ignored() {
        assertEvaluate("pg_get_constraintdef(" + pkOid + ", true)", "PRIMARY KEY (id)");
        assertEvaluate("pg_get_constraintdef(" + pkOid + ", false)", "PRIMARY KEY (id)");
    }

    @Test
    public void test_null_pretty_flag_returns_null() {
        // strict function, like in PostgreSQL
        assertEvaluateNull("pg_get_constraintdef(" + pkOid + ", null)");
    }

    @Test
    public void test_works_with_fully_qualified_name() {
        assertEvaluate("pg_catalog.pg_get_constraintdef(" + pkOid + ")", "PRIMARY KEY (id)");
    }

    @Test
    public void test_returns_null_for_user_without_privileges_on_table() {
        // mirrors the row filter on pg_constraint: constraints of tables the user
        // cannot see must not be resolvable
        sqlExpressions = new SqlExpressions(
            tableSources, null, RolesHelper.userOf("dummy_user"), List.of(), schemas);
        assertEvaluateNull("pg_get_constraintdef(" + pkOid + ")");
        assertEvaluateNull("pg_get_constraintdef(" + checkOid + ")");
    }
}

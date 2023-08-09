/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.integrationtests;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertSQLError;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

public class AlterTableIntegrationTest extends IntegTestCase {

    @Test
    public void test_create_soft_delete_setting_for_partitioned_tables() {
        assertSQLError(() -> execute(
                "create table test(i int) partitioned by (i) WITH(\"soft_deletes.enabled\" = false) "))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Creating tables with soft-deletes disabled is no longer supported.");
    }

    @Test
    public void test_alter_table_drop_column_used_meanwhile_in_generated_col() {
        execute("CREATE TABLE t(a int, b int)");
        PlanForNode plan = plan("ALTER TABLE t DROP b");
        execute("ALTER TABLE t ADD COLUMN c GENERATED ALWAYS AS (b + 1)");
        assertSQLError(() -> execute(plan).getResult())
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(INTERNAL_SERVER_ERROR, 5000)
            .hasMessageContaining("Dropping column: b which is used to produce values for generated column is not allowed");
    }

    @Test
    public void test_alter_table_drop_column_dropped_meanwhile() {
        execute("CREATE TABLE t(a int, b int)");
        PlanForNode plan = plan("ALTER TABLE t DROP b");
        execute("ALTER TABLE t DROP COLUMN b");
        assertSQLError(() -> execute(plan).getResult())
            .hasMessageContaining("Column b unknown");
    }
}

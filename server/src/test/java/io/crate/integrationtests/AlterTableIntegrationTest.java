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
import static io.crate.testing.Asserts.assertThrowsMatches;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

import org.junit.Test;


public class AlterTableIntegrationTest extends SQLIntegrationTestCase {

    @Test
    public void test_alter_soft_delete_setting_for_partitioned_tables() {
        execute("create table test(i int) partitioned by (i)");
        assertThat(response.rowCount(), is(1L));
        execute("alter table test set (\"soft_deletes.enabled\" = false)");

        assertThrowsMatches(
            () -> execute("insert into test values(1)"),
            isSQLError(startsWith("Creating tables with soft-deletes disabled is no longer supported."),
                       INTERNAL_ERROR,
                       BAD_REQUEST,
                       4000)
        );

        execute("alter table test set (\"soft_deletes.enabled\" = true)");
        execute("insert into test values(1)");
        assertThat(response.rowCount(), is(1L));
    }

}

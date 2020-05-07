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

package io.crate.integrationtests;

import io.crate.testing.SQLResponse;
import io.crate.testing.UseJdbc;
import org.junit.Test;

import java.sql.PreparedStatement;

@UseJdbc(value = 1)
public class PostgresCompatIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testBeginStatement() {
        execute("BEGIN");
        assertNoErrorResponse(response);
    }

    @Test
    public void testBeginStatementWithParameters() {
        execute("BEGIN WORK");
        assertNoErrorResponse(response);
        execute("BEGIN TRANSACTION");
        assertNoErrorResponse(response);
    }

    @Test
    public void testBeginStatementWithTransactionMode() {
        String[] statements = new String[]{
            "BEGIN ISOLATION LEVEL SERIALIZABLE",
            "BEGIN ISOLATION LEVEL REPEATABLE READ",
            "BEGIN ISOLATION LEVEL READ COMMITTED",
            "BEGIN ISOLATION LEVEL READ UNCOMMITTED",
            "BEGIN READ WRITE",
            "BEGIN READ ONLY",
            "BEGIN DEFERRABLE",
            "BEGIN NOT DEFERRABLE"
        };
        for (String statement : statements) {
            execute(statement);
            assertNoErrorResponse(response);
        }
    }

    @Test
    public void testBeginStatementWithMultipleTransactionModes() {
        execute("BEGIN ISOLATION LEVEL SERIALIZABLE, " +
                "      READ WRITE, " +
                "      DEFERRABLE");
        assertNoErrorResponse(response);
    }

    @Test
    public void testBeginStatementWithParameterAndTransactionMode() {
        execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, " +
                "                  READ ONLY, " +
                "                  NOT DEFERRABLE");
        assertNoErrorResponse(response);
    }

    @Test
    public void testCommitStatement() {
        execute("COMMIT");
        assertNoErrorResponse(response);
    }

    /**
     * In CrateDB -1 means row-count unknown, and -2 means error.
     * In JDBC -2 means row-count unknown and -3 means error.
     * That's why we add +1 in the {@link io.crate.testing.SQLTransportExecutor#executeAndConvertResult(PreparedStatement)}
     */
    private static void assertNoErrorResponse(SQLResponse response) {
        assertTrue(response.rowCount() > -1);
    }
}

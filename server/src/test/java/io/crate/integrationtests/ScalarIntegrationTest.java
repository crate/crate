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

package io.crate.integrationtests;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.api.Assertions;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.exceptions.ColumnUnknownException;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
import io.crate.types.DataTypes;

@UseJdbc(0) // data types needed
public class ScalarIntegrationTest extends IntegTestCase {

    @Test
    public void testExtractFunctionReturnTypes() {
        execute("SELECT EXTRACT(DAY FROM CURRENT_TIMESTAMP)");
        assertThat(response.columnTypes()[0]).isEqualTo(DataTypes.INTEGER);

        execute("SELECT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)");
        assertThat(response.columnTypes()[0]).isEqualTo(DataTypes.DOUBLE);
    }

    @Test
    public void testSubscriptFunctionFromUnnest() {
        try (var session = sqlExecutor.newSession()) {
            session.sessionSettings().setErrorOnUnknownObjectKey(true);
            Assertions.assertThatThrownBy(() -> sqlExecutor.exec(
                        "SELECT unnest['x'] FROM UNNEST(['{\"x\":1,\"y\":2}','{\"y\":2,\"z\":3}']::ARRAY(OBJECT))",
                        session))
                .isExactlyInstanceOf(ColumnUnknownException.class)
                .hasMessageContaining("The object `{y=2, z=3}` does not contain the key `x`");
            // This is documenting a bug. If this fails, it is a breaking change.
            var response = sqlExecutor.exec("SELECT [unnest]['x'] FROM UNNEST(['{\"x\":1,\"y\":2}','{\"y\":2,\"z\":3}']::ARRAY(OBJECT))",
                            session);
            assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("[1]\n[null]\n");
        }

        try (var session2 = sqlExecutor.newSession()) {
            session2.sessionSettings().setErrorOnUnknownObjectKey(false);
            response = sqlExecutor.exec("SELECT unnest['x'] FROM UNNEST(['{\"x\":1,\"y\":2}','{\"y\":2,\"z\":3}']::ARRAY(OBJECT))",
                                    session2);
            assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("1\nNULL\n");
            response = sqlExecutor.exec("SELECT [unnest]['x'] FROM UNNEST(['{\"x\":1,\"y\":2}','{\"y\":2,\"z\":3}']::ARRAY(OBJECT))",
                                            session2);
            assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("[1]\n[null]\n");
        }
    }
}

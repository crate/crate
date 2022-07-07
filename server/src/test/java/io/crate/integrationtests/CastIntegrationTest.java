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

import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

public class CastIntegrationTest extends SQLIntegrationTestCase {

    @Test
    public void testTryCastValidLiteralCasting() {
        execute("select try_cast('2' as integer), try_cast(['1', '2'] as array(integer))," +
                " try_cast(null as integer)");
        assertThat(printedTable(response.rows()), is(
            """
                2| [1, 2]| NULL
                """
        ));
    }

    @Test
    public void testTryCastNotValidLiteralCasting() {
        execute("select try_cast('2e' as integer), try_cast('1' as boolean), try_cast(128 as byte)");
        assertThat(printedTable(response.rows()), is(
            """
                NULL| NULL| NULL
                """
        ));
    }

    @Test
    public void testTryCastReturnNullWhenCastingFailsOnRows() {
        execute("create table types (i integer, str string, arr array(long))");
        execute("insert into types (i, str, arr) values (?, ?, ?)", new Object[] {1, null, new Object[] {1, 2}});
        execute("insert into types (i, str, arr) values (?, ?, ?)", new Object[] {2, "3d", new Object[] {1, 128}});
        execute("refresh table types");
        execute("select try_cast(i as integer), try_cast(str as integer), try_cast(arr as array(byte))" +
                " from types order by i asc");
        assertThat(printedTable(response.rows()), is(
            """
                1| NULL| [1, 2]
                2| NULL| NULL
                """));

        execute("select try_cast(str as integer) from types");
        assertThat(printedTable(response.rows()), is(
            """
                NULL
                NULL
                """));
    }
}

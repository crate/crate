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


import io.crate.exceptions.ColumnUnknownException;
import io.crate.testing.Asserts;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
import io.crate.types.DataTypes;
import org.junit.Test;
import java.util.Objects;
import java.util.List;

import static org.hamcrest.Matchers.is;

@UseJdbc(0) // data types needed
public class ScalarIntegrationTest extends SQLIntegrationTestCase
{

    @Test
    public void testExtractFunctionReturnTypes()
    {
        execute("SELECT EXTRACT(DAY FROM CURRENT_TIMESTAMP)");
        assertThat(response.columnTypes()[0], is(DataTypes.INTEGER));

        execute("SELECT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)");
        assertThat(response.columnTypes()[0], is(DataTypes.DOUBLE));
    }


    @Test
    public void testSubscriptFunctionFromUnnest()
    {
        var session = sqlExecutor.newSession();
        session.sessionContext().setErrorOnUnknownObjectKey(true);
        Asserts.assertThrowsMatches(
            () -> {
                sqlExecutor.exec("SELECT col1['x'] FROM UNNEST(['{\"x\":1,\"y\":2}','{\"y\":2,\"z\":3}']::ARRAY(OBJECT))",
                                 session);
                },
            ColumnUnknownException.class,
            "The object `{y=2, z=3}` does not contain the key `x`"
        );
        // This is documenting a bug. If this fails, it is a breaking change.
        var response = sqlExecutor.exec("SELECT [col1]['x'] FROM UNNEST(['{\"x\":1,\"y\":2}','{\"y\":2,\"z\":3}']::ARRAY(OBJECT))",
                         session);
        assertThat(TestingHelpers.printedTable(response.rows()), is("[1]\n[null]\n"));

        var session2 = sqlExecutor.newSession();
        session2.sessionContext().setErrorOnUnknownObjectKey(false);
        response = sqlExecutor.exec("SELECT col1['x'] FROM UNNEST(['{\"x\":1,\"y\":2}','{\"y\":2,\"z\":3}']::ARRAY(OBJECT))",
                                 session2);
        assertThat(TestingHelpers.printedTable(response.rows()), is("1\nNULL\n"));
        response = sqlExecutor.exec("SELECT [col1]['x'] FROM UNNEST(['{\"x\":1,\"y\":2}','{\"y\":2,\"z\":3}']::ARRAY(OBJECT))",
                                        session2);
        assertThat(TestingHelpers.printedTable(response.rows()), is("[1]\n[null]\n"));
    }


    @Test
    public void testAgeFunctionWithOrderBy()
    {

        Object[][] ar = {
            {"2021-10-01, 00:00"}, {"2021-10-02, 00:00"},
            {"2021-10-03, 00:00"}, {"2021-10-04, 00:00"},
            {"2021-10-05, 00:00"}, {"2021-10-06, 00:00"},
            {"2021-10-07, 00:00"}, {"2021-10-08, 00:00"},
            {"2021-10-09, 00:00"}, {"2021-10-10, 00:00"},
            {"2021-10-11, 00:00"}, {"2021-10-12, 00:00"},
            {"2021-10-13, 00:00"}, {"2021-10-14, 00:00"},
            {"2021-10-15, 00:00"}, {"2021-10-16, 00:00"},
            {"2021-10-17, 00:00"}, {"2021-10-18, 00:00"},
            {"2021-10-19, 00:00"}, {"2021-10-20, 00:00"},
            {"2021-10-21, 00:00"}, {"2021-10-22, 00:00"},
            {"2021-10-23, 00:00"}, {"2021-10-24, 00:00"},
            {"2021-10-25, 00:00"}, {"2021-10-26, 00:00"},
            {"2021-10-27, 00:00"}, {"2021-10-28, 00:00"},
            {"2021-10-29, 00:00"}, {"2021-10-30, 00:00"},
            {"2021-10-31, 00:00"}, {"2021-11-01, 00:00"},
            {"2021-11-02, 00:00"}

        };
        execute(
            "SELECT   date_format('%Y-%m-%d, %H:%i', x)  FROM generate_series('2021-10-01'::timestamp, '2021-11-02'::timestamp, '1 days'::interval) AS t(x) order by pg_catalog.age(x) desc");


        assertThat(Objects.deepEquals(response.rows(), ar), is(true));



    }
}

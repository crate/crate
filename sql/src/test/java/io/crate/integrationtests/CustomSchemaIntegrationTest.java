/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.test.integration.CrateIntegrationTest;
import io.crate.testing.TestingHelpers;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class CustomSchemaIntegrationTest extends SQLTransportIntegrationTest {


    @Test
    public void testInformationSchemaTablesReturnCorrectTablesIfCustomSchemaIsSimilarToTableName() throws Exception {
        // regression test.. this caused foobar to be detected as a table in the foo schema and caused a NPE
        execute("create table foobar (id int primary key) with (number_of_replicas = 0)");
        execute("create table foo.bar (id int primary key) with (number_of_replicas = 0)");

        execute("select schema_name, table_name from information_schema.tables " +
                "where table_name like 'foo%' or schema_name = 'foo' order by table_name");
        assertThat(TestingHelpers.printedTable(response.rows()), is("" +
                "foo| bar\n" +
                "doc| foobar\n"));
    }
}

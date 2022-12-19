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

import static io.crate.testing.TestingHelpers.printedTable;
import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

public class UppercaseSchemaNameIntegrationTest extends IntegTestCase {

    @Test
    public void test_upper_case_schema_name() {
        execute("create table \"Abc\".t (a int) partitioned by (a)");
        execute("insert into \"Abc\".t values (1), (2)");
        execute("create table Abc.t (b boolean) partitioned by (b)");
        execute("insert into Abc.t values (true), (false)");

        execute("create view v1 as select a from \"Abc\".t");
        execute("create view v2 as select b from Abc.t");

        refresh();

        execute("select * from \"Abc\".t order by a");
        assertThat(printedTable(response.rows())).isEqualTo("1\n2\n");
        execute("select * from Abc.t order by b");
        assertThat(printedTable(response.rows())).isEqualTo("false\ntrue\n");

        // views
        execute("select a from v1 order by a");
        assertThat(printedTable(response.rows())).isEqualTo("1\n2\n");
        execute("select b from v2 order by b");
        assertThat(printedTable(response.rows())).isEqualTo("false\ntrue\n");

        // udfs

    }
}

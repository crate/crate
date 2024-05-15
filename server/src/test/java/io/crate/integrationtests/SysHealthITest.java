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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

@IntegTestCase.ClusterScope(numDataNodes = 2)
public class SysHealthITest extends IntegTestCase {

    @Test
    public void testTablesHealth() throws IOException {
        execute("create table doc.t1 (id int) with(number_of_replicas=0)");
        // stopping 1 node so t1 is red (missing primaries)
        cluster().stopRandomDataNode();
        // yellow cause missing replicas
        execute("create table doc.t2 (id int) with(number_of_replicas=1, \"write.wait_for_active_shards\"=1)");
        // green, all fine
        execute("create table doc.t3 (id int) with(number_of_replicas=0)");

        execute("select * from sys.health order by severity desc");
        assertThat(printedTable(response.rows())).isEqualTo("RED| 2| NULL| 3| t1| doc| 0\n" +
                                                     "YELLOW| 0| NULL| 2| t2| doc| 4\n" +
                                                     "GREEN| 0| NULL| 1| t3| doc| 0\n");
    }

    @Test
    public void test_health_of_partitioned_table_with_different_number_of_shards_per_partition() {
        execute("create table doc.p1 (id int, p int)" +
                " clustered into 2 shards" +
                " partitioned by (p) " +
                " with (number_of_replicas=0)"
        );
        execute("insert into doc.p1 (id, p) values (1, 1)");
        execute("alter table doc.p1 set (number_of_shards = 4)");
        execute("insert into doc.p1 (id, p) values (2, 2)");
        refresh();

        execute("select * from sys.health order by severity, partition_ident desc");
        assertThat(printedTable(response.rows())).isEqualTo(
            "GREEN| 0| 04134| 1| p1| doc| 0\n" +
            "GREEN| 0| 04132| 1| p1| doc| 0\n");
    }
}

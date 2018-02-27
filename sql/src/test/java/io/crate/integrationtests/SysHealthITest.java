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

import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2)
public class SysHealthITest extends SQLTransportIntegrationTest {

    @Test
    public void testTablesHealth() throws IOException {
        execute("create table doc.t1 (id int) with(number_of_replicas=0)");
        // stopping 1 node so t1 is red (missing primaries)
        internalCluster().stopRandomDataNode();
        // yellow cause missing replicas
        execute("create table doc.t2 (id int) with(number_of_replicas=1, \"write.wait_for_active_shards\"=1)");
        // green, all fine
        execute("create table doc.t3 (id int) with(number_of_replicas=0)");

        execute("select * from sys.health order by severity desc");
        assertThat(printedTable(response.rows()), is("RED| 2| | 3| t1| doc| 0\n" +
                                                     "YELLOW| 0| | 2| t2| doc| 4\n" +
                                                     "GREEN| 0| | 1| t3| doc| 0\n"));
    }
}

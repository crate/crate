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

import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 1)
@UseJdbc
public class UnassignedShardsTest extends SQLTransportIntegrationTest {

    @Test
    public void testUnassignedReplicasAreVisibleAsUnassignedInSysShards() throws Exception {
        execute("create table t (id int) clustered into 1 shards with (number_of_replicas=1)");
        execute("select state, id, table_name from sys.shards where schema_name='doc' AND table_name='t' order by state");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("STARTED| 0| t\n" +
               "UNASSIGNED| 0| t\n"));
    }
}

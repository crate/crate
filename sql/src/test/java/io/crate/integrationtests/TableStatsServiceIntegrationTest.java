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

import io.crate.metadata.TableIdent;
import io.crate.planner.TableStats;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;


@ESIntegTestCase.ClusterScope(supportsDedicatedMasters = false, numDataNodes = 2, numClientNodes = 0)
public class TableStatsServiceIntegrationTest extends SQLTransportIntegrationTest {

    @Before
    public void setRefreshInterval() {
        execute("SET GLOBAL TRANSIENT \"stats.service.interval\" = '50ms'");
    }

    @After
    public void setResetInterval() {
        execute("RESET GLOBAL \"stats.service.interval\"");
    }

    @Test
    public void testStatsUpdated() throws Exception {
        execute("CREATE TABLE t1 (a INTEGER) WITH (number_of_replicas = 1)");
        ensureGreen();
        execute("INSERT INTO t1 (a) VALUES (1), (2), (3), (4), (5)");
        execute("REFRESH TABLE t1");
        assertBusy(() -> {
                TableStats tableStats = internalCluster().getDataNodeInstance(TableStats.class);
                assertThat(tableStats.numDocs(new TableIdent(sqlExecutor.getDefaultSchema(), "t1")), is(5L));
                // tableStats.tableStats.estimatedSizePerRow() is not tested because it's based on sys.shards size
                // column which is is cached for 10 secs in ShardSizeExpression which will increase the time needed
                // to run this test.
            }, 5, TimeUnit.SECONDS);
    }
}

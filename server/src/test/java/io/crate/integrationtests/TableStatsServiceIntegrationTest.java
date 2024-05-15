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

import java.util.concurrent.TimeUnit;

import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.metadata.RelationName;
import io.crate.statistics.TableStats;


@IntegTestCase.ClusterScope(supportsDedicatedMasters = false, numDataNodes = 2, numClientNodes = 0)
public class TableStatsServiceIntegrationTest extends IntegTestCase {

    @Before
    public void setRefreshInterval() {
        execute("set global transient stats.service.interval='50ms'");
    }

    @After
    public void setResetInterval() {
        execute("reset global stats.service.interval");
    }

    @Test
    public void testStatsUpdated() throws Exception {
        execute("create table t1(a int) with (number_of_replicas = 1)");
        ensureGreen();
        execute("insert into t1(a) values(1), (2), (3), (4), (5)");
        execute("refresh table t1");
        assertBusy(() -> {
            TableStats tableStats = cluster().getDataNodeInstance(TableStats.class);
            assertThat(tableStats.numDocs(new RelationName(sqlExecutor.getCurrentSchema(), "t1"))).isEqualTo(5L);
            // tableStats.tableStats.estimatedSizePerRow() is not tested because it's based on sys.shards size
            // column which is is cached for 10 secs in ShardSizeExpression which will increase the time needed
            // to run this test.
        }, 5, TimeUnit.SECONDS);
    }
}

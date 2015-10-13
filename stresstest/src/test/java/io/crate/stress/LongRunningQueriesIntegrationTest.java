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

package io.crate.stress;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import io.crate.action.sql.SQLBulkResponse;
import io.crate.action.sql.SQLResponse;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

@TimeoutSuite(millis = 30 * 20 * TimeUnits.MINUTE)
@TestLogging("io.crate.jobs.JobExecutionContext:TRACE,io.crate.jobs.KeepAliveTimers:TRACE")
@ElasticsearchIntegrationTest.ClusterScope (numDataNodes = 2)
public class LongRunningQueriesIntegrationTest extends SQLTransportIntegrationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final int ROWS = 500_000;
    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(2_500);

    @Override
    public SQLResponse execute(String stmt) {
        return super.execute(stmt, TIMEOUT);
    }

    @Override
    public SQLResponse execute(String stmt, Object[] args) {
        return super.execute(stmt, args, TIMEOUT);
    }

    @Override
    public SQLBulkResponse execute(String stmt, Object[][] bulkArgs) {
        return super.execute(stmt, bulkArgs, TIMEOUT);
    }

    @Before
    public void prepare() {

        execute("create table longt (id int, duration long) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        Object[][] args = new Object[ROWS][];
        for (int i = 0; i < ROWS; i++) {
            args[i] = new Object[]{i, 1};
        }
        execute("insert into longt (id, duration) values (?, ?)", args);
        execute("refresh table longt");
    }

    @Test
    public void testLongQueryThenFetchContextIsNotReapedWhileInCollector() throws Exception {
        // where clause is evaluated in LuceneDocCollector
        execute("select duration from longt where sleep(duration) = true limit " + ROWS);
    }

    @Test
    public void testLongQueryThenFetchContextIsNotReapedWhileInCollectorWithoutMatches() throws Exception {
        // where clause is evaluated in LuceneDocCollector
        execute("select duration from longt where sleep(duration) = false limit " + ROWS);
    }

    @Test
    public void testLongQTFContextIsNotReapedWhileSortingInCollector() throws Exception {
        // order by is evaluated in OrderedLuceneDocCollector
        execute("select duration from longt order by sleep(duration) limit " + ROWS);
    }

    @Test
    public void testLongQueryThenFetchContextIsNotReapedWhileInFetchProjector() throws Exception {
        // duration will be fetched in FetchProjector and sleep will be executed
        // there (localMerge)
        execute("select sleep(duration) from longt limit " + ROWS);
    }
}

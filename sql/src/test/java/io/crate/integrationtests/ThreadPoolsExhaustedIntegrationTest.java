/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

import io.crate.testing.SQLBulkResponse;
import io.crate.testing.SQLResponse;
import io.crate.testing.SQLTransportExecutor;
import io.crate.testing.UseJdbc;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(maxNumDataNodes = 2)
@UseJdbc
public class ThreadPoolsExhaustedIntegrationTest extends SQLTransportIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("thread_pool.search.size", 2)
            .put("thread_pool.search.queue_size", 2)
            .build();
    }

    @Test
    public void testRegularSelectWithFewAvailableThreadsShouldNeverGetStuck() throws Exception {
        execute("create table t (x int) with (number_of_replicas = 0)");
        ensureYellow();
        bulkInsert(10);

        List<ActionFuture<SQLResponse>> futures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            ActionFuture<SQLResponse> future = sqlExecutor.execute(
                "select * from t limit ?", new Object[]{10});
            futures.add(future);
        }

        for (ActionFuture<SQLResponse> future : futures) {
            try {
                future.get(SQLTransportExecutor.REQUEST_TIMEOUT.getMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                fail("query run into a timeout");
            } catch (Exception e) {
                assertThat(e.getMessage(), Matchers.containsString("rejected execution"));
            }
        }
    }

    private void bulkInsert(int docCount) {
        Object[][] bulkArgs = new Object[docCount][1];
        for (int i = 0; i < docCount; i++) {
            bulkArgs[i][0] = i;
        }
        SQLBulkResponse response = execute("insert into t (x) values (?)", bulkArgs);
        for (SQLBulkResponse.Result result : response.results()) {
            assertThat(result.rowCount(), is(1L));
        }
    }
}

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

import io.crate.action.sql.SQLAction;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import io.crate.concurrent.Threaded;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ConcurrentUpdateStressTest extends AbstractIntegrationStressTest {

    private String[] values;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings settings = super.nodeSettings(nodeOrdinal);
        return ImmutableSettings.builder().put(settings).put("threadpool.search.queue_size", 3_000).build();
    }

    @Override
    public void prepareFirst() {
        execute("create table rejected (id long primary key, value string, category int) with (number_of_replicas=0)");
        ensureYellow();
        int numArgs = 10_000;
        Object[][] args = new Object[numArgs][];
        values = new String[5];
        for (int i = 0; i < numArgs; i++) {
            String value = randomAsciiOfLength(10);
            args[i] = new Object[] { i, value , i%20 };
            values[i%5] = value;
        }
        execute("insert into rejected (id, value, category) values (?, ?, ?)", args);
        execute("refresh table rejected");
    }

    @Threaded(count = 20)
    @Test
    public void testProvokeRejectedExecution() throws Exception {
        // test that retry logic works for concurrent update by query
        int numRequests = 50;
        final List<ActionFuture<SQLResponse>> futures = new ArrayList<>(numRequests);
        final CountDownLatch latch = new CountDownLatch(numRequests);
        for (int i = 0; i < numRequests; i++) {
            String value = values[randomIntBetween(0, 4)];
            futures.add(
                    client().execute(SQLAction.INSTANCE, new SQLRequest("update rejected set value=? where category = ?", new Object[]{value + "U", (randomIntBetween(0, 19))}))
            );
            latch.countDown();
        }
        latch.await();
        for (ActionFuture<SQLResponse> actionFuture : futures) {
            actionFuture.actionGet();
        }
    }
}

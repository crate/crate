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

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import io.crate.action.sql.SQLResponse;
import io.crate.metadata.PartitionName;
import io.crate.testing.SQLTransportExecutor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.MetaData;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@TimeoutSuite(millis = 0)
public class PartitionedTableConcurrentIntegrationTest extends SQLTransportIntegrationTest {

    static Object[][] BULK_ARGS;

    @BeforeClass
    public static void setUpData() {
        int numberOfDocs = 1000;
        BULK_ARGS = new Object[numberOfDocs][];
        for (int i = 0; i < numberOfDocs; i++) {
            BULK_ARGS[i] = new Object[]{i % 2, randomAsciiOfLength(10)};
        }
    }

    private void deletePartitionWhileInsertingData(final boolean useBulk) throws Exception {
        execute("create table parted (id int, name string) " +
                "partitioned by (id) " +
                "with (number_of_replicas = 0)");
        ensureYellow();

        // partition to delete
        final int idToDelete = 1;

        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        final CountDownLatch insertLatch = new CountDownLatch(1);
        final String insertStmt = "insert into parted (id, name) values (?, ?)";
        Thread insertThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    if (useBulk) {
                        execute(insertStmt, BULK_ARGS);
                    } else {
                        for (Object[] args : BULK_ARGS) {
                            execute(insertStmt, args);
                        }
                    }
                } catch (Exception t) {
                    exceptionRef.set(t);
                } finally {
                    insertLatch.countDown();
                }
            }
        });

        final CountDownLatch deleteLatch = new CountDownLatch(1);
        final String partitionName = new PartitionName("parted",
                Collections.singletonList(new BytesRef(String.valueOf(idToDelete)))
        ).asIndexName();
        final Object[] deleteArgs = new Object[]{idToDelete};
        Thread deleteThread = new Thread(new Runnable() {
            @Override
            public void run() {
                boolean deleted = false;
                while (!deleted) {
                    try {
                        MetaData metaData = client().admin().cluster().prepareState().execute().actionGet()
                                .getState().metaData();
                        if (metaData.indices().get(partitionName) != null) {
                            SQLResponse response = execute("delete from parted where id = ?", deleteArgs);
                            if (response.rowCount() != 0) {
                                deleted = true;
                            }
                        }
                    } catch (Throwable t) {
                        // ignore
                    }
                }
                deleteLatch.countDown();
            }
        });

        insertThread.start();
        deleteThread.start();
        deleteLatch.await(SQLTransportExecutor.REQUEST_TIMEOUT.getSeconds() + 1, TimeUnit.SECONDS);
        insertLatch.await(SQLTransportExecutor.REQUEST_TIMEOUT.getSeconds() + 1, TimeUnit.SECONDS);

        Exception exception = exceptionRef.get();
        if (exception != null) {
            throw exception;
        }

        insertThread.join();
        deleteThread.join();
    }

    @Test
    public void testDeletePartitionWhileInsertingData() throws Exception {
        deletePartitionWhileInsertingData(false);
    }

    @Test
    public void testDeletePartitionWhileBulkInsertingData() throws Exception {
        deletePartitionWhileInsertingData(true);
    }
}
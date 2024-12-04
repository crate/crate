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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.testing.SQLResponse;

public class InformationSchemaQueryTest extends IntegTestCase {

    @Test
    public void testDoNotIgnoreClosedTables() throws Exception {
        execute("create table t3 (col1 integer, col2 string) " +
                "clustered into 5 shards");
        execute("create table t1 (col1 integer, col2 string) " +
                "clustered into 5 shards " +
                "with (number_of_replicas=0)");

        ensureYellow();
        execute("alter table t3 close");

        execute("select * from information_schema.tables where table_schema = ?", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(2L);
        execute("select * from information_schema.columns where table_name = 't3'");
        assertThat(response.rowCount()).isEqualTo(2);
    }

    @Test
    public void testConcurrentInformationSchemaQueries() throws Exception {
        final SQLResponse response = execute("select * from information_schema.columns " +
                                             "order by table_schema, table_name, column_name");
        final CountDownLatch latch = new CountDownLatch(40);
        final AtomicReference<AssertionError> lastAssertionError = new AtomicReference<>();

        for (int i = 0; i < 40; i++) {
            final Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    SQLResponse resp = execute("select * from information_schema.columns " +
                                               "order by table_schema, table_name, column_name");
                    try {
                        assertThat(resp.rows()).isEqualTo(response.rows());
                    } catch (AssertionError e) {
                        lastAssertionError.set(e);
                    }
                    latch.countDown();
                }
            });
            t.start();
        }

        latch.await();
        AssertionError assertionError = lastAssertionError.get();
        if (assertionError != null) {
            throw assertionError;
        }
    }

    @Test
    public void testConcurrentUnassignedShardsReferenceResolver() throws Exception {
        cluster().ensureAtMostNumDataNodes(1);
        execute("create table t1 (col1 integer) " +
                "clustered into 1 shards ");
        execute("create table t2 (col1 integer) " +
                "clustered into 1 shards ");
        ensureYellow();

        final SQLResponse response = execute("select * from sys.shards where table_name in ('t1', 't2') and state='UNASSIGNED' order by schema_name, table_name, id");
        final CountDownLatch latch = new CountDownLatch(40);
        final AtomicReference<AssertionError> lastAssertionError = new AtomicReference<>();
        for (int i = 0; i < 40; i++) {
            final Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    SQLResponse resp = execute("select * from sys.shards where table_name in ('t1', 't2') and state='UNASSIGNED' order by schema_name, table_name, id");
                    try {
                        assertThat(resp.rows()).isEqualTo(response.rows());
                    } catch (AssertionError e) {
                        lastAssertionError.set(e);
                    }
                    latch.countDown();
                }
            });
            t.start();
        }
        latch.await();
        AssertionError assertionError = lastAssertionError.get();
        if (assertionError != null) {
            throw assertionError;
        }
    }
}

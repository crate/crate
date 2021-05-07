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

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CreateTableIntegrationTest extends SQLIntegrationTestCase {

    @Test
    public void testCreateTableIfNotExistsConcurrently() throws Throwable {
        executeCreateTableThreaded("create table if not exists t (name string) with (number_of_replicas = 0)");
    }

    @Test
    public void testCreatePartitionedTableIfNotExistsConcurrently() throws Throwable {
        executeCreateTableThreaded("create table if not exists t " +
                                   "(name string, p string) partitioned by (p) " +
                                   "with (number_of_replicas = 0)");
    }

    @Test
    public void test_allow_drop_of_corrupted_table() throws Exception {
        execute("create table doc.tbl (ts timestamp without time zone as '2020-01-01')");
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("default")
                .startObject("_meta")
                    .startObject("generated_columns")
                        .field("ts", "foobar")
                    .endObject()
                .endObject()
            .endObject()
            .endObject();
        client().admin().indices().preparePutMapping("tbl")
            .setSource(builder)
            .execute()
            .actionGet(5, TimeUnit.SECONDS);

        assertThat(
            internalCluster().getInstance(ClusterService.class).state().metadata().hasIndex("tbl"),
            is(true)
        );
        assertThrows(Exception.class, () -> execute("select * from doc.tbl"));
        execute("drop table doc.tbl");
        execute("select count(*) from information_schema.tables where table_name = 'tbl'");
        assertThat(response.rows()[0][0], is(0L));
        assertThat(
            internalCluster().getInstance(ClusterService.class).state().metadata().hasIndex("tbl"),
            is(false)
        );
    }

    private void executeCreateTableThreaded(final String statement) throws Throwable {
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        final AtomicReference<Throwable> lastThrowable = new AtomicReference<>();

        for (int i = 0; i < 20; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        execute(statement);
                    } catch (Throwable t) {
                        lastThrowable.set(t);
                    }
                }
            });
        }

        executorService.shutdown();
        assertThat("executorservice did not shutdown within timeout", executorService.awaitTermination(10, TimeUnit.SECONDS), is(true));

        Throwable throwable = lastThrowable.get();
        if (throwable != null) {
            throw throwable;
        }
    }
}

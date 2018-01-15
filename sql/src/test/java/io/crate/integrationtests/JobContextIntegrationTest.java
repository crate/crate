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

import io.crate.execution.jobs.JobContextService;
import io.crate.execution.jobs.JobExecutionContext;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;

public class JobContextIntegrationTest extends SQLTransportIntegrationTest {

    Setup setup = new Setup(sqlExecutor);

    @Test
    public void testAllContextAreClosed() throws Exception {
        // lets create some contexts which must be closed after statement execution

        // group-by query (job collect context with sub-contexts + PageDownstreamContext are created)
        setup.groupBySetup();
        execute("select age, name from characters group by 1, 2");

        // system table query (job collect context without sub-contexts is created)
        execute("select random(), random() from sys.cluster limit 1");

        // information_schema table query (job collect context without sub-contexts is created)
        execute("select table_name from information_schema.tables");

        // multiple upserts (SymbolBasedBulkShardProcessorContext is created)
        execute("create table upserts (id int primary key, d long)");
        ensureYellow();
        execute("insert into upserts (id, d) values (?, ?)", new Object[][]{
            new Object[]{1, -7L},
            new Object[]{2, 3L},
        });
        refresh();

        // upsert-by-id (UpsertByIdContext is created)
        execute("update upserts set d = 5 where id = 1");

        // get by id (ESJobContext is created)
        execute("select * from upserts where id = 1");

        // count (CountContext is created)
        execute("select count(*) from upserts");


        // now check if all contexts are gone
        final Field activeContexts = JobContextService.class.getDeclaredField("activeContexts");
        activeContexts.setAccessible(true);

        assertBusy(() -> {
                for (JobContextService jobContextService : internalCluster().getInstances(JobContextService.class)) {
                    Map<UUID, JobExecutionContext> contextMap = null;
                    try {
                        contextMap = (Map<UUID, JobExecutionContext>) activeContexts.get(jobContextService);
                        assertThat(contextMap.size(), is(0));
                    } catch (Exception e) {
                        fail(e.getMessage());
                    }
                }
        }, 1, TimeUnit.SECONDS);
    }
}

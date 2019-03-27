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

import com.google.common.util.concurrent.SettableFuture;
import io.crate.action.sql.SQLActionException;
import io.crate.exceptions.SQLExceptions;
import io.crate.testing.SQLResponse;
import io.crate.testing.plugin.CrateTestingPlugin;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class KillIntegrationTest extends SQLTransportIntegrationTest {

    private Setup setup = new Setup(sqlExecutor);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(CrateTestingPlugin.class);
        return plugins;
    }

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testKillInsertFromSubQuery() throws Exception {
        setup.setUpEmployees();
        execute("create table new_employees (" +
                " name string, " +
                " department string," +
                " hired timestamp with time zone, " +
                " age short," +
                " income double, " +
                " good boolean" +
                ") with (number_of_replicas=0)");
        ensureYellow();
        assertGotCancelled("insert into new_employees (select * from employees)", null, true);
        // There could still be running upsert requests after the kill happened, so wait for them
        waitUntilShardOperationsFinished();
    }

    private void assertGotCancelled(final String statement, @Nullable final Object[] params, boolean killAll) throws Exception {
        try {
            ActionFuture<SQLResponse> future = sqlExecutor.execute(statement, params);
            String jobId = waitForJobEntry(statement);
            if (jobId == null) {
                // query finished too fast
                return;
            }
            if (killAll) {
                execute("kill all");
            } else {
                execute("kill ?", $(jobId));
            }
            try {
                future.get(10, TimeUnit.SECONDS);
            } catch (Throwable exception) {
                exception = SQLExceptions.unwrap(exception); // wrapped in ExecutionException
                assertThat(exception, instanceOf(SQLActionException.class));
                assertThat(exception.toString(), anyOf(
                    containsString("Job killed"), // CancellationException
                    containsString("RootTask for job"), // TaskMissing when root task not found
                    containsString("Task for job") // TaskMissing when task not found
                ));
            }
        } finally {
            waitUntilThreadPoolTasksFinished(ThreadPool.Names.SEARCH);
        }
    }

    /**
     * Wait for a statement to appear in sys.jobs.
     * If the query finished execution (is in sys.jobs_log) it will return null
     */
    @Nullable
    private String waitForJobEntry(final String statement) throws Exception {
        final SettableFuture<String> jobIdFuture = SettableFuture.create();
        assertBusy(() -> {
            SQLResponse logResponse = execute("select * from sys.jobs where stmt = ?", $(statement));
            if (logResponse.rowCount() == 0) {
                logResponse = execute("select * from sys.jobs_log where stmt = ?", $(statement));
                if (logResponse.rowCount() > 0L) {
                    // query finished before jobId could be retrieved
                    // finishing without killing - test will pass which is okay because it is not deterministic by design
                    jobIdFuture.set(null);
                    return;
                }
            }
            assertThat(logResponse.rowCount(), greaterThan(0L));
            String jobId = logResponse.rows()[0][0].toString();
            jobIdFuture.set(jobId);
        });
        return jobIdFuture.get(10, TimeUnit.SECONDS);
    }

    @Test
    public void testKillUpdateByQuery() throws Exception {
        setup.setUpEmployees();
        assertGotCancelled("update employees set income=income+100 where department='management'", null, true);
    }

    @Test
    public void testKillCopyTo() throws Exception {
        String path = Paths.get(temporaryFolder.newFolder().toURI()).toUri().toString();
        setup.setUpEmployees();
        assertGotCancelled("copy employees to directory ?", new Object[]{path}, true);
    }

    @Test
    public void testKillGroupBy() throws Exception {
        setup.setUpEmployees();
        assertGotCancelled("SELECT sleep(500), sum(income) as summed_income, count(distinct name), department " +
                           "from employees " +
                           "group by 1, department " +
                           "having avg(income) > 100 " +
                           "order by department desc nulls first " +
                           "limit 10", null, true);
    }

    @Test
    public void testKillSelectSysTable() throws Exception {
        assertGotCancelled("SELECT sleep(500) FROM sys.nodes", null, true);
    }

    @Test
    public void testKillSelectDocTable() throws Exception {
        setup.setUpEmployees();
        assertGotCancelled("SELECT name, department, sleep(500) FROM employees ORDER BY name", null, true);
    }

    @Test
    public void testKillSelectSysTableJobById() throws Exception {
        assertGotCancelled("SELECT sleep(500) FROM sys.nodes", null, false);
    }

    @Test
    public void testKillNonExisitingJob() throws Exception {
        UUID jobId = UUID.randomUUID();
        SQLResponse killResponse = execute("KILL ?", new Object[]{jobId.toString()});
        assertThat(killResponse.rowCount(), is(0L));
        SQLResponse logResponse = execute("select * from sys.jobs_log where error = ?", new Object[]{"KILLED"});
        assertThat(logResponse.rowCount(), is(0L));
    }

}

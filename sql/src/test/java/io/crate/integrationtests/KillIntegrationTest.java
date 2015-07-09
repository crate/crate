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

import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLResponse;
import io.crate.exceptions.Exceptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.StringContains.containsString;

public class KillIntegrationTest extends SQLTransportIntegrationTest {

    private Setup setup = new Setup(sqlExecutor);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setSetup() {
        execute("SET GLOBAL stats.enabled = true");
    }

    @Test
    public void testKillInsertFromSubQuery() throws Exception {
        setup.setUpEmployees();
        execute("create table new_employees (" +
                " name string, " +
                " department string," +
                " hired timestamp, " +
                " age short," +
                " income double, " +
                " good boolean" +
                ") with (number_of_replicas=1)");
        ensureYellow();
        assertGotCancelled("insert into new_employees (select * from employees)", null);

        // if the insert is killed there will still be pending insert operations,
        // the refresh seems to block long enough that those operations can be finished,
        // otherwise the test would be flaky.
        refresh();
    }

    private void assertGotCancelled(final String statement, @Nullable final Object[] params) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        final AtomicReference<Throwable> thrown = new AtomicReference<>();
        final CountDownLatch happened = new CountDownLatch(1);
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    while (thrown.get() == null) {
                        try {
                            execute(statement, params);
                        } catch (Throwable e) {
                            Throwable unwrapped = Exceptions.unwrap(e);
                            thrown.compareAndSet(null, unwrapped);
                        } finally {
                            happened.countDown();
                        }
                    }
                }
            });
            happened.await();
            SQLResponse logResponse = execute("select * from sys.jobs_log where stmt = ?",
                    new Object[]{statement});
            String jobId = logResponse.rows()[0][0].toString();
            SQLResponse response = execute(jobId == null ? "kill all" : String.format("kill '%s'", jobId));
            executor.shutdown();
            executor.awaitTermination(2, TimeUnit.SECONDS);
            // if killed, then cancellationException, nothing else
            Throwable exception = thrown.get();
            if (exception != null) {
                assertThat(response.rows()[0][0].toString(), containsString(jobId));
                assertThat(exception, instanceOf(SQLActionException.class));
                assertThat(((SQLActionException)exception).stackTrace(), containsString("Job killed by user"));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testKillUpdateByQuery() throws Exception {
        setup.setUpEmployees();
        assertGotCancelled("update employees set income=income+100 where department='management'", null);
    }

    @Test
    public void testKillCopyTo() throws Exception {
        String path = temporaryFolder.newFolder().getAbsolutePath();
        setup.setUpEmployees();
        assertGotCancelled("copy employees to directory ?", new Object[]{path});
    }

    @Test
    public void testKillGroupBy() throws Exception {
        setup.setUpEmployees();
        assertGotCancelled("SELECT sum(income) as summed_income, count(distinct name), department " +
                "from employees " +
                "group by department " +
                "having avg(income) > 100 " +
                "order by department desc nulls first " +
                "limit 10", null);
        runJobContextReapers();
    }

    @Test
    public void testKillCopyToJob() throws Exception {
        String path = temporaryFolder.newFolder().getAbsolutePath();
        setup.setUpEmployees();
        assertGotCancelled("copy employees to directory ?", new Object[]{path});
    }

    @Test
    public void testKillInsetJob() throws Exception {
        setup.setUpEmployees();
        execute("create table new_employees (" +
                " name string, " +
                " department string," +
                " hired timestamp, " +
                " age short," +
                " income double, " +
                " good boolean" +
                ") with (number_of_replicas=1)");
        ensureYellow();
        assertGotCancelled("insert into new_employees (select * from employees)", null);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        execute("reset GLOBAL stats.enabled");
    }

}

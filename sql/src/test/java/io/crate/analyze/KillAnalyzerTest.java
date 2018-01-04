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

package io.crate.analyze;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import java.util.Locale;
import java.util.UUID;

import static org.hamcrest.core.Is.is;

public class KillAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService).build();
    }

    @Test
    public void testAnalyzeKillAll() throws Exception {
        KillAnalyzedStatement stmt = e.analyze("kill all");
        assertNull(stmt.jobId());
    }

    @Test
    public void testAnalyzeKillJobWithParameter() throws Exception {
        UUID jobId = UUID.randomUUID();
        KillAnalyzedStatement stmt = e.analyze("kill $2", new Object[]{2, jobId});
        assertThat(stmt.jobId(), is(jobId));
        stmt = e.analyze("kill $1", new Object[]{jobId});
        assertThat(stmt.jobId(), is(jobId));
        stmt = e.analyze("kill ?", new Object[]{jobId});
        assertThat(stmt.jobId(), is(jobId));
    }

    @Test
    public void testAnalyzeKillJobWithLiteral() throws Exception {
        UUID jobId = UUID.randomUUID();
        KillAnalyzedStatement stmt = e.analyze(String.format(Locale.ENGLISH, "kill '%s'", jobId.toString()));
        assertThat(stmt.jobId(), is(jobId));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAnalyzeKillJobsNotParsable() throws Exception {
        e.analyze("kill '6a3d6401-4333-933d-b38c9322fca7'");
    }
}

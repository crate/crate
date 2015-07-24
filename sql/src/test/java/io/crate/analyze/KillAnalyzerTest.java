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

import io.crate.metadata.MetaDataModule;
import io.crate.testing.MockedClusterServiceModule;
import org.elasticsearch.common.inject.Module;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import static org.hamcrest.core.Is.is;

public class KillAnalyzerTest extends BaseAnalyzerTest {

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
                new MockedClusterServiceModule(),
                new MetaDataModule()
        ));
        return modules;
    }

    @Test
    public void testAnalyzeKillAll() throws Exception {
        KillAnalyzedStatement stmt = (KillAnalyzedStatement) analyze("kill all");
        assertThat(stmt.jobId().isPresent(), is(false));
    }

    @Test
    public void testAnalyzeKillJobWithParameter() throws Exception {
        UUID jobId = UUID.randomUUID();
        KillAnalyzedStatement stmt = (KillAnalyzedStatement) analyze("kill $2", new Object[]{2, jobId});
        assertThat(stmt.jobId().get(), is(jobId));
        stmt = (KillAnalyzedStatement) analyze("kill $1", new Object[]{jobId});
        assertThat(stmt.jobId().get(), is(jobId));
        stmt = (KillAnalyzedStatement) analyze("kill ?", new Object[]{jobId});
        assertThat(stmt.jobId().get(), is(jobId));
    }

    @Test
    public void testAnalyzeKillJobWithLiteral() throws Exception {
        UUID jobId = UUID.randomUUID();
        KillAnalyzedStatement stmt = (KillAnalyzedStatement) analyze(String.format(Locale.ENGLISH, "kill '%s'", jobId.toString()));
        assertThat(stmt.jobId().get(), is(jobId));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAnalyzeKillJobsNotParsable() throws Exception {
        analyze("kill '6a3d6401-4333-933d-b38c9322fca7'");
    }

}

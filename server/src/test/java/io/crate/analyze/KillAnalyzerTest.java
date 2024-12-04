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

package io.crate.analyze;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Locale;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import io.crate.data.RowN;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.management.KillPlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class KillAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private PlannerContext plannerContext;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService).build();
        plannerContext = e.getPlannerContext();
    }

    private UUID analyze(String stmt, Object... arguments) {
        AnalyzedKill analyzedStatement = e.analyze(stmt);
        return KillPlan.boundJobId(
            analyzedStatement.jobId(),
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            new RowN(arguments),
            SubQueryResults.EMPTY
        );
    }

    @Test
    public void testAnalyzeKillAll() {
        assertThat(analyze("KILL ALL")).isNull();
    }

    @Test
    public void testAnalyzeKillJobWithParameter() {
        UUID jobId = UUID.randomUUID();
        assertThat(analyze("KILL $2", 2, jobId.toString())).isEqualTo(jobId);
        assertThat(analyze("KILL $1", jobId.toString())).isEqualTo(jobId);
        assertThat(analyze("KILL ?", jobId.toString())).isEqualTo(jobId);
    }

    @Test
    public void testAnalyzeKillJobWithLiteral() {
        UUID jobId = UUID.randomUUID();
        assertThat(analyze(String.format(Locale.ENGLISH, "KILL '%s'", jobId.toString())))
            .isEqualTo(jobId);
    }

    @Test
    public void testAnalyzeKillJobsNotParsable() {
        assertThatThrownBy(() -> analyze("KILL '6a3d6401-4333-933d-b38c9322fca7'"))
            .isExactlyInstanceOf(IllegalArgumentException.class);
    }
}

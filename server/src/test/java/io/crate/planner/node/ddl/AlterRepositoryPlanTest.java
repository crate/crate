/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner.node.ddl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.repositories.put.AlterRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportAlterRepository;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.crate.analyze.AnalyzedAlterRepository;
import io.crate.analyze.repositories.RepositoryParamValidator;
import io.crate.data.Row;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.execution.ddl.RepositoryService;
import io.crate.expression.symbol.Literal;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.GenericProperties;

@RunWith(MockitoJUnitRunner.class)
public class AlterRepositoryPlanTest {
    @Mock
    RepositoryService repoService;
    @Mock
    RepositoryParamValidator repoParamValidator;
    @Mock
    Client client;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    PlannerContext plannerCtx;
    TestingRowConsumer rowConsumer;
    @Mock
    DependencyCarrier dependencyCarrier;

    @Before
    public void setUp() {
        rowConsumer = new TestingRowConsumer();
        when(dependencyCarrier.repositoryService()).thenReturn(repoService);
        when(dependencyCarrier.repositoryParamValidator()).thenReturn(repoParamValidator);
        when(dependencyCarrier.client()).thenReturn(client);
    }

    @Test
    public void test_execute_or_fail() throws Exception {
        var underTest = new AlterRepositoryPlan(
            new AnalyzedAlterRepository(
                "repo-name",
                new GenericProperties<>(Map.of("location", Literal.of("/tmp/data")))
            )
        );

        when(repoService.getRepository("repo-name"))
            .thenReturn(new RepositoryMetadata("repo-name", "dummy-type", Settings.EMPTY));
        when(client.execute(
            TransportAlterRepository.ACTION, new AlterRepositoryRequest("repo-name", Settings.builder().put("location", "/tmp/data").build())
        )).thenReturn(CompletableFuture.completedFuture(new AcknowledgedResponse(true)));

        underTest.executeOrFail(dependencyCarrier, plannerCtx, rowConsumer, Row.EMPTY, SubQueryResults.EMPTY);

        assertThat(rowConsumer.getResult()).hasSize(1);
        assertThat(rowConsumer.getResult().getFirst()).hasSize(1);
        // the number of rows changed
        assertThat(rowConsumer.getResult().getFirst()[0]).isEqualTo(1L);
    }

    @Test
    public void test_execute_or_fail_missing_repository() {
        var underTest = new AlterRepositoryPlan(
            new AnalyzedAlterRepository("repo-name", new GenericProperties<>(Map.of()))
        );

        assertThatThrownBy(() ->
            underTest.executeOrFail(dependencyCarrier, plannerCtx, rowConsumer, Row.EMPTY, SubQueryResults.EMPTY)
        ).isExactlyInstanceOf(RepositoryMissingException.class)
            .hasMessageContaining("[repo-name]");
    }

    @Test
    public void test_execute_or_fail_unsupported_property() {
        var underTest = new AlterRepositoryPlan(
            new AnalyzedAlterRepository("repo-name", new GenericProperties<>(Map.of()))
        );

        when(repoService.getRepository("repo-name"))
            .thenReturn(new RepositoryMetadata("repo-name", "dummy-type", Settings.EMPTY));
        doThrow(new IllegalArgumentException("invalid property"))
            .when(repoParamValidator).validateSupportedOnly(any(), any());

        assertThatThrownBy(() ->
            underTest.executeOrFail(dependencyCarrier, plannerCtx, rowConsumer, Row.EMPTY, SubQueryResults.EMPTY)
        ).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("invalid property");
    }

    @Test
    public void test_execute_or_fail_min_node_version() {
        var underTest = new AlterRepositoryPlan(
            new AnalyzedAlterRepository("repo-name", new GenericProperties<>(Map.of()))
        );

        when(plannerCtx.clusterState().nodes().getMinNodeVersion()).thenReturn(Version.V_6_3_2);

        assertThatThrownBy(() ->
            underTest.executeOrFail(dependencyCarrier, plannerCtx, rowConsumer, Row.EMPTY, SubQueryResults.EMPTY)
        ).isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Cannot execute ALTER REPOSITORY while there are <6.4.0 nodes in the cluster");
    }
}

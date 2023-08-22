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

import java.util.Collections;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.repositories.RepositoryParamValidator;
import io.crate.analyze.repositories.TypeSettings;
import io.crate.data.Row;
import io.crate.exceptions.RepositoryAlreadyExistsException;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.ddl.CreateRepositoryPlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class CreateDropRepositoryAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private PlannerContext plannerContext;
    private RepositoryParamValidator repositoryParamValidator;

    @Before
    public void prepare() {
        RepositoriesMetadata repositoriesMetadata = new RepositoriesMetadata(
            Collections.singletonList(
                new RepositoryMetadata(
                    "my_repo",
                    "fs",
                    Settings.builder().put("location", "/tmp/my_repo").build()
                )));
        ClusterState clusterState = ClusterState.builder(new ClusterName("testing"))
            .metadata(Metadata.builder()
                          .putCustom(RepositoriesMetadata.TYPE, repositoriesMetadata))
            .build();
        ClusterServiceUtils.setState(clusterService, clusterState);
        e = SQLExecutor.builder(clusterService).build();
        plannerContext = e.getPlannerContext(clusterService.state());
        repositoryParamValidator = new RepositoryParamValidator(Map.of(
            "fs", new TypeSettings(FsRepository.mandatorySettings(), FsRepository.optionalSettings())
        ));
    }

    @SuppressWarnings("unchecked")
    private <S> S analyze(SQLExecutor e, String stmt) {
        AnalyzedStatement analyzedStatement = e.analyze(stmt);
        if (analyzedStatement instanceof AnalyzedCreateRepository) {
            return (S) CreateRepositoryPlan.createRequest(
                (AnalyzedCreateRepository) analyzedStatement,
                plannerContext.transactionContext(),
                plannerContext.nodeContext(),
                Row.EMPTY,
                SubQueryResults.EMPTY,
                repositoryParamValidator);
        } else if (analyzedStatement instanceof AnalyzedDropRepository) {
            return (S) analyzedStatement;
        } else {
            throw new AssertionError("Statement of type " + analyzedStatement.getClass() + " not supported");
        }
    }

    @Test
    public void testCreateRepository() {
        PutRepositoryRequest request = analyze(
            e,
            "CREATE REPOSITORY \"new_repository\" TYPE \"fs\" WITH (" +
            "   location='/mount/backups/my_backup'," +
            "   compress=True)");
        assertThat(request.name()).isEqualTo("new_repository");
        assertThat(request.type()).isEqualTo("fs");
        assertThat(request.settings().getAsStructuredMap()).satisfies(
            v -> assertThat(v).containsEntry("compress", "true"),
            v -> assertThat(v).containsEntry("location", "/mount/backups/my_backup")
        );
    }

    @Test
    public void testCreateExistingRepository() {
        assertThatThrownBy(() -> analyze(e, "CREATE REPOSITORY my_repo TYPE fs"))
            .isExactlyInstanceOf(RepositoryAlreadyExistsException.class)
            .hasMessage("Repository 'my_repo' already exists");
    }

    @Test
    public void testDropUnknownRepository() {
        assertThatThrownBy(() -> analyze(e, "DROP REPOSITORY \"unknown_repo\""))
            .isExactlyInstanceOf(RepositoryMissingException.class)
            .hasMessage("[unknown_repo] missing");
    }

    @Test
    public void testDropExistingRepo() {
        AnalyzedDropRepository statement = analyze(e, "DROP REPOSITORY my_repo");
        assertThat(statement.name()).isEqualTo("my_repo");
    }
}

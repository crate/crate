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

package org.elasticsearch.repositories.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.AnalyzedCreateRepository;
import io.crate.analyze.AnalyzedDropRepository;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.repositories.RepositoryParamValidator;
import io.crate.analyze.repositories.TypeSettings;
import io.crate.data.Row;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.ddl.CreateRepositoryPlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.StringLiteral;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class S3RepositoryPluginAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private RepositoryParamValidator repositoryParamValidator;
    private PlannerContext plannerContext;

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
        plannerContext = e.getPlannerContext();
        repositoryParamValidator = new RepositoryParamValidator(
            Map.of("s3", new TypeSettings(List.of(), S3Repository.optionalSettings()))
        );
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
    public void testValidateS3ConfigParams() {
        Map<String, Expression> properties = new HashMap<>();
        properties.put("access_key", new StringLiteral("foobar"));
        properties.put("base_path", new StringLiteral("/data"));
        properties.put("bucket", new StringLiteral("myBucket"));
        properties.put("buffer_size", new StringLiteral("5mb"));
        properties.put("canned_acl", new StringLiteral("cannedACL"));
        properties.put("chunk_size", new StringLiteral("4g"));
        properties.put("compress", new StringLiteral("true"));
        properties.put("endpoint", new StringLiteral("myEndpoint"));
        properties.put("max_retries", new StringLiteral("8"));
        properties.put("protocol", new StringLiteral("http"));
        properties.put("secret_key", new StringLiteral("thisIsASecretKey"));
        properties.put("server_side_encryption", new StringLiteral("false"));
        properties.put("storage_class", new StringLiteral("standard_ia"));
        GenericProperties<Expression> genericProperties = new GenericProperties<>(properties);
        repositoryParamValidator.validate(
            "s3",
            genericProperties,
            toSettings(genericProperties));
    }


    @Test
    public void testCreateS3RepoWithWrongSettings() {
        assertThatThrownBy(
            () -> analyze(e, "CREATE REPOSITORY foo TYPE s3 WITH (wrong=true)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Setting 'wrong' is not supported");
    }

    @Test
    public void testCreateS3RepositoryWithAllSettings() {
        PutRepositoryRequest request = analyze(
            e,
            "CREATE REPOSITORY foo TYPE s3 WITH (" +
            "   bucket='abc'," +
            "   endpoint='www.example.com'," +
            "   protocol='http'," +
            "   base_path='/holz/'," +
            "   access_key='0xAFFE'," +
            "   secret_key='0xCAFEE'," +
            "   chunk_size='12mb'," +
            "   compress=true," +
            "   server_side_encryption=false," +
            "   buffer_size='5mb'," +
            "   max_retries=2," +
            "   use_throttle_retries=false," +
            "   readonly=false, " +
            "   canned_acl=false, " +
            "   storage_class='standard_ia')");
        assertThat(request.name()).isEqualTo("foo");
        assertThat(request.type()).isEqualTo("s3");
        assertThat(request.settings().getAsStructuredMap())
            .hasFieldOrPropertyWithValue("access_key", "0xAFFE")
                .hasFieldOrPropertyWithValue("base_path", "/holz/")
                .hasFieldOrPropertyWithValue("bucket", "abc")
                .hasFieldOrPropertyWithValue("buffer_size", "5mb")
                .hasFieldOrPropertyWithValue("canned_acl", "false")
                .hasFieldOrPropertyWithValue("chunk_size", "12mb")
                .hasFieldOrPropertyWithValue("compress", "true")
                .hasFieldOrPropertyWithValue("endpoint", "www.example.com")
                .hasFieldOrPropertyWithValue("max_retries", "2")
                .hasFieldOrPropertyWithValue("use_throttle_retries", "false")
                .hasFieldOrPropertyWithValue("protocol", "http")
                .hasFieldOrPropertyWithValue("secret_key", "0xCAFEE")
                .hasFieldOrPropertyWithValue("server_side_encryption", "false")
                .hasFieldOrPropertyWithValue("readonly", "false")
                .hasFieldOrPropertyWithValue("storage_class", "standard_ia");
    }

    private static Settings toSettings(GenericProperties<Expression> genericProperties) {
        Settings.Builder builder = Settings.builder();
        for (Map.Entry<String, Expression> property : genericProperties.properties().entrySet()) {
            builder.put(property.getKey(), property.getValue().toString());
        }
        return builder.build();
    }
}

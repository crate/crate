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

package org.elasticsearch.repositories.s3;

import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.hamcrest.Matchers;
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
import io.crate.sql.tree.GenericProperty;
import io.crate.sql.tree.StringLiteral;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class S3RepositoryPluginAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private RepositoryParamValidator repositoryParamValidator;
    private PlannerContext plannerContext;

    @Before
    public void prepare() {
        RepositoriesMetaData repositoriesMetaData = new RepositoriesMetaData(
            Collections.singletonList(
                new RepositoryMetaData(
                    "my_repo",
                    "fs",
                    Settings.builder().put("location", "/tmp/my_repo").build()
                )));
        ClusterState clusterState = ClusterState.builder(new ClusterName("testing"))
            .metaData(MetaData.builder()
                          .putCustom(RepositoriesMetaData.TYPE, repositoriesMetaData))
            .build();
        ClusterServiceUtils.setState(clusterService, clusterState);
        e = SQLExecutor.builder(clusterService).build();
        plannerContext = e.getPlannerContext(clusterService.state());
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
                plannerContext.functions(),
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
        GenericProperties<Expression> genericProperties = new GenericProperties<>();
        genericProperties.add(new GenericProperty<>("access_key", new StringLiteral("foobar")));
        genericProperties.add(new GenericProperty<>("base_path", new StringLiteral("/data")));
        genericProperties.add(new GenericProperty<>("bucket", new StringLiteral("myBucket")));
        genericProperties.add(new GenericProperty<>("buffer_size", new StringLiteral("5mb")));
        genericProperties.add(new GenericProperty<>("canned_acl", new StringLiteral("cannedACL")));
        genericProperties.add(new GenericProperty<>("chunk_size", new StringLiteral("4g")));
        genericProperties.add(new GenericProperty<>("compress", new StringLiteral("true")));
        genericProperties.add(new GenericProperty<>("endpoint", new StringLiteral("myEndpoint")));
        genericProperties.add(new GenericProperty<>("max_retries", new StringLiteral("8")));
        genericProperties.add(new GenericProperty<>("protocol", new StringLiteral("http")));
        genericProperties.add(new GenericProperty<>("secret_key", new StringLiteral("thisIsASecretKey")));
        genericProperties.add(new GenericProperty<>("server_side_encryption", new StringLiteral("false")));
        repositoryParamValidator.validate(
            "s3",
            genericProperties,
            toSettings(genericProperties));
    }


    @Test
    public void testCreateS3RepoWithWrongSettings() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("setting 'wrong' not supported");
        analyze(e, "CREATE REPOSITORY foo TYPE s3 WITH (wrong=true)");
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
            "   canned_acl=false)");
        assertThat(request.name(), is("foo"));
        assertThat(request.type(), is("s3"));
        assertThat(
            request.settings().getAsStructuredMap(),
            Matchers.allOf(
                Matchers.hasEntry("access_key", "0xAFFE"),
                Matchers.hasEntry("base_path", "/holz/"),
                Matchers.hasEntry("bucket", "abc"),
                Matchers.hasEntry("buffer_size", "5mb"),
                Matchers.hasEntry("canned_acl", "false"),
                Matchers.hasEntry("chunk_size", "12mb"),
                Matchers.hasEntry("compress", "true"),
                Matchers.hasEntry("endpoint", "www.example.com"),
                Matchers.hasEntry("max_retries", "2"),
                Matchers.hasEntry("use_throttle_retries", "false"),
                Matchers.hasEntry("protocol", "http"),
                Matchers.hasEntry("secret_key", "0xCAFEE"),
                Matchers.hasEntry("server_side_encryption", "false"),
                Matchers.hasEntry("readonly", "false")
            )
        );
    }

    private static Settings toSettings(GenericProperties<Expression> genericProperties) {
        Settings.Builder builder = Settings.builder();
        for (Map.Entry<String, Expression> property : genericProperties.properties().entrySet()) {
            builder.put(property.getKey(), property.getValue().toString());
        }
        return builder.build();
    }
}

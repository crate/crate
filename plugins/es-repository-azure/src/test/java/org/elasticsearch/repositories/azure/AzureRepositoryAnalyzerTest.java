package org.elasticsearch.repositories.azure;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
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
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class AzureRepositoryAnalyzerTest extends CrateDummyClusterServiceUnitTest {

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
        plannerContext = e.getPlannerContext(clusterService.state());
        repositoryParamValidator = new RepositoryParamValidator(
            Map.of("azure", new TypeSettings(AzureRepository.mandatorySettings(), AzureRepository.optionalSettings()))
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
    public void testCreateAzureRepoWithMissingMandatorySettings() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The following required parameters are missing to" +
                                        " create a repository of type \"azure\": [key]");
        analyze(e, "CREATE REPOSITORY foo TYPE azure WITH (account='test')");
    }


    @Test
    public void testCreateAzureRepositoryWithAllSettings() {
        PutRepositoryRequest request = analyze(
            e,
            "CREATE REPOSITORY foo TYPE azure WITH (" +
            "   container='test_container'," +
            "   base_path='test_path'," +
            "   chunk_size='12mb'," +
            "   compress=true," +
            "   readonly=true," +
            "   location_mode='primary_only'," +
            "   account='test_account'," +
            "   key='test_key'," +
            "   max_retries=3," +
            "   endpoint_suffix='.com'," +
            "   timeout='30s'," +
            "   proxy_port=0," +
            "   proxy_type='socks'," +
            "   proxy_host='localhost')");
        assertThat(request.name(), is("foo"));
        assertThat(request.type(), is("azure"));
        assertThat(
            request.settings().getAsStructuredMap(),
            Matchers.allOf(
                Matchers.hasEntry("container", "test_container"),
                Matchers.hasEntry("base_path", "test_path"),
                Matchers.hasEntry("chunk_size", "12mb"),
                Matchers.hasEntry("compress", "true"),
                Matchers.hasEntry("readonly", "true"),
                Matchers.hasEntry("account", "test_account"),
                Matchers.hasEntry("key", "test_key"),
                Matchers.hasEntry("max_retries", "3"),
                Matchers.hasEntry("endpoint_suffix", ".com"),
                Matchers.hasEntry("timeout", "30s"),
                Matchers.hasEntry("proxy_port", "0"),
                Matchers.hasEntry("proxy_type", "socks"),
                Matchers.hasEntry("proxy_host", "localhost")
            )
        );
    }


    @Test
    public void testCreateAzureRepoWithWrongSettings() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("setting 'wrong' not supported");
        analyze(e, "CREATE REPOSITORY foo TYPE azure WITH (wrong=true)");
    }
}

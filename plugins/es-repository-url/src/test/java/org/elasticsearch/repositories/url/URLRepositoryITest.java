
package org.elasticsearch.repositories.url;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThrowsMatches;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugin.repository.url.URLRepositoryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.crate.integrationtests.SQLIntegrationTestCase;

public class URLRepositoryITest extends SQLIntegrationTestCase {

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private File defaultRepositoryLocation;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put("path.repo", TEMPORARY_FOLDER.getRoot().getAbsolutePath())
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(URLRepositoryPlugin.class);
        return plugins;
    }

    @Before
    public void createRepository() throws Exception {
        defaultRepositoryLocation = TEMPORARY_FOLDER.newFolder();
        execute("CREATE REPOSITORY my_repo TYPE \"fs\" with (location=?, compress=True)",
            new Object[]{defaultRepositoryLocation.getAbsolutePath()});
        assertThat(response.rowCount()).isEqualTo(1L);
    }

    @Test
    public void testCreateSnapshotInURLRepoFails() throws Exception {
        // lets be sure the repository location contains some data, empty directories will result in "no data found" error instead
        execute("CREATE SNAPSHOT my_repo.my_snapshot ALL WITH (wait_for_completion=true)");

        // URL Repositories are always marked as read_only, use the same location that the existing repository to have valid data
        execute("CREATE REPOSITORY uri_repo TYPE url WITH (url=?)",
            new Object[]{defaultRepositoryLocation.toURI().toString()});
        waitNoPendingTasksOnAll();

        assertThrowsMatches(() -> execute("CREATE SNAPSHOT uri_repo.my_snapshot ALL WITH (wait_for_completion=true)"),
                     isSQLError(is("[uri_repo] cannot create snapshot in a readonly repository"),
                         INTERNAL_ERROR,
                         INTERNAL_SERVER_ERROR,
                         5000));
    }

}

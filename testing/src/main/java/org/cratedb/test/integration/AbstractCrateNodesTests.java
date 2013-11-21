package org.cratedb.test.integration;

import org.elasticsearch.AbstractNodesTests;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

public class AbstractCrateNodesTests extends AbstractNodesTests {

    private Map<String, Path> tmpDataDirs = new HashMap<>();

    @Override
    public Node buildNode(String id, Settings settings) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        builder.put(settings);

        // Create temporary directory and use it as the data directory
        try {
            Path tmpDataDir = Files.createTempDirectory(null);
            tmpDataDirs.put(id, tmpDataDir);
            builder.put("path.data", tmpDataDir.toAbsolutePath());
            if (settings.get("gateway.type") == null) {
                // default to non gateway
                builder.put("gateway.type", "none");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return super.buildNode(id, builder.build());
    }

    public void deleteTemporaryDataDirectory(String id) {
        assert tmpDataDirs.containsKey(id);
        Path tmpDataDir = tmpDataDirs.get(id);
        if (tmpDataDir != null) {
            FileSystemUtils.deleteRecursively(tmpDataDir.toFile(), true);
        }
    }

    @Override
    public void closeAllNodes(boolean preventRelocation) {
        synchronized (AbstractCrateNodesTests.class) {
            for (String id : tmpDataDirs.keySet()) {
                deleteTemporaryDataDirectory(id);
            }
        }
        super.closeAllNodes(preventRelocation);
    }

    @Override
    public void closeNode(String id) {
        deleteTemporaryDataDirectory(id);
        super.closeNode(id);
    }

    public void refresh(Client client) {
        RefreshResponse actionGet = client.admin().indices().prepareRefresh().execute().actionGet();
        assertNoFailures(actionGet);
    }

    public void createIndex(Client client, String indexName, Settings indexSettings) {
        client.admin().indices().prepareCreate(indexName).setSettings(indexSettings).execute().actionGet();
        refresh(client);
    }
}

package org.cratedb.test.integration;

import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.integration.AbstractNodesTests;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class AbstractCrateNodesTests extends AbstractNodesTests {

    private Map<String, Path> tmpDataDirs = new HashMap<String, Path>();

    public Node buildNode(String id, Settings settings) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        builder.put(settings);

        // Create temporary directory and use it as the data directory
        File currentWorkingDir = new File(System.getProperty("user.dir"));
        try {
            Path tmpDataDir = Files.createTempDirectory(currentWorkingDir.toPath(), null);
            tmpDataDirs.put(id, tmpDataDir);
            builder.put("path.data", tmpDataDir.toAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return super.buildNode(id, settings);
    }

    public void deleteTemporaryDataDirectory(String id) {
        assert !tmpDataDirs.containsKey(id);
        Path tmpDataDir = tmpDataDirs.get(id);
        if (tmpDataDir != null) {
            FileSystemUtils.deleteRecursively(tmpDataDir.toFile(), true);
        }
    }

    public void closeAllNodes(boolean preventRelocation) {
        synchronized (AbstractCrateNodesTests.class) {
            for (String id : tmpDataDirs.keySet()) {
                deleteTemporaryDataDirectory(id);
            }
        }
        super.closeAllNodes(preventRelocation);
    }

    public void closeNode(String id) {
        deleteTemporaryDataDirectory(id);
        super.closeNode(id);
    }
}

package org.cratedb.test.integration;

import org.elasticsearch.TestCluster;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * TestCluster that creates data-dirs as a temporary directory
 * and that deletes data directories of nodes after closing
 *
 * To ensure that all created directories are destroyed afterwards
 * please call close() when your test is finished (e.g. in method annotated with @AfterClass)
 */
public class CrateTestCluster extends TestCluster {

    private Map<String, Path> tmpDataDirs = new HashMap<>();

    public CrateTestCluster(Random random) {
        super(random);
    }

    @Override
    public Node buildNode(Settings settings) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        Node node = null;
        // Create temporary directory and use it as the data directory
        try {
            Path tmpDataDir = Files.createTempDirectory(null);
            builder.put("path.data", tmpDataDir.toAbsolutePath());
            node = super.buildNode(builder.put(settings).put().build());
            String nodeName = node.settings().get("name");
            tmpDataDirs.put(nodeName, tmpDataDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return node;
    }

    @Override
    public void closeNode(Node node) {
        Path nodeDataDir = tmpDataDirs.get(node.settings().get("name"));
        if (nodeDataDir != null) {
            FileSystemUtils.deleteRecursively(nodeDataDir.toFile(), true);
        }
        super.closeNode(node);
    }

    @Override
    public void close() {
        for (Path tmpPath : tmpDataDirs.values()) {
            FileSystemUtils.deleteRecursively(tmpPath.toFile(), true);
        }
        super.close();    //To change body of overridden methods use File | Settings | File Templates.
    }

}

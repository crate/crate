package crate.elasticsearch.integrationtests;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;


import java.io.File;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public abstract class AbstractNodesTests {

    protected final ESLogger logger = Loggers.getLogger(getClass());

    private Map<String, Node> nodes = newHashMap();

        private Map<String, Client> clients = newHashMap();

        private Settings defaultSettings =
            settingsBuilder()
            .put("cluster.name", "test-cluster-" + NetworkUtils.getLocalAddress().getHostName())
            .build();

        public void putDefaultSettings(Settings.Builder settings) {
            putDefaultSettings(settings.build());
        }

        public void putDefaultSettings(Settings settings) {
            defaultSettings = settingsBuilder().put(defaultSettings).put(settings).build();
        }

        public Node startNode(String id) {
            return buildNode(id).start();
        }

        public Node startNode(String id, Settings.Builder settings) {
            return startNode(id, settings.build());
        }

        public Node startNode(String id, Settings settings) {
            return buildNode(id, settings).start();
        }

        public Node buildNode(String id) {
            return buildNode(id, EMPTY_SETTINGS);
        }

        public Node buildNode(String id, Settings.Builder settings) {
            return buildNode(id, settings.build());
        }

        public Node buildNode(String id, Settings settings) {
            String settingsSource = getClass().getName().replace('.', '/') + ".yml";
            Settings finalSettings = settingsBuilder()
                .loadFromClasspath(settingsSource)
                .put(defaultSettings)
                .put(settings)
                .put("name", id)
                .build();

            if (finalSettings.get("gateway.type") == null) {
                // default to non gateway
                finalSettings = settingsBuilder().put(finalSettings).put("gateway.type", "none").build();
            }
            if (finalSettings.get("cluster.routing.schedule") != null) {
                // decrease the routing schedule so new nodes will be added quickly
                finalSettings = settingsBuilder().put(finalSettings).put("cluster.routing.schedule", "50ms").build();
            }

            Node node = nodeBuilder()
                .settings(finalSettings)
                .build();
            nodes.put(id, node);
            clients.put(id, node.client());
            return node;
        }

        public void closeNode(String id) {
            Client client = clients.remove(id);
            if (client != null) {
                client.close();
            }
            Node node = nodes.remove(id);
            if (node != null) {
                node.close();
            }
        }

        public Node node(String id) {
            return nodes.get(id);
        }

        public Client client(String id) {
            return clients.get(id);
        }

        public void closeAllNodes() {
            for (Client client : clients.values()) {
                client.close();
            }
            clients.clear();
            for (Node node : nodes.values()) {
                node.close();
            }
            nodes.clear();
        }

        public ImmutableSet<ClusterBlock> waitForNoBlocks(TimeValue timeout, String node) throws InterruptedException {
            long start = System.currentTimeMillis();
            ImmutableSet<ClusterBlock> blocks;
            do {
                blocks = client(node).admin().cluster().prepareState().setLocal(true).execute().actionGet()
                    .getState().blocks().global(ClusterBlockLevel.METADATA);
            }
            while (!blocks.isEmpty() && (System.currentTimeMillis() - start) < timeout.millis());
            return blocks;
        }
    }

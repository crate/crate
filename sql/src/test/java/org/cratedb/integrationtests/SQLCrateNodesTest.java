package org.cratedb.integrationtests;

import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.test.integration.AbstractCrateNodesTests;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.junit.AfterClass;

import java.util.HashMap;
import java.util.Map;

public class SQLCrateNodesTest extends AbstractCrateNodesTests {

    public static Map<String, Node> nodes = new HashMap<>();

    @Override
    public Node startNode(String id) {
        Node started = super.startNode(id);
        nodes.put(id, started);
        return started;
    }

    @Override
    public Node startNode(String id, Settings settings) {
        Node started = super.startNode(id, settings);    //To change body of overridden methods use File | Settings | File Templates.
        nodes.put(id, started);
        return started;
    }

    @AfterClass
    public static void cleanNodes() {
        for (Node node : nodes.values()) {
            if (node != null) {
                node.stop();
                node.close();
            }
        }
        nodes.clear();
        nodes = null;
    }

    public SQLResponse execute(Client client, String stmt, Object[]  args) {
        return client.execute(SQLAction.INSTANCE, new SQLRequest(stmt, args)).actionGet();
    }

    public SQLResponse execute(Client client, String stmt) {
        return execute(client, stmt, new Object[0]);
    }

    public SQLResponse execute(String stmt, Object[] args) {
        return execute(client(), stmt, args);
    }

    public SQLResponse execute(String stmt) {
        return execute(stmt, new Object[0]);
    }
}

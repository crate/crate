package org.cratedb.client;

import org.cratedb.action.sql.SQLResponse;
import org.cratedb.test.integration.AbstractCrateNodesTests;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Test;

import java.util.Arrays;

public class CrateClientTest extends AbstractCrateNodesTests {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Test
    public void testCreateClient() throws Exception {

        Node server = startNode("serverside");
        int port = ((InetSocketTransportAddress) ((InternalNode) server).injector()
                .getInstance(TransportService.class)
                .boundAddress().boundAddress()).address().getPort();

        server.client().prepareIndex("test", "default", "1")
                .setRefresh(true)
                .setSource("{}")
                .execute()
                .actionGet();

        CrateClient client = new CrateClient("localhost:" +  port);
        SQLResponse r = client.sql("select \"_id\" from test").actionGet();

        assertEquals(1, r.rows().length);
        assertEquals("_id", r.cols()[0]);
        assertEquals("1", r.rows()[0][0]);

        System.out.println(Arrays.toString(r.cols()));
        for (Object[] row: r.rows()){
            System.out.println(Arrays.toString(row));
        }

    }

    @After
    public void shutdownNodes() throws Exception {
        super.tearDown();
        closeAllNodes();
    }

}

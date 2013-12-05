package org.cratedb.client;

import org.cratedb.action.sql.SQLResponse;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Test;

import java.util.Arrays;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.SUITE, numNodes = 0)
public class CrateClientTest extends CrateIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Test
    public void testCreateClient() throws Exception {

        String nodeName = cluster().startNode();

        int port = ((InetSocketTransportAddress) cluster()
            .getInstance(TransportService.class)
            .boundAddress().boundAddress()).address().getPort();

        client(nodeName).prepareIndex("test", "default", "1")
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
}

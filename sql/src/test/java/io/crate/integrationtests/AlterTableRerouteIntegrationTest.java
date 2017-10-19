package io.crate.integrationtests;

import io.crate.testing.UseJdbc;
import io.crate.testing.UseRandomizedSession;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

// ensure that only data nodes are picked for rerouting
@ESIntegTestCase.ClusterScope(supportsDedicatedMasters = false, numDataNodes = 2, numClientNodes = 0)
@UseRandomizedSession(schema = false)
@UseJdbc(0) // reroute table has no rowcount
public class AlterTableRerouteIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testAlterTableRerouteMoveShard() throws Exception {
        int shardId = 0;
        String tableName = "my_table";
        execute("create table " + tableName + " (" +
            "id int primary key," +
            "date timestamp" +
            ") clustered into 1 shards " +
            "with (number_of_replicas=0)");
        ensureGreen();
        execute("select _node['id'] from sys.shards where id = ? and table_name = ?", new Object[]{shardId, tableName});
        String fromNode = (String) response.rows()[0][0];
        execute("select id from sys.nodes where id != ?", new Object[]{fromNode});
        String toNode = (String) response.rows()[0][0];

        execute("ALTER TABLE my_table REROUTE MOVE SHARD ? FROM ? TO ?", new Object[]{shardId, fromNode, toNode});
        assertThat(response.rowCount(), is(1L));
        ensureGreen();
        execute("select * from sys.shards where id = ? and _node['id'] = ? and table_name = ?", new Object[]{shardId, toNode, tableName});
        assertBusy(() -> assertThat(response.rowCount(), is(1L)));
        execute("select * from sys.shards where id = ? and _node['id'] = ? and table_name = ?", new Object[]{shardId, fromNode, tableName});
        assertBusy(() -> assertThat(response.rowCount(), is(0L)));
    }
}

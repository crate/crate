package org.cratedb.integrationtests;

import org.cratedb.SQLCrateClusterTest;
import org.cratedb.action.sql.SQLResponse;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class ShardStatsTest extends SQLCrateClusterTest {

    private SQLResponse response;
    private Setup setup = new Setup(this);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    @Override
    protected int numberOfNodes() {
        return 2;
    }

    /**
     * override execute to store response in property for easier access
     */
    @Override
    public SQLResponse execute(String stmt, Object[] args) {
        response = super.execute(stmt, args);
        return response;
    }

    @Before
    public void initTestData() throws Exception{
        setup.groupBySetup();
        execute("create table quotes (id integer primary key, quote string)");
        ensureGreen();
    }


    @Test
    public void testSelectGroupByWhereTable() throws Exception {
        execute("select count(*), num_docs from stats.shards where table_name = 'characters' " +
                "group by num_docs order by count(*)");
        assertThat(response.rowCount(), greaterThan(0L));
    }

    @Test
    public void testSelectGroupByAllTables() throws Exception {
        execute("select count(*), table_name from stats.shards " +
                "group by table_name order by table_name");
        assertEquals(2L, response.rowCount());
        assertEquals(10L, response.rows()[0][0]);
        assertEquals("characters", response.rows()[0][1]);
        assertEquals("quotes", response.rows()[1][1]);
    }

    @Test
    public void testSelectWhereTable() throws Exception {
        execute("select node_id, shard_id, size from stats.shards where table_name = " +
                "'characters'");
        assertEquals(10L, response.rowCount());
    }

    @Test
    public void testSelectStarWhereTable() throws Exception {
        execute("select * from stats.shards where table_name = 'characters'");
        assertEquals(10L, response.rowCount());
        assertEquals(9, response.cols().length);
    }

    @Test
    public void testSelectStarAllTables() throws Exception {
        execute("select * from stats.shards");
        assertEquals(20L, response.rowCount());
        assertEquals(9, response.cols().length);
    }

    @Test
    public void testSelectStarLike() throws Exception {
        execute("select * from stats.shards where table_name like 'charact%'");
        assertEquals(10L, response.rowCount());
        assertEquals(9, response.cols().length);
    }

    @Test
    public void testSelectStarNotLike() throws Exception {
        execute("select * from stats.shards where table_name not like 'quotes%'");
        assertEquals(10L, response.rowCount());
        assertEquals(9, response.cols().length);
    }

    @Test
    public void testSelectStarIn() throws Exception {
        execute("select * from stats.shards where table_name in ('characters')");
        assertEquals(10L, response.rowCount());
        assertEquals(9, response.cols().length);
    }

    @Test
    public void testSelectStarMatch() throws Exception {
        execute("select * from stats.shards where match(table_name, 'characters')");
        assertEquals(10L, response.rowCount());
        assertEquals(9, response.cols().length);
    }

    @Test
    public void testSelectOrderBy() throws Exception {
        execute("select * from stats.shards order by table_name");
        assertEquals(20L, response.rowCount());
        assertEquals("characters", response.rows()[0][8]);
        assertEquals("characters", response.rows()[1][8]);
        assertEquals("characters", response.rows()[2][8]);
        assertEquals("characters", response.rows()[3][8]);
        assertEquals("characters", response.rows()[4][8]);
        assertEquals("quotes", response.rows()[10][8]);
    }

    @Test
    public void testSelectGreaterThan() throws Exception {
        execute("select * from stats.shards where num_docs > 0");
        assertThat(response.rowCount(), greaterThan(0L));
    }

    @Test
    public void testSelectWhereBoolean() throws Exception {
        execute("select * from stats.shards where \"primary\" = false");
        assertEquals(10L, response.rowCount());
    }

    @Test
    public void testSelectIncludingUnassignedShards() throws Exception {
        execute("create table locations (id integer primary key, name string) replicas 2");
        refresh();
        ensureYellow();

        execute("select * from stats.shards order by state");
        assertEquals(35L, response.rowCount());
        assertEquals(9, response.cols().length);
    }

    @Test
    public void testSelectGroupByIncludingUnassignedShards() throws Exception {
        execute("create table locations (id integer primary key, name string) replicas 2");
        refresh();
        ensureYellow();

        execute("select count(*), state from stats.shards " +
                "group by state order by state desc");
        assertThat(response.rowCount(), greaterThanOrEqualTo(2L));
        assertEquals(2, response.cols().length);
        assertThat((Long)response.rows()[0][0], greaterThanOrEqualTo(5L));
        assertEquals("UNASSIGNED", response.rows()[0][1]);
    }

    @Test
    public void testSelectGlobalAggregates() throws Exception {
        execute("select sum(size), min(size), max(size), avg(size) from stats.shards");
        assertEquals(1L, response.rowCount());
        assertEquals(4, response.rows()[0].length);
        assertNotNull(response.rows()[0][0]);
        assertNotNull(response.rows()[0][1]);
        assertNotNull(response.rows()[0][2]);
        assertNotNull(response.rows()[0][3]);
    }
}

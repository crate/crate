package org.cratedb.module.sql.test;


import com.google.common.collect.ImmutableMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.information_schema.AbstractInformationSchemaTable;
import org.cratedb.information_schema.InformationSchemaColumn;
import org.cratedb.information_schema.InformationSchemaStringColumn;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.AbstractZenNodesTests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.node.Node;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;

public class InformationSchemaTableTest extends AbstractZenNodesTests {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    public static class TestInformationSchemaTable extends
            AbstractInformationSchemaTable {
        public static final String NAME = "nodes";

        @Override
        public void doIndex(ClusterState clusterState) throws IOException {
            for (DiscoveryNode node : clusterState.nodes()) {
                Document doc = new Document();
                doc.add(new StringField("id", node.getId(), Field.Store.YES));
                doc.add(new StringField("name", node.getName(), Field.Store.YES));
                doc.add(new StringField("address", node.address().toString(), Field.Store.YES));
                indexWriter.addDocument(doc);
            }
        }

        @Override
        public Iterable<String> cols() {
            return Arrays.asList("id", "name", "address");
        }

        @Override
        public ImmutableMap<String, InformationSchemaColumn> fieldMapper() {
            return ImmutableMap.of(
                    "id", (InformationSchemaColumn) new InformationSchemaStringColumn("id"),
                    "name", new InformationSchemaStringColumn("name"),
                    "address", new InformationSchemaStringColumn("address")
            );
        }
    }

    private SQLResponse response = null;
    private TestInformationSchemaTable testTable = null;
    private static final List<Node> nodes = new ArrayList<>(3);
    /**
     * execUsingClient the statement using the transportClient
     * @param statement
     * @param args
     * @throws Exception
     */
    private void execUsingClient(String statement, Object[] args) throws Exception {
        response = client().execute(SQLAction.INSTANCE, new SQLRequest(statement, args)).actionGet();
    }

    @Before
    public void startNodes() {
        if (nodes.isEmpty()) {
            nodes.add(startNode("node1"));
            nodes.add(startNode("node2"));
            nodes.add(startNode("node3"));
        }

    }

    @AfterClass
    public static void shutDownNodes() {
        for (Node node: nodes) {
            node.stop();
            node.close();
        }
        nodes.clear();
    }

    @After
    public void cleanTestTable() {
        if (testTable != null) {
            testTable.close();
        }
        testTable = null;
    }

    @Test
    public void initTestTable() {
        testTable = new TestInformationSchemaTable();
        assertFalse(testTable.initialized());
        testTable.init();
        assertTrue(testTable.initialized());
    }

    @Test
    public void lazyInitializeOnIndex() {
        testTable = new TestInformationSchemaTable();
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet()
                .getState();
        assertFalse(testTable.initialized());
        testTable.index(state);
        assertTrue(testTable.initialized());
    }

    @Test
    public void indexThenQuery() {
        testTable = new TestInformationSchemaTable();
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet()
                .getState();
        testTable.index(state);
        ParsedStatement stmt = new ParsedStatement("select id, name, address from nodes");
        stmt.limit = 1000;
        stmt.offset = 0;
        stmt.orderByColumns = new ArrayList<>();
        stmt.query = new MatchAllDocsQuery();
        stmt.outputFields.add(new Tuple<>("id", "id"));
        stmt.outputFields.add(new Tuple<>("name", "name"));
        stmt.outputFields.add(new Tuple<>("address", "address"));

        testTable.query(stmt, new ActionListener<SQLResponse>() {
            @Override
            public void onResponse(SQLResponse sqlResponse) {
                assertEquals(3L, sqlResponse.rowCount());
                assertThat(new String[]{
                        (String)sqlResponse.rows()[0][1], (String)sqlResponse.rows()[1][1],
                        (String)sqlResponse.rows()[2][1]},
                        arrayContainingInAnyOrder("node1", "node2", "node3"));
            }

            @Override
            public void onFailure(Throwable e) {
                // ignore
            }
        });
    }

    @Test
    public void emptyQuery() {
        testTable = new TestInformationSchemaTable();
        testTable.init();
        assertEquals(0L, testTable.count());

        ParsedStatement stmt = new ParsedStatement("select id, name, address from nodes");
        stmt.limit = 1000;
        stmt.offset = 0;
        stmt.orderByColumns = new ArrayList<>();
        stmt.query = new MatchAllDocsQuery();
        stmt.outputFields.add(new Tuple<>("id", "id"));
        stmt.outputFields.add(new Tuple<>("name", "name"));
        stmt.outputFields.add(new Tuple<>("address", "address"));

        testTable.query(stmt, new ActionListener<SQLResponse>() {
            @Override
            public void onResponse(SQLResponse sqlResponse) {
                assertEquals(0L, sqlResponse.rowCount());
                assertEquals(sqlResponse.rows().length, 0);
            }

            @Override
            public void onFailure(Throwable e) {
                // ignore
            }
        });
    }
}

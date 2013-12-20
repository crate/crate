package org.cratedb.module.sql.test;


import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.parser.context.HandlerContext;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.information_schema.AbstractInformationSchemaTable;
import org.cratedb.information_schema.InformationSchemaTable;
import org.cratedb.information_schema.InformationSchemaTableExecutionContext;
import org.cratedb.lucene.LuceneFieldMapper;
import org.cratedb.lucene.fields.StringLuceneField;
import org.cratedb.service.SQLParseService;
import org.cratedb.stubs.HitchhikerMocks;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.mockito.Mockito.when;

public class InformationSchemaTableTest extends CrateIntegrationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public static class TestInformationSchemaTable extends AbstractInformationSchemaTable {
        public static final String NAME = "nodes";

        public TestInformationSchemaTable(Map<String, AggFunction> aggFunctionMap) {
            super(aggFunctionMap, null);
        }

        @Override
        public void doIndex(ClusterState clusterState) throws IOException {
            for (DiscoveryNode node : clusterState.nodes()) {
                Document doc = new Document();
                doc.add(new StringField("id", node.getId(), Field.Store.YES));
                doc.add(new StringField("name", node.getName(), Field.Store.YES));
                doc.add(new StringField("address", node.address().toString(), Field.Store.YES));
                doc.add(new StringField("many", node.version().number(), Field.Store.YES));
                doc.add(new StringField("many", node.version().luceneVersion.toString(), Field.Store.YES));
                indexWriter.addDocument(doc);
            }
        }

        @Override
        public Iterable<String> cols() {
            return Arrays.asList("id", "name", "address", "many");
        }

        @Override
        public LuceneFieldMapper fieldMapper() {
            return new LuceneFieldMapper(){{
                put("id", new StringLuceneField("id"));
                put("name", new StringLuceneField("name"));
                put("address", new StringLuceneField("address"));
                put("many", new StringLuceneField("many", true));
            }};
        }
    }

    private SQLParseService parseService;
    private TestInformationSchemaTable testTable = null;


    @Before
    public void initParseService() throws Exception {
        NodeExecutionContext nec = HitchhikerMocks.nodeExecutionContext();
        when(nec.tableContext("information_schema", "nodes")).thenReturn(new InformationSchemaTableExecutionContext(
                new HashMap<String, InformationSchemaTable>(1) {{
                    put("nodes", new TestInformationSchemaTable(HitchhikerMocks.aggFunctionMap));
                }},
                "nodes"
        ));
        parseService = new SQLParseService(nec);
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
        testTable = new TestInformationSchemaTable(HitchhikerMocks.aggFunctionMap);
        assertFalse(testTable.initialized());
        testTable.init();
        assertTrue(testTable.initialized());
    }

    @Test
    public void lazyInitializeOnIndex() {
        testTable = new TestInformationSchemaTable(HitchhikerMocks.aggFunctionMap);
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet()
                .getState();
        assertFalse(testTable.initialized());
        testTable.index(state);
        assertTrue(testTable.initialized());
    }

    @Test
    public void indexThenQuery() {
        testTable = new TestInformationSchemaTable(HitchhikerMocks.aggFunctionMap);
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        testTable.index(state);
        ParsedStatement stmt = parseService.parse("select id, name, address, many from information_schema.nodes", HandlerContext.INSTANCE);

        testTable.query(stmt, new ActionListener<SQLResponse>() {
            @Override
            public void onResponse(SQLResponse sqlResponse) {
                assertEquals(2L, sqlResponse.rowCount());
                assertThat(new String[]{
                        (String)sqlResponse.rows()[0][1], (String)sqlResponse.rows()[1][1]},
                        arrayContainingInAnyOrder("node_0", "node_1"));
                assertTrue(sqlResponse.rows()[0][3] instanceof List);
            }

            @Override
            public void onFailure(Throwable e) {
                fail(e.getMessage());
            }
        }, System.currentTimeMillis());
    }

    @Test
    public void emptyQuery() throws Exception {
        testTable = new TestInformationSchemaTable(HitchhikerMocks.aggFunctionMap);
        testTable.init();
        assertEquals(0L, testTable.count());

        ParsedStatement stmt = parseService.parse("select id, name, address, many from information_schema.nodes", HandlerContext.INSTANCE);

        testTable.query(stmt, new ActionListener<SQLResponse>() {
            @Override
            public void onResponse(SQLResponse sqlResponse) {
                assertEquals(0L, sqlResponse.rowCount());
                assertEquals(sqlResponse.rows().length, 0);
            }

            @Override
            public void onFailure(Throwable e) {
                fail(e.getMessage());
            }
        }, System.currentTimeMillis());
    }
}

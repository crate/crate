package org.cratedb.integrationtests;

import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.core.Constants;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.ValidationException;
import org.cratedb.test.integration.AbstractCrateNodesTests;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.internal.InternalNode;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class SQLTypeMappingTest extends AbstractCrateNodesTests {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private static InternalNode node1 = null;
    private static InternalNode node2 = null;
    private static SQLParseService parseService = null;

    @Before
    public void before() throws Exception {
        if (node1 == null) {
            node1 = (InternalNode)startNode("node1");
            parseService = node1.injector().getInstance(SQLParseService.class);
        }
        if (node2 == null) {
            node2 = (InternalNode)startNode("node2");
        }
    }

    @After
    public void cleanUp() throws Exception {
        Set<String> indices = node1.client().admin().cluster().prepareState().execute()
                .actionGet()
                .getState().metaData().getIndices().keySet();
        node1.client().admin().indices()
                .prepareDelete(indices.toArray(new String[indices.size()]))
                .execute()
                .actionGet();
        refresh(node1.client());
    }

    @AfterClass
    public static void shutdownNode() throws Exception {
        parseService = null;
        if (node1 != null) {
            node1.stop();
            node1.close();
            node1 = null;
        }
        if (node2 != null) {
            node2.stop();
            node2.close();
            node2 = null;
        }
    }

    public SQLResponse execute(Client client, String stmt, Object[]  args) {
        return client.execute(SQLAction.INSTANCE, new SQLRequest(stmt, args)).actionGet();
    }

    public SQLResponse execute(Client client, String stmt) {
        return execute(client, stmt, new Object[0]);
    }

    public SQLResponse execute(String stmt, Object[] args) {
        Client client = ((getRandom().nextInt(10) % 2) == 0 ? node1.client() : node2.client());
        return execute(client, stmt, args);
    }

    public SQLResponse execute(String stmt) {
        return execute(stmt, new Object[0]);
    }

    private void setUpSimple() {
        execute(node1.client(), "create table t1(" +
                "  id integer primary key," +
                "  title string," +
                "  checked boolean," +
                "  created timestamp," +
                "  weight double," +
                "  size byte" +
                ") clustered into 1 shards replicas 0");
        refresh(node1.client());
    }

    @Test
    public void testInsertAtNodeWithoutShard() {
       setUpSimple();

        execute(node1.client(), "insert into t1 (id, title, " +
                "created, size) values (?, ?, ?, ?)", new Object[]{1, "With",
                "1970-01-01T00:00:00", 127});
        execute(node2.client(), "insert into t1 (id, title, " +
                "created, size) values (?, ?, ?, ?)", new Object[]{2, "Without",
                "1970-01-01T01:00:00", Byte.MIN_VALUE});
        refresh(node1.client());
        SQLResponse response = execute("select id, title, created, size from t1 order by id");

        assertEquals(1, response.rows()[0][0]);
        assertEquals("With", response.rows()[0][1]);
        assertEquals(0, response.rows()[0][2]);
        assertEquals(127, response.rows()[0][3]);

        assertEquals(2, response.rows()[1][0]);
        assertEquals("Without", response.rows()[1][1]);
        assertEquals(3600000, response.rows()[1][2]);
        assertEquals(-128, response.rows()[1][3]);
    }

    public void setUpCratyMapping() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .startObject("properties")
                    .startObject("craty_field")
                        .startObject("properties")
                            .startObject("size")
                                .field("type", "byte")
                                .field("index", "not_analyzed")
                            .endObject()
                            .startObject("created")
                                .field("type", "date")
                                .field("index", "not_analyzed")
                            .endObject()
                        .endObject()
                    .endObject()
                    .startObject("strict_field")
                        .field("type", "object")
                        .field("dynamic", "strict")
                        .startObject("properties")
                            .startObject("path")
                                .field("type", "string")
                                .field("index", "not_analyzed")
                            .endObject()
                            .startObject("created")
                                .field("type", "date")
                                .field("index", "not_analyzed")
                            .endObject()
                        .endObject()
                    .endObject()
                    .startObject("no_dynamic_field")
                        .field("type", "object")
                        .field("dynamic", false)
                        .startObject("properties")
                            .startObject("path")
                                .field("type", "string")
                                .field("index", "not_analyzed")
                            .endObject()
                            .startObject("dynamic_again")
                                .startObject("properties")
                                    .startObject("field")
                                        .field("type", "date")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .endObject()
                .endObject();
        node1.client().admin().indices().prepareCreate("test12")
                .setSettings(ImmutableSettings.builder()
                        .put("number_of_replicas", 0)
                        .put("number_of_shards", 2))
                .addMapping(Constants.DEFAULT_MAPPING_TYPE, builder)
                .execute().actionGet();
        refresh(node1.client());
    }

    @Test
    public void testParseInsertObject() throws Exception {
        setUpCratyMapping();

        execute("insert into test12 (craty_field, strict_field, " +
                "no_dynamic_field) values (?,?,?)",
                new Object[]{
                    new HashMap<String, Object>(){{
                        put("size", 127);
                        put("created", "2013-11-19");
                    }},
                    new HashMap<String, Object>(){{
                       put("path", "/dev/null");
                       put("created", "1970-01-01T00:00:00");
                    }},
                    new HashMap<String, Object>(){{
                        put("path", "/etc/shadow");
                        put("dynamic_again", new HashMap<String, Object>(){{
                                put("field", 1384790145.289);
                            }}
                        );
                    }}
        });
        refresh(node1.client());
        SQLResponse response = execute("select craty_field, strict_field, " +
                "no_dynamic_field from test12");
        assertEquals(1, response.rowCount());
        assertThat(response.rows()[0][0], instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> cratyMap = (Map<String, Object>)response.rows()[0][0];
        assertEquals(1384819200000L, cratyMap.get("created"));
        assertEquals(127, cratyMap.get("size"));

        assertThat(response.rows()[0][1], instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> strictMap = (Map<String, Object>)response.rows()[0][1];
        assertEquals("/dev/null", strictMap.get("path"));
        assertEquals(0, strictMap.get("created"));

        assertThat(response.rows()[0][2], instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> noDynamicMap = (Map<String, Object>)response.rows()[0][2];
        assertEquals("/etc/shadow", noDynamicMap.get("path"));
        assertEquals(
                new HashMap<String, Object>(){{ put("field", 1384790145289L); }},
                noDynamicMap.get("dynamic_again")
        );

        response = execute("select craty_field['created'], craty_field['size'], " +
                "no_dynamic_field['dynamic_again']['field'] from test12");
        assertEquals(1384819200000L, response.rows()[0][0]);
        assertEquals(127, response.rows()[0][1]);
        assertEquals(1384790145289L, response.rows()[0][2]);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInsertObjectField() throws Exception {

        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Nested Column Reference not allowes in INSERT statement");

        setUpCratyMapping();
        execute("insert into test12 (craty_field['size']) values (127)");

    }

    @Test
    public void testInvalidInsertIntoCraty() throws Exception {

        expectedException.expect(ValidationException.class);
        expectedException.expectMessage("Validation failed for craty_field.created: Invalid timestamp");

        setUpCratyMapping();
        execute("insert into test12 (craty_field, strict_field) values (?,?)", new Object[]{
                new HashMap<String, Object>(){{
                    put("created", true);
                    put("size", 127);
                }},
                new HashMap<String, Object>() {{
                    put("path", "/dev/null");
                    put("created", 0);
                }}

        });
    }

    @Test
    public void testWhereClause()  {
        setUpSimple();
        ParsedStatement stmt = parseService.parse("select * from t1 where " +
                "created='1970-01-01T00:00:00'");
        assertEquals(
                "{\"fields\":[\"checked\",\"created\",\"id\",\"size\",\"title\",\"weight\"]," +
                "\"query\":{\"term\":{\"created\":0}},\"size\":1000}",
                stmt.xcontent.toUtf8());
    }

    @Test
    public void testInvalidWhereClause() {

        expectedException.expect(ValidationException.class);
        expectedException.expectMessage("Validation failed for size: byte out of bounds");

        setUpSimple();
        ParsedStatement stmt = parseService.parse("delete from t1 where size=129");
    }

    @Test
    public void testInvalidWhereInWhereClause() {
        expectedException.expect(ValidationException.class);
        expectedException.expectMessage("Validation failed for size: invalid byte");

        setUpSimple();
        ParsedStatement stmt = parseService.parse("update t1 set size=0 where size='0'");
    }

    @Test
    public void testSetUpdate() {
        setUpSimple();

        execute("insert into t1 (id, size) values (0, 0)");
        execute("update t1 set created='1970-01-01T00:00:00' " +
                "where id=0");
        refresh(node1.client());

        SQLResponse response = execute("select created from t1 where id=0");
        assertEquals(1, response.rowCount());
        assertEquals(0, response.rows()[0][0]);
    }

}

package org.cratedb.integrationtests;

import org.cratedb.Constants;
import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.ColumnUnknownException;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.ValidationException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class SQLTypeMappingTest extends SQLTransportIntegrationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static SQLParseService parseService;

    @Before
    protected void beforeSQLTypeMappingTest() {
        parseService = cluster().getInstance(SQLParseService.class);
    }

    @AfterClass
    protected static void afterSQLTypeMappingTest() {
        parseService = null;
    }

    private void setUpSimple() throws IOException {
        setUpSimple(2);
    }

    private void setUpSimple(int numShards) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .startObject("_meta")
                    .field("primary_keys", "id")
                .endObject()
                .startObject("properties")
                    .startObject("id")
                        .field("type", "integer")
                        .field("index", "not_analyzed")
                    .endObject()
                    .startObject("string_field")
                        .field("type", "string")
                        .field("index", "not_analyzed")
                    .endObject()
                    .startObject("boolean_field")
                        .field("type", "boolean")
                        .field("index", "not_analyzed")
                    .endObject()
                    .startObject("byte_field")
                        .field("type", "byte")
                        .field("index", "not_analyzed")
                    .endObject()
                    .startObject("short_field")
                        .field("type", "short")
                        .field("index", "not_analyzed")
                    .endObject()
                    .startObject("integer_field")
                        .field("type", "integer")
                        .field("index", "not_analyzed")
                    .endObject()
                    .startObject("long_field")
                        .field("type", "long")
                        .field("index", "not_analyzed")
                    .endObject()
                    .startObject("float_field")
                        .field("type", "float")
                        .field("index", "not_analyzed")
                    .endObject()
                    .startObject("double_field")
                        .field("type", "double")
                        .field("index", "not_analyzed")
                    .endObject()
                    .startObject("timestamp_field")
                        .field("type", "date")
                        .field("index", "not_analyzed")
                    .endObject()
                    .startObject("craty_field")
                        .field("type", "object")
                        .startObject("properties")
                            .startObject("inner")
                                .field("type", "date")
                                .field("index", "not_analyzed")
                            .endObject()
                        .endObject()
                    .endObject()
                    .startObject("ip_field")
                        .field("type", "ip")
                        .field("index", "not_analyzed")
                    .endObject()
                .endObject()
                .endObject();

        client().admin().indices().prepareCreate("t1")
                .setSettings(ImmutableSettings.builder()
                        .put("number_of_replicas", 0)
                        .put("number_of_shards", numShards)
                        .put("index.mapper.map_source", false))
                .addMapping(Constants.DEFAULT_MAPPING_TYPE, builder)
                .execute().actionGet();
    }

    @Test
    public void testInsertAtNodeWithoutShard() throws Exception {
        setUpSimple(1);

        Iterator<Client> iterator = clients().iterator();
        Client client1 = iterator.next();
        Client client2 = iterator.next();

        client1.execute(SQLAction.INSTANCE, new SQLRequest(
            "insert into t1 (id, string_field, " +
                "timestamp_field, byte_field) values (?, ?, ?, ?)", new Object[]{1, "With",
            "1970-01-01T00:00:00", 127})).actionGet();

        client2.execute(SQLAction.INSTANCE, new SQLRequest(
            "insert into t1 (id, string_field, timestamp_field, byte_field) values (?, ?, ?, ?)",
            new Object[]{2, "Without", "1970-01-01T01:00:00", Byte.MIN_VALUE})).actionGet();
        refresh();
        SQLResponse response = execute("select id, string_field, timestamp_field, byte_field from t1 order by id");

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
        client().admin().indices().prepareCreate("test12")
                .setSettings(ImmutableSettings.builder()
                        .put("number_of_replicas", 0)
                        .put("number_of_shards", 2))
                .addMapping(Constants.DEFAULT_MAPPING_TYPE, builder)
                .execute().actionGet();
        refresh();
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
        refresh();

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
    public void testWhereClause() throws Exception {
        setUpSimple();
        ParsedStatement stmt = parseService.parse("select * from t1 where " +
                "timestamp_field='1970-01-01T00:00:00'");
        assertEquals(
                "{\"fields\":[\"boolean_field\",\"byte_field\",\"craty_field\",\"double_field\"," +
                        "\"float_field\",\"id\",\"integer_field\",\"ip_field\",\"long_field\"," +
                        "\"short_field\",\"string_field\",\"timestamp_field\"]," +
                        "\"query\":{\"term\":{\"timestamp_field\":0}},\"size\":10000}",
                stmt.xcontent.toUtf8());
    }

    @Test
    public void testInvalidWhereClause() throws Exception {

        expectedException.expect(ValidationException.class);
        expectedException.expectMessage("Validation failed for byte_field: Invalid byte: out of bounds");

        setUpSimple();
        ParsedStatement stmt = parseService.parse("delete from t1 where byte_field=129");
    }

    @Test
    public void testInvalidWhereInWhereClause() throws Exception {
        expectedException.expect(ValidationException.class);
        expectedException.expectMessage("Validation failed for byte_field: Invalid byte");

        setUpSimple();
        ParsedStatement stmt = parseService.parse("update t1 set byte_field=0 where byte_field in ('0')");
    }

    @Test
    public void testSetUpdate() throws Exception {
        setUpSimple();

        execute("insert into t1 (id, byte_field, short_field, integer_field, long_field, " +
                "float_field, double_field, boolean_field, string_field, timestamp_field," +
                "craty_field) values (?,?,?,?,?,?,?,?,?,?,?)", new Object[]{
                    0, 0, 0, 0, 0, 0.0, 1.0, false, "", "1970-01-01", new HashMap<String, Object>(){{ put("inner", "1970-01-01"); }}
                });
        execute("update t1 set " +
                "byte_field=?," +
                "short_field=?," +
                "integer_field=?," +
                "long_field=?," +
                "float_field=?," +
                "double_field=?," +
                "boolean_field=?," +
                "string_field=?," +
                "timestamp_field=?," +
                "craty_field=?," +
                "ip_field=?" +
                "where id=0", new Object[]{
                    Byte.MAX_VALUE, Short.MIN_VALUE, Integer.MAX_VALUE, Long.MIN_VALUE,
                    1.0, Math.PI, true, "a string", "2013-11-20",
                    new HashMap<String, Object>() {{put("inner", "2013-11-20");}}, "127.0.0.1"
        });
        refresh();

        SQLResponse response = execute("select id, byte_field, short_field, integer_field, long_field," +
                "float_field, double_field, boolean_field, string_field, timestamp_field," +
                "craty_field, ip_field from t1 where id=0");
        assertEquals(1, response.rowCount());
        assertEquals(0, response.rows()[0][0]);
        assertEquals(127, response.rows()[0][1]);
        assertEquals(-32768, response.rows()[0][2]);
        assertEquals(0x7fffffff, response.rows()[0][3]);
        assertEquals(0x8000000000000000L, response.rows()[0][4]);
        assertEquals(1.0, response.rows()[0][5]);
        assertEquals(Math.PI, response.rows()[0][6]);
        assertEquals(true, response.rows()[0][7]);
        assertEquals("a string", response.rows()[0][8]);
        assertEquals(1384905600000L, response.rows()[0][9]);
        assertEquals(new HashMap<String, Object>() {{ put("inner", 1384905600000L); }}, response.rows()[0][10]);
        assertEquals("127.0.0.1", response.rows()[0][11]);
    }

    @Test
    public void testGetRequestMapping() throws Exception {
        setUpSimple();
        execute("insert into t1 (id, string_field, boolean_field, byte_field, short_field, integer_field," +
                "long_field, float_field, double_field, craty_field," +
                "timestamp_field, ip_field) values " +
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", new Object[]{
                0, "Blabla", true, 120, 1000, 1200000,
                120000000000L, 1.4, 3.456789, new HashMap<String, Object>(){{put("inner", "1970-01-01");}},
                "1970-01-01", "127.0.0.1"
        });
        refresh();
        SQLResponse getResponse = execute("select * from t1 where id=0");
        SQLResponse searchResponse = execute("select * from t1 limit 1");
        for (int i=0; i < getResponse.rows()[0].length; i++) {
            assertThat(getResponse.rows()[0][i], is(searchResponse.rows()[0][i]));
        }
    }

    @Test
    public void testInsertNewColumn() throws Exception {
        setUpSimple();
        execute("insert into t1 (id, new_col) values (?,?)", new Object[]{0, "1970-01-01"});
        refresh();
        SQLResponse response = execute("select id, new_col from t1 where id=0");
        assertEquals(0, response.rows()[0][1]);
    }

    @Test
    public void testInsertNewCratyColumn() throws Exception {
        setUpSimple();
        execute("insert into t1 (id, new_col) values (?,?)", new Object[]{
                0,
                new HashMap<String, Object>(){{
                    put("a_date", "1970-01-01");
                    put("an_int", 127);
                    put("a_long", Long.MAX_VALUE);
                    put("a_boolean", true);
                }}
        });
        refresh();

        SQLResponse response = execute("select id, new_col from t1 where id=0");
        @SuppressWarnings("unchecked")
        Map<String, Object> mapped = (Map<String, Object>)response.rows()[0][1];
        assertEquals(0, mapped.get("a_date"));
        assertEquals(127, mapped.get("an_int"));
        assertEquals(0x7fffffffffffffffL, mapped.get("a_long"));
        assertEquals(true, mapped.get("a_boolean"));
    }

    @Test
    public void testInsertNewColumnToCraty() throws Exception {
        setUpCratyMapping();
        Map<String, Object> cratyContent = new HashMap<String, Object>(){{
            put("new_col", "a string");
            put("another_new_col", "1970-01-01T00:00:00");
        }};
        execute("insert into test12 (craty_field) values (?)",
                new Object[]{cratyContent});
        refresh();
        SQLResponse response = execute("select craty_field from test12");
        assertEquals(1, response.rowCount());
        @SuppressWarnings("unchecked")
        Map<String, Object> selectedCraty = (Map<String, Object>)response.rows()[0][0];

        assertThat((String)selectedCraty.get("new_col"), is("a string"));
        assertEquals(0, selectedCraty.get("another_new_col"));
    }

    @Test
    public void testInsertNewColumnToStrictCraty() throws Exception {

        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column unknown");

        setUpCratyMapping();
        Map<String, Object> strictContent = new HashMap<String, Object>(){{
            put("new_col", "a string");
            put("another_new_col", "1970-01-01T00:00:00");
        }};
        execute("insert into test12 (strict_field) values (?)",
                new Object[]{strictContent});
    }

    @Test
    public void testInsertNewColumnToNotDynamicCraty() throws Exception {

        setUpCratyMapping();
        Map<String, Object> notDynamicContent = new HashMap<String, Object>(){{
            put("new_col", "a string");
            put("another_new_col", "1970-01-01T00:00:00");
        }};
        execute("insert into test12 (no_dynamic_field) values (?)",
                new Object[]{notDynamicContent});
        refresh();
        SQLResponse response = execute("select no_dynamic_field from test12");
        assertEquals(1, response.rowCount());
        @SuppressWarnings("unchecked")
        Map<String, Object> selectedNoDynamic = (Map<String, Object>)response.rows()[0][0];
        // no mapping applied
        assertThat((String)selectedNoDynamic.get("new_col"), is("a string"));
        assertThat((String)selectedNoDynamic.get("another_new_col"), is("1970-01-01T00:00:00"));
    }

}

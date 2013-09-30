package org.cratedb.module.sql.test;

import com.google.common.collect.ImmutableSet;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryVisitorTest {

    private ParsedStatement stmt;

    @Test(expected = SQLParseException.class)
    public void testUnsupportedStatement() throws StandardException, IOException {
        execStatement("explain select * from x");
    }

    private String getSource() throws StandardException {
        return stmt.buildSearchRequest().source().toUtf8();
    }

    private ParsedStatement execStatement(String stmt) throws StandardException {
        return execStatement(stmt, new Object[]{});
    }

    private ParsedStatement execStatement(String sql, Object[] args) throws StandardException {
        NodeExecutionContext nec = mock(NodeExecutionContext.class);
        NodeExecutionContext.TableExecutionContext tec = mock(
                NodeExecutionContext.TableExecutionContext.class);
        when(nec.tableContext("locations")).thenReturn(tec);
        when(tec.allCols()).thenReturn(ImmutableSet.of("a", "b"));
        stmt = new ParsedStatement(sql, args, nec);
        return stmt;
    }

    @Test(expected = SQLParseException.class)
    public void testStatementWithUnsupportedNode() throws StandardException, IOException {
        execStatement(
                "select * from locations inner join planets on planets.name = locations.name");
    }

    @Test(expected = SQLParseException.class)
    public void testUnsupportedExistsClause() throws StandardException, IOException {
        execStatement("select * from locations where exists (select 1 from locations)");
    }

    @Test(expected = SQLParseException.class)
    public void testSelectWithNumericConstantValue() throws StandardException, IOException {
        execStatement("select 1 from locations");
    }

    @Test(expected = SQLParseException.class)
    public void testSelectWithCharConstantValue() throws StandardException, IOException {
        execStatement("select 'name' from locations");
    }

    @Test
    public void testSelectAllFromTable() throws StandardException, IOException {
        execStatement("select * from locations");
        stmt.buildSearchRequest().source().toString();
        assertEquals("{\"query\":{\"match_all\":{}},\"fields\":[\"a\",\"b\"]}",
                stmt.buildSearchRequest().source().toUtf8());
    }

    @Test
    public void testSelectWithLimitAsParameter() throws Exception {
        execStatement("SELECT name from locations limit ?", new Object[]{5});
    }

    @Test
    public void testSelectWithLimitAsOffset() throws Exception {
        execStatement("SELECT name from locations limit 1 offset ?", new Object[]{5});
    }

    @Test
    public void testSelectWithFieldAs() throws StandardException, IOException {

        execStatement("select name as n from locations");
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .field("match_all", new HashMap())
                        .endObject()
                        .field("fields", Arrays.asList("name"))
                        .endObject()
                        .string();
        stmt.outputFields();

        assertEquals(expected, getSource());
        assertEquals("n", stmt.outputFields().get(0).v1());
        assertEquals("name", stmt.outputFields().get(0).v2());
    }

    @Test
    public void testSelectVersion() throws StandardException, IOException {

        execStatement("select \"_version\" from locations");
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .field("match_all", new HashMap())
                        .endObject()
                        .field("version", true)
                        .endObject()
                        .string();

        assertEquals(expected, getSource());
    }

    @Test
    public void testSelectAllAndFieldFromTable() throws StandardException, IOException {
        execStatement("select *, name from locations");
        assertEquals("{\"query\":{\"match_all\":{}},\"fields\":[\"a\",\"b\",\"name\"]}",
                getSource());
    }

    @Test
    public void testSelectWithLimit() throws StandardException, IOException {
        execStatement("select * from locations limit 5");
        assertEquals("{\"query\":{\"match_all\":{}},\"fields\":[\"a\",\"b\"],\"size\":5}",
                getSource());
    }

    @Test
    public void testSelectWithLimitAndOffset() throws StandardException, IOException {
        execStatement("select * from locations limit 5 offset 3");
        assertEquals(
                "{\"query\":{\"match_all\":{}},\"fields\":[\"a\",\"b\"],\"from\":3,\"size\":5}",
                getSource());
    }

    @Test
    public void testSelectWithOrderBy() throws StandardException, IOException {

        execStatement("select * from locations order by kind");
        assertEquals(
                "{\"sort\":[{\"kind\":{\"order\":\"asc\",\"ignore_unmapped\":true}}]," +
                        "\"query\":{\"match_all\":{}},\"fields\":[\"a\"," +
                        "\"b\"]}", getSource());
    }

    @Test
    public void testSelectWithMultipleOrderBy() throws StandardException, IOException {

        execStatement("select * from locations order by kind asc, name desc");
        assertEquals(
                "{\"sort\":[{\"kind\":{\"order\":\"asc\",\"ignore_unmapped\":true}}," +
                        "{\"name\":{\"order\":\"desc\"," +
                        "\"ignore_unmapped\":true}}],\"query\":{\"match_all\":{}}," +
                        "\"fields\":[\"a\",\"b\"]}",
                getSource());
    }

    @Test
    public void testSelectFieldsFromTable() throws StandardException, IOException {

        execStatement("select name, kind from locations");
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .field("match_all", new HashMap())
                        .endObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .endObject()
                        .string();

        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseToTermsQuery() throws StandardException, IOException {
        execStatement("select name, kind from locations where name = 'Bartledan'");
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("term").field("name", "Bartledan").endObject()
                        .endObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .endObject()
                        .string();
        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseToTermsQueryWithUnderscoreField() throws StandardException,
            IOException {
        execStatement("select name, kind from locations where \"_id\" = 1");
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("term").field("_id", 1).endObject()
                        .endObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .endObject()
                        .string();
        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseWithNotEqual() throws StandardException, IOException {
        execStatement("select name, kind from locations where position != 1");
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("bool")
                        .startObject("must_not")
                        .startObject("term").field("position", 1).endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .endObject()
                        .string();
        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseWithIsNull() throws StandardException, IOException {
        execStatement("select name from locations where name is null");

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("filtered")
                        .startObject("filter")
                        .startObject("missing")
                        .field("field", "name")
                        .field("existence", true)
                        .field("null_value", true)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .field("fields", Arrays.asList("name"))
                        .endObject()
                        .string();

        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseWithIsNotNull() throws StandardException, IOException {
        execStatement("select name from locations where name is not null");

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("bool")
                        .startObject("must_not")
                        .startObject("filtered")
                        .startObject("filter")
                        .startObject("missing")
                        .field("field", "name")
                        .field("existence", true)
                        .field("null_value", true)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .field("fields", Arrays.asList("name"))
                        .endObject()
                        .string();

        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseWithNotEqualLtGtSyntax() throws StandardException, IOException {
        execStatement("select name, kind from locations where position <> 1");

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("bool")
                        .startObject("must_not")
                        .startObject("term").field("position", 1).endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .endObject()
                        .string();
        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseToTermsQueryWithDateField() throws StandardException, IOException {
        execStatement("select name, kind from locations where date = '2013-07-16'");

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("term").field("date", "2013-07-16").endObject()
                        .endObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .endObject()
                        .string();
        assertEquals(expected, getSource());
    }
    @Test
    public void testWhereClauseToTermsQueryWithNumberField() throws StandardException,
            IOException {
        execStatement("select * from locations where position = 4");

        assertEquals("{\"query\":{\"term\":{\"position\":4}},\"fields\":[\"a\",\"b\"]}",
                getSource());
    }
    @Test
    public void testWhereClauseWithYodaCondition() throws StandardException, IOException {
        execStatement("select name, kind from locations where 'Bartledan' = name");

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("term").field("name", "Bartledan").endObject()
                        .endObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .endObject()
                        .string();

        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseWithOneAnd() throws StandardException, IOException {
        execStatement("select name, kind from locations where 'Bartledan' = name and kind = " +
                "'Planet'");
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("bool")
                        .field("minimum_should_match", 1)
                        .startArray("must")
                        .startObject().startObject("term").field("name",
                        "Bartledan").endObject().endObject()
                        .startObject().startObject("term").field("kind",
                        "Planet").endObject().endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .endObject()
                        .string();

        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseWithOneOr() throws StandardException, IOException {
        execStatement("select name, kind from locations where kind = 'Galaxy' or kind = 'Planet'");
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("bool")
                        .field("minimum_should_match", 1)
                        .startArray("should")
                        .startObject().startObject("term").field("kind",
                        "Galaxy").endObject().endObject()
                        .startObject().startObject("term").field("kind",
                        "Planet").endObject().endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .endObject()
                        .string();

        String actual = getSource();
        assertEquals(expected, actual);
    }

    @Test
    public void testWhereClauseWithManyOr() throws StandardException, IOException {
        execStatement(
                "select name, kind from locations where kind = 'Galaxy' or kind = 'Planet' " +
                        "or kind = 'x' or kind = 'y'");
        // this query could be optimized in either an terms query or a simplified bool query
        // without the nesting.
        // but the SQL-Syntax Tree's structure makes this format easier to generate and ES
        // optimized this probably anyway later on.

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("bool")
                        .field("minimum_should_match", 1)
                        .startArray("should")
                        .startObject()
                        .startObject("bool")
                        .field("minimum_should_match", 1)
                        .startArray("should")
                        .startObject()
                        .startObject("bool")
                        .field("minimum_should_match", 1)
                        .startArray("should")
                        .startObject().startObject("term").field("kind",
                        "Galaxy").endObject().endObject()
                        .startObject().startObject("term").field("kind",
                        "Planet").endObject().endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .startObject().startObject("term").field("kind",
                        "x").endObject().endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .startObject().startObject("term").field("kind",
                        "y").endObject().endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .endObject()
                        .string();

        String actual = getSource();
        assertEquals(expected, actual);
    }
    @Test
    public void testWhereClauseWithOrAndNestedAnd() throws StandardException, IOException {
        execStatement("select name, kind from locations where name = 'Bartledan' or (kind = " +
                "'Planet' and \"_id\" = '11')");
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("bool")
                        .field("minimum_should_match", 1)
                        .startArray("should")
                        .startObject()
                        .startObject("term").field("name", "Bartledan").endObject()
                        .endObject()
                        .startObject()
                        .startObject("bool")
                        .field("minimum_should_match", 1)
                        .startArray("must")
                        .startObject().startObject("term").field("kind",
                        "Planet").endObject().endObject()
                        .startObject().startObject("term").field("_id",
                        "11").endObject().endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .endObject()
                        .string();

        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseWithAndAndGreaterThan() throws StandardException, IOException {
        execStatement("select name, kind from locations where kind = 'Planet' and \"_id\" > '4'");
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("bool")
                        .field("minimum_should_match", 1)
                        .startArray("must")
                        .startObject().startObject("term").field("kind",
                        "Planet").endObject().endObject()
                        .startObject()
                        .startObject("range")
                        .startObject("_id")
                        .field("gt", "4")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .endObject()
                        .string();

        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseToRangeQueryWithGtNumberField() throws StandardException,
            IOException {
        execStatement("select * from locations where position > 4");
        assertEquals("{\"query\":{\"range\":{\"position\":{\"gt\":4}}},\"fields\":[\"a\",\"b\"]}",
                getSource());
    }

    @Test
    public void testWhereClauseToRangeQueryWithGteNumberField() throws StandardException,
            IOException {
        execStatement("select * from locations where position >= 4");
        assertEquals("{\"query\":{\"range\":{\"position\":{\"gte\":4}}},\"fields\":[\"a\",\"b\"]}",
                getSource());
    }

    @Test
    public void testWhereClauseToRangeQueryWithGtNumberFieldYoda() throws StandardException,
            IOException {
        execStatement("select * from locations where 4 < position");
        assertEquals("{\"query\":{\"range\":{\"position\":{\"gt\":4}}},\"fields\":[\"a\",\"b\"]}",
                getSource());
    }

    @Test
    public void testWhereClauseToRangeQueryWithLteNumberField() throws StandardException,
            IOException {
        execStatement("select * from locations where position <= 4");
        assertEquals("{\"query\":{\"range\":{\"position\":{\"lte\":4}}},\"fields\":[\"a\",\"b\"]}",
                getSource());
    }

    @Test
    public void testDeleteQuery() throws Exception {
        execStatement("delete from locations where 4 < position");
        assertEquals("{\"range\":{\"position\":{\"gt\":4}}}",
                getSource());
    }


    @Test
    public void testUpdate() throws Exception {
        execStatement("update locations set a=? where a=2", new Object[]{1});
        assertEquals("{\"query\":{\"term\":{\"a\":2}},\"facets\":{\"sql\":{\"stmt\":\"update " +
                "locations set a=? where a=2\",\"args\":[1]}}}", getSource());
    }

    @Test
    public void testUpdateDocWithArgs() throws Exception {
        execStatement("update locations set a=? where a=2", new Object[]{1});
        Map<String, Object> expected = new HashMap<String, Object>(1);
        expected.put("a", 1);
        assertEquals(expected, stmt.updateDoc());
    }

    @Test
    public void testUpdateDocWith2Args() throws Exception {
        execStatement("update locations set a=?,b=? where a=2", new Object[]{1,2});
        Map<String, Object> expected = new HashMap<String, Object>(1);
        expected.put("a", 1);
        expected.put("b", 2);
        assertEquals(expected, stmt.updateDoc());
    }


    @Test
    public void testUpdateDocWithConstant() throws Exception {
        execStatement("update locations set a=1 where a=2");
        Map<String, Object> expected = new HashMap<String, Object>(1);
        // NOTE: this test does result in a null to be generated because the mock does not
        // mock the mapper - but it shows that the logic goes another path in this case
        expected.put("a", null);
        assertEquals(expected, stmt.updateDoc());

    }


}

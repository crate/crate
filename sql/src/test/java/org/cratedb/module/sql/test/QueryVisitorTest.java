package org.cratedb.module.sql.test;

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.SQLParserContext;
import com.akiban.sql.parser.StatementNode;
import com.google.common.collect.ImmutableSet;
import org.cratedb.action.parser.QueryVisitor;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.sql.SQLParseException;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryVisitorTest {

    @Test(expected = SQLParseException.class)
    public void testUnsupportedStatement() throws StandardException, IOException {
        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "update locations set name = 'Restaurant at the end of the Galaxy'";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);
    }

    private QueryVisitor getQueryVisitor() throws StandardException {

        NodeExecutionContext nec = mock(NodeExecutionContext.class);
        NodeExecutionContext.TableExecutionContext tec = mock(
                NodeExecutionContext.TableExecutionContext.class);
        when(nec.tableContext("locations")).thenReturn(tec);
        when(tec.allCols()).thenReturn(ImmutableSet.of("a", "b"));

        return new QueryVisitor(nec);
    }

    @Test(expected = SQLParseException.class)
    public void testStatementWithUnsupportedNode() throws StandardException, IOException {
        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select * from locations inner join planets on planets.name = locations.name";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);
    }

    @Test(expected = SQLParseException.class)
    public void testUnsupportedExistsClause() throws StandardException, IOException {
        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select * from locations where exists (select 1 from locations)";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);
    }

    @Test(expected = SQLParseException.class)
    public void testSelectWithNumericConstantValue() throws StandardException, IOException {

        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select 1 from locations";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);
    }

    @Test(expected = SQLParseException.class)
    public void testSelectWithCharConstantValue() throws StandardException, IOException {

        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select 'name' from locations";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);
    }

    @Test
    public void testSelectAllFromTable() throws StandardException, IOException {

        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select * from locations";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

        assertEquals("{\"query\":{\"match_all\":{}},\"fields\":[\"a\",\"b\"]}",
                visitor.getXContentBuilder().string());
    }

    @Test
    public void testSelectWithFieldAs() throws StandardException, IOException {

        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select name as n from locations";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .field("match_all", new HashMap())
                        .endObject()
                        .field("fields", Arrays.asList("name"))
                        .endObject()
                        .string();

        assertEquals(expected, visitor.getXContentBuilder().string());
        assertEquals("n", visitor.outputFields().get(0).v1());
        assertEquals("name", visitor.outputFields().get(0).v2());
    }

    @Test
    public void testSelectVersion() throws StandardException, IOException {
        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select \"_version\" from locations";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .field("match_all", new HashMap())
                        .endObject()
                        .field("version", true)
                        .endObject()
                        .string();

        assertEquals(expected, visitor.getXContentBuilder().string());
    }

    @Test
    public void testSelectAllAndFieldFromTable() throws StandardException, IOException {

        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select *, name from locations";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

        assertEquals("{\"query\":{\"match_all\":{}},\"fields\":[\"a\",\"b\",\"name\"]}",
                visitor.getXContentBuilder().string());
    }

    @Test
    public void testSelectWithLimit() throws StandardException, IOException {

        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select * from locations limit 5";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

        assertEquals("{\"query\":{\"match_all\":{}},\"fields\":[\"a\",\"b\"],\"size\":5}",
                visitor.getXContentBuilder().string());
    }

    @Test
    public void testSelectWithLimitAndOffset() throws StandardException, IOException {

        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select * from locations limit 5 offset 3";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

        assertEquals(
                "{\"query\":{\"match_all\":{}},\"fields\":[\"a\",\"b\"],\"from\":3,\"size\":5}",
                visitor.getXContentBuilder().string());
    }

    @Test
    public void testSelectWithOrderBy() throws StandardException, IOException {

        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select * from locations order by kind";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

        assertEquals(
                "{\"sort\":[{\"kind\":{\"order\":\"asc\",\"ignore_unmapped\":true}}]," +
                        "\"query\":{\"match_all\":{}},\"fields\":[\"a\"," +
                        "\"b\"]}", visitor.getXContentBuilder().string());
    }

    @Test
    public void testSelectWithMultipleOrderBy() throws StandardException, IOException {

        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select * from locations order by kind asc, name desc";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

        assertEquals(
                "{\"sort\":[{\"kind\":{\"order\":\"asc\",\"ignore_unmapped\":true}}," +
                        "{\"name\":{\"order\":\"desc\"," +
                        "\"ignore_unmapped\":true}}],\"query\":{\"match_all\":{}}," +
                        "\"fields\":[\"a\",\"b\"]}",
                visitor.getXContentBuilder().string());
    }

    @Test
    public void testSelectFieldsFromTable() throws StandardException, IOException {

        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select name, kind from locations";
        StatementNode statement = parser.parseStatement(sql);
        statement.accept(visitor);

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .field("match_all", new HashMap())
                        .endObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .endObject()
                        .string();

        assertEquals(expected, visitor.getXContentBuilder().string());
    }

    @Test
    public void testWhereClauseToTermsQuery() throws StandardException, IOException {
        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select name, kind from locations where name = 'Bartledan'";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("term").field("name", "Bartledan").endObject()
                        .endObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .endObject()
                        .string();


        assertEquals(expected, visitor.getXContentBuilder().string());
    }

    @Test
    public void testWhereClauseToTermsQueryWithUnderscoreField() throws StandardException,
            IOException {
        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select name, kind from locations where \"_id\" = 1";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("term").field("_id", 1).endObject()
                        .endObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .endObject()
                        .string();


        assertEquals(expected, visitor.getXContentBuilder().string());
    }

    @Test
    public void testWhereClauseWithNotEqual() throws StandardException, IOException {
        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select name, kind from locations where position != 1";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

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


        assertEquals(expected, visitor.getXContentBuilder().string());
    }

    @Test
    public void testWhereClauseWithIsNull() throws StandardException, IOException {
        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select name from locations where name is null";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

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

        assertEquals(expected, visitor.getXContentBuilder().string());
    }

    @Test
    public void testWhereClauseWithIsNotNull() throws StandardException, IOException {
        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select name from locations where name is not null";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

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

        assertEquals(expected, visitor.getXContentBuilder().string());
    }

    @Test
    public void testWhereClauseWithNotEqualLtGtSyntax() throws StandardException, IOException {
        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select name, kind from locations where position <> 1";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

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


        assertEquals(expected, visitor.getXContentBuilder().string());
    }

    @Test
    public void testWhereClauseToTermsQueryWithDateField() throws StandardException, IOException {
        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select name, kind from locations where date = '2013-07-16'";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("term").field("date", "2013-07-16").endObject()
                        .endObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .endObject()
                        .string();


        assertEquals(expected, visitor.getXContentBuilder().string());
    }


    @Test
    public void testWhereClauseToTermsQueryWithNumberField() throws StandardException,
            IOException {
        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select * from locations where position = 4";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

        assertEquals("{\"query\":{\"term\":{\"position\":4}},\"fields\":[\"a\",\"b\"]}",
                visitor.getXContentBuilder().string());
    }


    @Test
    public void testWhereClauseWithYodaCondition() throws StandardException, IOException {
        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select name, kind from locations where 'Bartledan' = name";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("term").field("name", "Bartledan").endObject()
                        .endObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .endObject()
                        .string();

        assertEquals(expected, visitor.getXContentBuilder().string());
    }

    @Test
    public void testWhereClauseWithOneAnd() throws StandardException, IOException {

        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select name, kind from locations where 'Bartledan' = name and kind = " +
                "'Planet'";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

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

        assertEquals(expected, visitor.getXContentBuilder().string());
    }

    @Test
    public void testWhereClauseWithOneOr() throws StandardException, IOException {

        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select name, kind from locations where kind = 'Galaxy' or kind = 'Planet'";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

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

        String actual = visitor.getXContentBuilder().string();
        assertEquals(expected, actual);
    }

    @Test
    public void testWhereClauseWithManyOr() throws StandardException, IOException {

        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select name, kind from locations where kind = 'Galaxy' or kind = 'Planet' " +
                "or kind = 'x' or kind = 'y'";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

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

        String actual = visitor.getXContentBuilder().string();
        assertEquals(expected, actual);
    }


    @Test
    public void testWhereClauseWithOrAndNestedAnd() throws StandardException, IOException {
        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select name, kind from locations where name = 'Bartledan' or (kind = " +
                "'Planet' and \"_id\" = '11')";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

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

        assertEquals(expected, visitor.getXContentBuilder().string());
    }

    @Test
    public void testWhereClauseWithAndAndGreaterThan() throws StandardException, IOException {
        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select name, kind from locations where kind = 'Planet' and \"_id\" > '4'";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

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

        assertEquals(expected, visitor.getXContentBuilder().string());
    }

    @Test
    public void testWhereClauseToRangeQueryWithGtNumberField() throws StandardException,
            IOException {
        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select * from locations where position > 4";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

        assertEquals("{\"query\":{\"range\":{\"position\":{\"gt\":4}}},\"fields\":[\"a\",\"b\"]}",
                visitor.getXContentBuilder().string());
    }

    @Test
    public void testWhereClauseToRangeQueryWithGteNumberField() throws StandardException,
            IOException {
        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select * from locations where position >= 4";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

        assertEquals("{\"query\":{\"range\":{\"position\":{\"gte\":4}}},\"fields\":[\"a\",\"b\"]}",
                visitor.getXContentBuilder().string());
    }

    @Test
    public void testWhereClauseToRangeQueryWithGtNumberFieldYoda() throws StandardException,
            IOException {
        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select * from locations where 4 < position";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

        assertEquals("{\"query\":{\"range\":{\"position\":{\"gt\":4}}},\"fields\":[\"a\",\"b\"]}",
                visitor.getXContentBuilder().string());
    }

    @Test
    public void testWhereClauseToRangeQueryWithLteNumberField() throws StandardException, IOException {
        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "select * from locations where position <= 4";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);

        assertEquals("{\"query\":{\"range\":{\"position\":{\"lte\":4}}},\"fields\":[\"a\",\"b\"]}",
                visitor.getXContentBuilder().string());
    }

    @Test
    public void testDeleteQuery() throws Exception {
        QueryVisitor visitor = getQueryVisitor();
        SQLParser parser = new SQLParser();
        String sql = "delete from locations where 4 < position";
        StatementNode statement = parser.parseStatement(sql);

        statement.accept(visitor);
        assertEquals("{\"range\":{\"position\":{\"gt\":4}}}",
            visitor.getXContentBuilder().string());
    }
}

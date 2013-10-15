package org.cratedb.module.sql.test;

import com.google.common.collect.ImmutableSet;
import org.cratedb.action.parser.QueryPlanner;
import org.cratedb.action.parser.XContentGenerator;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.parser.StandardException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryPlannerTest {

    private ParsedStatement stmt;

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
        // Force enabling query planner
        Settings settings = mock(ImmutableSettings.class);
        when(settings.getAsBoolean(QueryPlanner.SETTINGS_OPTIMIZE_PK_QUERIES,
                true)).thenReturn(true);
        QueryPlanner queryPlanner = new QueryPlanner(settings);
        when(nec.queryPlanner()).thenReturn(queryPlanner);
        when(nec.tableContext("phrases")).thenReturn(tec);
        when(tec.allCols()).thenReturn(ImmutableSet.of("pk_col", "phrase"));
        when(tec.isRouting("pk_col")).thenReturn(true);
        when(tec.primaryKeys()).thenReturn(new ArrayList<String>(1) {{
            add("pk_col");
        }});
        when(tec.primaryKeysIncludingDefault()).thenReturn(new ArrayList<String>(1) {{
            add("pk_col");
        }});
        stmt = new ParsedStatement(sql, args, nec);
        return stmt;
    }

    @Test
    public void testSelectWherePrimaryKey() throws Exception {
        execStatement("select pk_col, phrase from phrases where pk_col=?", new Object[]{1});
        assertEquals(ParsedStatement.GET_ACTION, stmt.type());
        assertEquals("1", stmt.getPlannerResult(QueryPlanner.PRIMARY_KEY_VALUE));
        assertEquals(1, stmt.plannerResults().size());
    }

    @Test
    public void testDeleteWherePrimaryKey() throws Exception {
        execStatement("delete from phrases where ?=pk_col", new Object[]{1});
        assertEquals(ParsedStatement.DELETE_ACTION, stmt.type());
        assertEquals("1", stmt.getPlannerResult(QueryPlanner.PRIMARY_KEY_VALUE));
        assertEquals(1, stmt.plannerResults().size());
    }

    @Test
    public void testUpdateWherePrimaryKey() throws Exception {
        execStatement("update phrases set phrase=? where pk_col=?",
                new Object[]{"don't panic", 1});
        assertEquals(ParsedStatement.UPDATE_ACTION, stmt.type());
        assertEquals("1", stmt.getPlannerResult(QueryPlanner.PRIMARY_KEY_VALUE));
        assertEquals(1, stmt.plannerResults().size());
    }

    @Test
    public void testSelectWherePrimaryKeyAnd() throws StandardException, IOException {
        execStatement("select pk_col, phrase from phrases where pk_col=? and phrase = ?",
                new Object[]{1, "don't panic"});
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("fields", Arrays.asList("pk_col", "phrase"))
                        .startObject("query")
                        .startObject("bool")
                        .field("minimum_should_match", 1)
                        .startArray("must")
                        .startObject().startObject("term").field("pk_col",
                        1).endObject().endObject()
                        .startObject().startObject("term").field("phrase",
                        "don't panic").endObject().endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .field("size", XContentGenerator.DEFAULT_SELECT_LIMIT)
                        .endObject()
                        .string();

        assertEquals(expected, getSource());
        assertEquals(ParsedStatement.SEARCH_ACTION, stmt.type());
        @SuppressWarnings("unchecked")
        Set<String> routingValues = (Set<String>) stmt.getPlannerResult(QueryPlanner.ROUTING_VALUES);
        assertThat(routingValues.contains("1"), is(true));
        assertThat(routingValues.size(), is(1));
        assertEquals("1", stmt.buildSearchRequest().routing());
        assertEquals(1, stmt.plannerResults().size());
    }

    @Test
    public void testSelectWherePrimaryKeyNestedAnd() throws StandardException, IOException {
        execStatement("select pk_col, phrase from phrases where author=? and (phrase = ? and " +
                "pk_col = ?)",
                new Object[]{"Ford", "don't panic", 1});
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("fields", Arrays.asList("pk_col", "phrase"))
                        .startObject("query")
                        .startObject("bool")
                        .field("minimum_should_match", 1)
                        .startArray("must")
                        .startObject().startObject("term").field("author",
                        "Ford").endObject().endObject()
                        .startObject().startObject("bool")
                        .field("minimum_should_match", 1)
                        .startArray("must")
                        .startObject().startObject("term").field("phrase",
                        "don't panic").endObject().endObject()
                        .startObject().startObject("term").field("pk_col",
                        1).endObject().endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .field("size", XContentGenerator.DEFAULT_SELECT_LIMIT)
                        .endObject()
                        .string();

        assertEquals(expected, getSource());
        assertEquals(ParsedStatement.SEARCH_ACTION, stmt.type());
        @SuppressWarnings("unchecked")
        Set<String> routingValues = (Set<String>) stmt.getPlannerResult(QueryPlanner.ROUTING_VALUES);
        assertThat(routingValues.contains("1"), is(true));
        assertEquals("1", stmt.buildSearchRequest().routing());
        assertEquals(1, stmt.plannerResults().size());
    }

    @Test
    public void testDeleteWherePrimaryKeyAnd() throws StandardException, IOException {
        execStatement("delete from phrases where pk_col=? and phrase = ?",
                new Object[]{1, "don't panic"});

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject().startObject("bool")
                        .field("minimum_should_match", 1)
                        .startArray("must")
                        .startObject().startObject("term").field("pk_col",
                        1).endObject().endObject()
                        .startObject().startObject("term").field("phrase",
                        "don't panic").endObject().endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .string();

        assertEquals(ParsedStatement.DELETE_BY_QUERY_ACTION, stmt.type());

        assertEquals("[[phrases]][[]], querySource["+expected+"]",
                stmt.buildDeleteByQueryRequest().toString());

        @SuppressWarnings("unchecked")
        Set<String> routingValues = (Set<String>) stmt.getPlannerResult(QueryPlanner.ROUTING_VALUES);
        assertThat(routingValues.contains("1"), is(true));
        assertEquals("1", stmt.buildDeleteByQueryRequest().routing());
        assertEquals(1, stmt.plannerResults().size());
    }

    @Test
    public void testUpdateWherePrimaryKeyAnd() throws StandardException, IOException {
        execStatement("update phrases set phrase = ? where pk_col=? and phrase = ?",
                new Object[]{"don't panic, don't panic", 1, "don't panic"});

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("bool")
                        .field("minimum_should_match", 1)
                        .startArray("must")
                        .startObject().startObject("term").field("pk_col",
                        1).endObject().endObject()
                        .startObject().startObject("term").field("phrase",
                        "don't panic").endObject().endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .startObject("facets")
                        .startObject("sql")
                        .startObject("sql")
                        .field("stmt", "update phrases set phrase = ? where pk_col=? and phrase = ?")
                        .startArray("args")
                        .value("don't panic, don't panic")
                        .value(1)
                        .value("don't panic")
                        .endArray()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .string();

        assertEquals(ParsedStatement.SEARCH_ACTION, stmt.type());

        assertEquals(expected, getSource());

        @SuppressWarnings("unchecked")
        Set<String> routingValues = (Set<String>) stmt.getPlannerResult(QueryPlanner.ROUTING_VALUES);
        assertThat(routingValues.contains("1"), is(true));
        assertEquals("1", stmt.buildSearchRequest().routing());
        assertEquals(1, stmt.plannerResults().size());
    }

    @Test
    public void testSelectMultiplePrimaryKeysSimpleOr() throws StandardException {
        execStatement("SELECT pk_col, phrase FROM phrases WHERE pk_col=? OR pk_col=?", new Object[]{"1", "2"});
        @SuppressWarnings("unchecked")
        Set<String> primaryKeyValues = (Set<String>)stmt.getPlannerResult(QueryPlanner.ROUTING_VALUES);
        assertThat(primaryKeyValues, is(notNullValue()));
        assertThat(primaryKeyValues, hasItems("1", "2"));
        assertEquals(1, stmt.plannerResults().size());
        assertThat(stmt.type(), is(ParsedStatement.SEARCH_ACTION));
    }

    @Test
    public void testSelectMultiplePrimaryKeysDoubleOr() throws StandardException {
        execStatement("SELECT * FROM phrases WHERE pk_col=? OR pk_col=? OR pk_col=?", new Object[]{"foo", "bar", "baz"});
        @SuppressWarnings("unchecked")
        Set<String> primaryKeyValues = (Set<String>)stmt.getPlannerResult(QueryPlanner.ROUTING_VALUES);
        assertThat(primaryKeyValues, is(notNullValue()));
        assertThat(primaryKeyValues, hasItems("foo", "bar", "baz"));
        assertEquals(1, stmt.plannerResults().size());
        assertThat(stmt.type(), is(ParsedStatement.SEARCH_ACTION));
    }

    @Test
    public void testSelectMultiplePrimaryKeysNestedOr() throws StandardException {
        execStatement("SELECT * FROM phrases WHERE (pk_col=? OR pk_col=?) OR (pk_col=? OR (pk_col=? OR pk_col=?))",
                new Object[]{"TinkyWinky", "Dipsy", "Lala", "Po", "Hallo"});
        @SuppressWarnings("unchecked")
        Set<String> primaryKeyValues = (Set<String>)stmt.getPlannerResult(QueryPlanner.ROUTING_VALUES);
        assertThat(primaryKeyValues, is(notNullValue()));
        assertThat(primaryKeyValues, hasItems("TinkyWinky", "Dipsy", "Lala", "Po", "Hallo"));
        assertEquals(1, stmt.plannerResults().size());
        assertThat(stmt.type(), is(ParsedStatement.SEARCH_ACTION));
    }

    @Test
    public void testSelectMultiplePrimaryKeysInvalid() throws StandardException {
        execStatement("SELECT * FROM phrases WHERE pk_col=? OR phrase=?",
                new Object[]{"in", "valid"});
        @SuppressWarnings("unchecked")
        Set<String> primaryKeyValues = (Set<String>)stmt.getPlannerResult(QueryPlanner.ROUTING_VALUES);
        assertThat(primaryKeyValues, is(nullValue()));
    }

    @Test
    public void testSelectMultiplePrimaryKeysInvalidWithAnd() throws StandardException {
        execStatement("SELECT * FROM phrases WHERE pk_col=? OR (pk_col=? AND pk_col=?)",
                new Object[]{"still", "in", "valid"});
        @SuppressWarnings("unchecked")
        Set<String> primaryKeyValues = (Set<String>)stmt.getPlannerResult(QueryPlanner.ROUTING_VALUES);
        assertThat(primaryKeyValues, is(nullValue()));
    }

    @Test
    public void testSelectMultiplePrimaryKeysInvalidNested() throws StandardException {
        execStatement("SELECT * FROM phrases WHERE (pk_col=? OR pk_col=?) OR (pk_col=? OR (phrase=? OR pk_col=?))",
                new Object[]{"in", "va", "lid", "ne", "sted"});
        assertThat(stmt.getPlannerResult(QueryPlanner.ROUTING_VALUES), is(nullValue()));
    }

    @Test
    public void testSelectMultiplePrimaryKeysWhereIn() throws StandardException {
        execStatement("SELECT * FROM phrases WHERE pk_col IN (?, ?, ?)",
                new Object[]{"foo", "bar", "baz"});
        @SuppressWarnings("unchecked")
        Set<String> primaryKeyValues = (Set<String>)stmt.getPlannerResult(QueryPlanner.ROUTING_VALUES);
        assertThat(primaryKeyValues, is(notNullValue()));
        assertThat(primaryKeyValues, hasItems("foo", "bar", "baz"));
        assertEquals(1, stmt.plannerResults().size());
        assertThat(stmt.type(), is(ParsedStatement.SEARCH_ACTION));
    }

    @Test
    public void testSelectMultiplePrimaryKeysWhereInAndOr() throws StandardException {
        execStatement("SELECT * FROM phrases WHERE pk_col IN (?, ?, ?) OR pk_col=?",
                new Object[]{"foo", "bar", "baz", "dunno"});
        @SuppressWarnings("unchecked")
        Set<String> primaryKeyValues = (Set<String>)stmt.getPlannerResult(QueryPlanner.ROUTING_VALUES);
        assertThat(primaryKeyValues, is(notNullValue()));
        assertThat(primaryKeyValues, hasItems("foo", "bar", "baz", "dunno"));
        assertEquals(1, stmt.plannerResults().size());
        assertThat(stmt.type(), is(ParsedStatement.SEARCH_ACTION));

        execStatement("SELECT * FROM phrases WHERE pk_col=? OR pk_col=? OR pk_col IN (?, ?)",
                new Object[]{"foo", "bar", "baz", "dunno"});
        primaryKeyValues = (Set<String>)stmt.getPlannerResult(QueryPlanner.ROUTING_VALUES);
        assertThat(primaryKeyValues, is(notNullValue()));
        assertThat(primaryKeyValues, hasItems("foo", "bar", "baz", "dunno"));
        assertEquals(1, stmt.plannerResults().size());
        assertThat(stmt.type(), is(ParsedStatement.SEARCH_ACTION));
    }

    @Test
    public void testSelectMultiplePrimarykeysWhereInInvalid() throws StandardException {
        execStatement("SELECT * FROM phrases WHERE phrase IN (?, ?, ?)",
                new Object[]{"foo", "bar", "baz"});
        assertThat(stmt.getPlannerResult(QueryPlanner.ROUTING_VALUES), is(nullValue()));
    }

}

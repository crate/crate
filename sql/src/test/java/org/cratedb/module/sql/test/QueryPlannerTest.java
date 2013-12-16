package org.cratedb.module.sql.test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import org.cratedb.DataType;
import org.cratedb.action.parser.ESRequestBuilder;
import org.cratedb.action.parser.QueryPlanner;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.TableExecutionContext;
import org.cratedb.index.ColumnDefinition;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.ValueNode;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryPlannerTest {

    private ParsedStatement stmt;
    private SQLParseService parseService;
    private ESRequestBuilder requestBuilder;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private String getSource() throws StandardException {
        return stmt.xcontent.toUtf8();
    }

    private ParsedStatement execStatement(String stmt) throws StandardException {
        return execStatement(stmt, new Object[]{});
    }

    private ParsedStatement execStatement(String sql, Object[] args) throws StandardException {
        NodeExecutionContext nec = mock(NodeExecutionContext.class);
        TableExecutionContext tec = mock(TableExecutionContext.class);
        // Force enabling query planner
        Settings settings = ImmutableSettings.builder().put(QueryPlanner.SETTINGS_OPTIMIZE_PK_QUERIES, true).build();
        QueryPlanner queryPlanner = new QueryPlanner(settings);
        when(tec.getColumnDefinition("phrase")).thenReturn(
            Optional.of(new ColumnDefinition("phrases", "phrase", DataType.STRING, "plain", 0, false, false)));
        when(nec.queryPlanner()).thenReturn(queryPlanner);
        when(nec.tableContext(null, "phrases")).thenReturn(tec);
        when(tec.allCols()).thenReturn(ImmutableSet.of("pk_col", "phrase"));
        when(tec.isRouting("pk_col")).thenReturn(true);
        when(tec.primaryKeys()).thenReturn(new ArrayList<String>(1) {{
            add("pk_col");
        }});
        when(tec.primaryKeysIncludingDefault()).thenReturn(new ArrayList<String>(1) {{
            add("pk_col");
        }});
        when(tec.getCollectorExpression(any(ValueNode.class))).thenCallRealMethod();
        parseService = new SQLParseService(nec);
        stmt = parseService.parse(sql, args);
        requestBuilder = new ESRequestBuilder(stmt);
        return stmt;
    }

    private void assertOnlyPrimaryKeyValueSet(String pkValue) {
        assertEquals(pkValue, stmt.primaryKeyLookupValue);
        assertThat(stmt.primaryKeyValues.isEmpty(), is(true));
        assertThat(stmt.routingValues.isEmpty(), is(true));
        assertNull(stmt.versionFilter);
    }

    private void assertOnlyPrimaryKeyValuesAreSet() {
        assertThat(stmt.primaryKeyValues, is(notNullValue()));
        assertThat("routing values are empty", stmt.routingValues.isEmpty(), is(true));
        assertNull("primary key value is null", stmt.primaryKeyLookupValue);
        assertNull(stmt.versionFilter);
    }

    private void assertOnlyRoutingValuesAreSet() {
        assertThat(stmt.primaryKeyValues.isEmpty(), is(true));
        assertNull(stmt.primaryKeyLookupValue);
        assertNull(stmt.versionFilter);
        assertThat("Routing values are set", !stmt.routingValues.isEmpty(), is(true));
    }

    @Test
    public void testSelectWherePrimaryKey() throws Exception {
        execStatement("select pk_col, phrase from phrases where pk_col=?", new Object[]{1});
        assertEquals(ParsedStatement.ActionType.GET_ACTION, stmt.type());
        assertOnlyPrimaryKeyValueSet("1");
    }

    @Test
    public void testDeleteWherePrimaryKey() throws Exception {
        execStatement("delete from phrases where ?=pk_col", new Object[]{1});
        assertEquals(ParsedStatement.ActionType.DELETE_ACTION, stmt.type());
        assertOnlyPrimaryKeyValueSet("1");
    }

    @Test
    public void testUpdateWherePrimaryKey() throws Exception {
        execStatement("update phrases set phrase=? where pk_col=?",
                new Object[]{"don't panic", 1});
        assertEquals(ParsedStatement.ActionType.UPDATE_ACTION, stmt.type());

        assertOnlyPrimaryKeyValueSet("1");
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
                        .field("size", SQLParseService.DEFAULT_SELECT_LIMIT)
                        .endObject()
                        .string();

        assertEquals(expected, getSource());
        assertEquals(ParsedStatement.ActionType.SEARCH_ACTION, stmt.type());
        Set<String> routingValues = stmt.routingValues;
        assertThat(routingValues.contains("1"), is(true));
        assertThat(routingValues.size(), is(1));

        assertEquals("1", requestBuilder.buildSearchRequest().routing());
        assertOnlyRoutingValuesAreSet();
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
                        .field("size", SQLParseService.DEFAULT_SELECT_LIMIT)
                        .endObject()
                        .string();

        assertEquals(expected, getSource());
        assertEquals(ParsedStatement.ActionType.SEARCH_ACTION, stmt.type());
        Set<String> routingValues = stmt.routingValues;
        assertThat(routingValues.contains("1"), is(true));
        assertEquals("1", requestBuilder.buildSearchRequest().routing());
        assertOnlyRoutingValuesAreSet();
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

        assertEquals(ParsedStatement.ActionType.DELETE_BY_QUERY_ACTION, stmt.type());

        assertEquals("[[phrases]][[]], querySource["+expected+"]",
                requestBuilder.buildDeleteByQueryRequest().toString());

        Set<String> routingValues = stmt.routingValues;
        assertThat(routingValues.contains("1"), is(true));
        assertEquals("1", requestBuilder.buildDeleteByQueryRequest().routing());
        assertOnlyRoutingValuesAreSet();
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

        assertEquals(ParsedStatement.ActionType.SEARCH_ACTION, stmt.type());

        assertEquals(expected, getSource());

        Set<String> routingValues = stmt.routingValues;
        assertThat(routingValues.contains("1"), is(true));
        assertEquals("1", requestBuilder.buildSearchRequest().routing());
        assertOnlyRoutingValuesAreSet();
    }

    @Test
    public void testSelectMultiplePrimaryKeysSimpleOr() throws StandardException {
        execStatement("SELECT pk_col, phrase FROM phrases WHERE pk_col=? OR pk_col=?", new Object[]{"1", "2"});

        Set<String> primaryKeyValues = stmt.primaryKeyValues;
        assertThat(primaryKeyValues, hasItems("1", "2"));
        assertOnlyPrimaryKeyValuesAreSet();
        assertThat(stmt.type(), is(ParsedStatement.ActionType.MULTI_GET_ACTION));
    }

    @Test
    public void testSelectMultiplePrimaryKeysDoubleOr() throws StandardException {
        execStatement("SELECT * FROM phrases WHERE pk_col=? OR pk_col=? OR pk_col=?",
            new Object[]{"foo", "bar", "baz"});
        Set<String> primaryKeyValues = stmt.primaryKeyValues;
        assertOnlyPrimaryKeyValuesAreSet();
        assertThat(primaryKeyValues, hasItems("foo", "bar", "baz"));
        assertThat(stmt.type(), is(ParsedStatement.ActionType.MULTI_GET_ACTION));
    }

    @Test
    public void testSelectMultiplePrimaryKeysNestedOr() throws StandardException {
        execStatement("SELECT * FROM phrases WHERE (pk_col=? OR pk_col=?) OR (pk_col=? OR (pk_col=? OR pk_col=?))",
                new Object[]{"TinkyWinky", "Dipsy", "Lala", "Po", "Hallo"});
        Set<String> primaryKeyValues = stmt.primaryKeyValues;
        assertThat(primaryKeyValues, hasItems("TinkyWinky", "Dipsy", "Lala", "Po", "Hallo"));
        assertThat(stmt.type(), is(ParsedStatement.ActionType.MULTI_GET_ACTION));
    }


    @Test
    public void testSelectMultiplePrimaryKeysOrderBy() throws StandardException {
        execStatement("SELECT * FROM phrases WHERE pk_col=? OR pk_col=? OR pk_col=? order by phrase",
            new Object[]{"foo", "bar", "baz"});
        assertThat(stmt.primaryKeyValues.isEmpty(), is(true));

        Set<String> routingValues = stmt.routingValues;
        assertOnlyRoutingValuesAreSet();
        assertThat(routingValues, hasItems("foo", "bar", "baz"));
        assertThat(stmt.type(), is(ParsedStatement.ActionType.SEARCH_ACTION));
        assertThat(requestBuilder.buildSearchRequest().routing().split(","),
            arrayContainingInAnyOrder("foo", "bar", "baz"));
    }

    @Test
    public void testSelectMultiplePrimaryKeysLimit() throws StandardException {
        execStatement("SELECT * FROM phrases WHERE pk_col=? OR pk_col=? OR pk_col=? limit 1",
            new Object[]{"foo", "bar", "baz"});

        Set<String> routingValues = stmt.routingValues;
        assertOnlyRoutingValuesAreSet();
        assertThat(routingValues, hasItems("foo", "bar", "baz"));
        assertThat(stmt.type(), is(ParsedStatement.ActionType.SEARCH_ACTION));
        assertThat(requestBuilder.buildSearchRequest().routing().split(","),
            arrayContainingInAnyOrder("foo", "bar", "baz"));
    }

    @Test
    public void testSelectMultiplePrimaryKeysGroupBy() throws StandardException {
        execStatement("SELECT pk_col, phrase FROM phrases WHERE pk_col=? OR pk_col=? OR pk_col=? group by phrase",
            new Object[]{"foo", "bar", "baz"});

        Set<String> routingValues = stmt.routingValues;
        assertThat(routingValues, hasItems("foo", "bar", "baz"));
        assertOnlyRoutingValuesAreSet();
        assertThat(stmt.type(), is(ParsedStatement.ActionType.SEARCH_ACTION));
    }

    @Test
    public void testSelectMultiplePrimaryKeysGroupByOrderby() throws StandardException {
        execStatement(
            "SELECT pk_col, phrase FROM phrases WHERE pk_col=? OR pk_col=? OR pk_col=? group by phrase order by phrase",
            new Object[]{"foo", "bar", "baz"}
        );
        Set<String> routingValues = stmt.routingValues;
        assertThat(routingValues, hasItems("foo", "bar", "baz"));
        assertOnlyRoutingValuesAreSet();
        assertThat(stmt.type(), is(ParsedStatement.ActionType.SEARCH_ACTION));
    }

    @Test
    public void testUpdateMultiplePrimaryKeysOr() throws StandardException {
        execStatement(
            "UPDATE phrases SET phrase='blabla' WHERE pk_col=? OR pk_col=?",
            new Object[]{"TinkyWinky", "Dipsy"});
        assertOnlyRoutingValuesAreSet();
        Set<String> routingValues = stmt.routingValues;
        assertThat(routingValues, hasItems("TinkyWinky", "Dipsy"));
        assertThat(requestBuilder.buildSearchRequest().routing().split(","),
            arrayContainingInAnyOrder("TinkyWinky", "Dipsy"));
    }

    @Test
    public void testDeleteMultiplePrimaryKeysOr() throws StandardException {
        execStatement("DELETE FROM phrases WHERE pk_col=? OR pk_col=?",
                new Object[]{"TinkyWinky", "Dipsy"});
        Set<String> routingValues = stmt.routingValues;
        assertOnlyRoutingValuesAreSet();
        assertThat(routingValues, hasItems("TinkyWinky", "Dipsy"));
        assertThat(requestBuilder.buildDeleteByQueryRequest().routing().split(","),
            arrayContainingInAnyOrder("TinkyWinky", "Dipsy"));
    }

    @Test
    public void testSelectMultiplePrimaryKeysInvalid() throws StandardException {
        execStatement("UPDATE phrases SET phrase='invalid' WHERE pk_col=? OR phrase=?",
                new Object[]{"in", "valid"});
        Set<String> routingValues = stmt.routingValues;
        assertThat("Routing values are empty", routingValues.isEmpty(), is(true));
    }

    @Test
    public void testSelectMultiplePrimaryKeysInvalidWithAnd() throws StandardException {
        execStatement("SELECT * FROM phrases WHERE pk_col=? OR (pk_col=? AND pk_col=?)",
                new Object[]{"still", "in", "valid"});
        Set<String> routingValues = stmt.routingValues;
        assertThat(routingValues.isEmpty(), is(true));
    }

    @Test
    public void testSelectMultiplePrimaryKeysInvalidNested() throws StandardException {
        execStatement("SELECT * FROM phrases WHERE (pk_col=? OR pk_col=?) OR (pk_col=? OR (phrase=? OR pk_col=?))",
                new Object[]{"in", "va", "lid", "ne", "sted"});
        assertThat("Routing values are empty", stmt.routingValues.isEmpty(), is(true));
    }

    @Test
    public void testSelectMultiplePrimaryKeysWhereIn() throws StandardException {
        execStatement("SELECT * FROM phrases WHERE pk_col IN (?, ?, ?)",
                new Object[]{"foo", "bar", "baz"});
        Set<String> multiGetPrimaryKeyValues = stmt.primaryKeyValues;
        assertOnlyPrimaryKeyValuesAreSet();
        assertThat(multiGetPrimaryKeyValues, hasItems("foo", "bar", "baz"));
        assertThat(stmt.type(), is(ParsedStatement.ActionType.MULTI_GET_ACTION));
    }

    @Test
    public void testSelectMultiplePrimaryKeysWhereInAndOr() throws StandardException {
        execStatement("SELECT * FROM phrases WHERE pk_col IN (?, ?, ?) OR pk_col=?",
                new Object[]{"foo", "bar", "baz", "dunno"});
        Set<String> multiGetPrimaryKeyValues = stmt.primaryKeyValues;
        assertOnlyPrimaryKeyValuesAreSet();
        assertThat(multiGetPrimaryKeyValues, hasItems("foo", "bar", "baz", "dunno"));
        assertThat(stmt.type(), is(ParsedStatement.ActionType.MULTI_GET_ACTION));

        execStatement("SELECT * FROM phrases WHERE pk_col=? OR pk_col=? OR pk_col IN (?, ?)",
                new Object[]{"foo", "bar", "baz", "dunno"});
        multiGetPrimaryKeyValues = stmt.primaryKeyValues;
        assertOnlyPrimaryKeyValuesAreSet();
        assertThat(multiGetPrimaryKeyValues, hasItems("foo", "bar", "baz", "dunno"));
        assertThat(stmt.type(), is(ParsedStatement.ActionType.MULTI_GET_ACTION));
    }

    @Test
    public void testSelectMultiplePrimarykeysWhereInInvalid() throws StandardException {
        execStatement("SELECT * FROM phrases WHERE phrase IN (?, ?, ?)", new Object[]{"foo", "bar", "baz"});
        assertThat(stmt.routingValues.isEmpty(), is(true));
        assertThat(stmt.primaryKeyValues.isEmpty(), is(true));
    }

    @Test
    public void selectGetRequestWithColumnAlias() throws StandardException {
        execStatement("SELECT phrase as satz FROM phrases WHERE pk_col=?",
                new Object[]{"foo"});
        assertThat(requestBuilder.buildGetRequest().fields(), arrayContaining("phrase"));
    }

    @Test
    public void testDeleteWhereVersion() throws Exception {
        execStatement("delete from phrases where pk_col = ? and \"_version\" = ?",
                new Object[]{112, 1});
        assertEquals(ParsedStatement.ActionType.DELETE_ACTION, stmt.type());
        assertEquals("112", stmt.primaryKeyLookupValue);
        assertEquals(1L, stmt.versionFilter.longValue());

        assertThat(stmt.routingValues.isEmpty(), is(true));
        assertThat(stmt.primaryKeyValues.isEmpty(), is(true));
    }

    @Test
    public void testDeleteByQueryWhereVersionException() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage(
            "_version is only valid in the WHERE clause if paired with a single primary key column and crate.planner.optimize.pk_queries enabled");
        execStatement("delete from phrases where phrase = ? and \"_version\" = ?",
                new Object[]{"don't panic", 1});
    }

    @Test
    public void testUpdateWhereVersion() throws Exception {
        execStatement("update phrases set phrase = ? where pk_col = ? and \"_version\" = ?",
                new Object[]{"don't panic", 112, 1});
        assertEquals(ParsedStatement.ActionType.SEARCH_ACTION, stmt.type());
        assertEquals("112", stmt.primaryKeyLookupValue);
        assertEquals(1L, stmt.versionFilter.longValue());

        assertThat(stmt.routingValues.isEmpty(), is(true));
        assertThat(stmt.primaryKeyValues.isEmpty(), is(true));
    }

    @Test
    public void testUpdateByQueryWhereVersion() throws Exception {
        execStatement("update phrases set phrase = ? where phrase = ? and \"_version\" = ?",
                new Object[]{"now panic", "don't panic", 1});
        assertEquals(ParsedStatement.ActionType.SEARCH_ACTION, stmt.type());
        assertNull(stmt.primaryKeyLookupValue);
        assertEquals(1L, stmt.versionFilter.longValue());
    }
}

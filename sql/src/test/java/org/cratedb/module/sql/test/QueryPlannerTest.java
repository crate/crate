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

import static org.junit.Assert.assertEquals;
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
        assertEquals("1", stmt.getPlannerResult(QueryPlanner.ROUTING_VALUE));
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
        assertEquals("1", stmt.getPlannerResult(QueryPlanner.ROUTING_VALUE));
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

        assertEquals("1", stmt.getPlannerResult(QueryPlanner.ROUTING_VALUE));
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

        assertEquals("1", stmt.getPlannerResult(QueryPlanner.ROUTING_VALUE));
        assertEquals("1", stmt.buildSearchRequest().routing());
        assertEquals(1, stmt.plannerResults().size());
    }

}

package org.cratedb.module.sql.test;

import com.google.common.collect.ImmutableSet;
import org.cratedb.action.parser.QueryPlanner;
import org.cratedb.action.parser.TableVisitor;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TableVisitorTest {

    private ParsedStatement stmt;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private ParsedStatement execStatement(String stmt) throws StandardException {
        return execStatement(stmt, new Object[]{});
    }

    private ParsedStatement execStatement(String sql, Object[] args) throws StandardException {
        NodeExecutionContext nec = mock(NodeExecutionContext.class);
        NodeExecutionContext.TableExecutionContext tec = mock(
                NodeExecutionContext.TableExecutionContext.class);
        // Force enabling query planner
        Settings settings = ImmutableSettings.builder().put(QueryPlanner.SETTINGS_OPTIMIZE_PK_QUERIES, true).build();
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
    public void testCreateTable() throws Exception {
        execStatement("create table phrases (pk_col int primary key, phrase string)");
        TableVisitor visitor = (TableVisitor)stmt.visitor();

        // default values
        Map<String, Object> expectedSettings = new HashMap<String, Object>(){{
            put("number_of_shards", 5);
            put("number_of_replicas", 1);
        }};
        Map<String, Object> expectedMapping = new HashMap<String, Object>(){{
            put("_meta", new HashMap<String, String>(){{
                put("primary_keys", "pk_col");
            }});
            put("properties", new HashMap<String, Object>(){{
                put("pk_col", new HashMap<String, Object>(){{
                    put("type", "integer");
                    put("index", "not_analyzed");
                    put("store", "false");
                }});
                put("phrase", new HashMap<String, Object>(){{
                    put("type", "string");
                    put("index", "not_analyzed");
                    put("store", "false");
                }});
            }});
        }};

        assertEquals(expectedSettings, visitor.settings());
        assertEquals(expectedMapping, visitor.mapping());

        assertNotNull(stmt.buildCreateIndexRequest());
    }

    @Test
    public void testCrateTable() throws Exception {
        execStatement("crate table phrases (pk_col int primary key, phrase string)");
        TableVisitor visitor = (TableVisitor)stmt.visitor();

        // default values
        Map<String, Object> expectedSettings = new HashMap<String, Object>(){{
            put("number_of_shards", 5);
            put("number_of_replicas", 1);
        }};
        Map<String, Object> expectedMapping = new HashMap<String, Object>(){{
            put("_meta", new HashMap<String, String>(){{
                put("primary_keys", "pk_col");
            }});
            put("properties", new HashMap<String, Object>(){{
                put("pk_col", new HashMap<String, Object>(){{
                    put("type", "integer");
                    put("index", "not_analyzed");
                    put("store", "false");
                }});
                put("phrase", new HashMap<String, Object>(){{
                    put("type", "string");
                    put("index", "not_analyzed");
                    put("store", "false");
                }});
            }});
        }};

        assertEquals(expectedSettings, visitor.settings());
        assertEquals(expectedMapping, visitor.mapping());

        assertNotNull(stmt.buildCreateIndexRequest());
    }

    @Test
    public void testCreateTableWithTableProperties() throws Exception {
        execStatement("create table phrases (pk_col int primary key, " +
                "phrase string) replicas 2 clustered by(pk_col) into 10 shards");
        TableVisitor visitor = (TableVisitor)stmt.visitor();

        Map<String, Object> expectedSettings = new HashMap<String, Object>(){{
            put("number_of_shards", 10);
            put("number_of_replicas", 2);
        }};
        Map<String, Object> expectedMapping = new HashMap<String, Object>(){{
            put("_meta", new HashMap<String, String>(){{
                put("primary_keys", "pk_col");
            }});
            put("_routing", new HashMap<String, String>(){{
                put("path", "pk_col");
            }});
            put("properties", new HashMap<String, Object>(){{
                put("pk_col", new HashMap<String, Object>(){{
                    put("type", "integer");
                    put("index", "not_analyzed");
                    put("store", "false");
                }});
                put("phrase", new HashMap<String, Object>(){{
                    put("type", "string");
                    put("index", "not_analyzed");
                    put("store", "false");
                }});
            }});
        }};

        assertEquals(expectedSettings, visitor.settings());
        assertEquals(expectedMapping, visitor.mapping());

        assertNotNull(stmt.buildCreateIndexRequest());
    }


    @Test
    public void testCreateTableThrowUnsupportedTypeException() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Unsupported type");
        execStatement("create table phrases (pk_col real, phrase varchar(10))");
    }

    @Test
    public void testDropTable() throws Exception {
        execStatement("drop table phrases");
        assertNotNull(stmt.buildDeleteIndexRequest());
    }

}

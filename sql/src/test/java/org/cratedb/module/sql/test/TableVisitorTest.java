package org.cratedb.module.sql.test;

import com.google.common.collect.ImmutableSet;
import org.cratedb.action.parser.ESRequestBuilder;
import org.cratedb.action.parser.QueryPlanner;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.TableExecutionContext;
import org.cratedb.action.sql.analyzer.AnalyzerService;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TableVisitorTest {

    private ParsedStatement stmt;
    private ESRequestBuilder requestBuilder;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private ParsedStatement execStatement(String stmt) throws StandardException {
        return execStatement(stmt, new Object[]{});
    }

    private ParsedStatement execStatement(String sql, Object[] args) throws StandardException {
        NodeExecutionContext nec = mock(NodeExecutionContext.class);
        TableExecutionContext tec = mock(TableExecutionContext.class);
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        MetaData clusterMetaData = mock(MetaData.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metaData()).thenReturn(clusterMetaData);

        // example custom analyzer
        try {
            when(clusterMetaData.persistentSettings()).thenReturn(ImmutableSettings.builder()
                    .put(
                            "crate.analysis.custom.analyzer.tabletest",
                            AnalyzerService.encodeSettings(
                                    ImmutableSettings.builder()
                                            .put("index.analysis.analyzer.tabletest.type", "custom")
                                            .put("index.analysis.analyzer.tabletest.tokenizer", "mytok")
                                            .put("index.analysis.analyzer.tabletest.filter.0", "asciifolding")
                                            .build()
                            ).toUtf8()
                    )
                    .put(
                            "crate.analysis.custom.tokenizer.mytok",
                            AnalyzerService.encodeSettings(
                                    ImmutableSettings.builder()
                                            .put("index.analysis.tokenizer.mytok.type", "standard")
                                            .put("index.analysis.tokenizer.mytok.max_token_length", "100")
                                            .build()
                            ).toUtf8()
                    )
                    .put(
                            "crate.analysis.custom.analyzer.invalid",
                            AnalyzerService.encodeSettings(
                                    ImmutableSettings.builder()
                                            .put("index.analysis.analyzer.invalid.type", "custom")
                                            .put("index.analysis.analyzer.invalid.tokenizer", "nonexistent")
                                            .build()
                            ).toUtf8()
                    )
                    .build());
        } catch (IOException e) {
            throw new StandardException(e);
        }

        AnalyzerService analyzerService = new AnalyzerService(clusterService,
                new IndicesAnalysisService(ImmutableSettings.EMPTY));
        when(nec.analyzerService()).thenReturn(analyzerService);
        // Force enabling query planner
        Settings settings = ImmutableSettings.builder().put(QueryPlanner.SETTINGS_OPTIMIZE_PK_QUERIES, true).build();
        QueryPlanner queryPlanner = new QueryPlanner(settings);
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


        SQLParseService parseService = new SQLParseService(nec);
        stmt = parseService.parse(sql, args);
        requestBuilder = new ESRequestBuilder(stmt);
        return stmt;
    }

    @Test
    public void testCreateTable() throws Exception {
        execStatement("create table phrases (pk_col int primary key, phrase string)");

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

        assertEquals(expectedSettings, stmt.indexSettings);
        assertEquals(expectedMapping, stmt.indexMapping);

        assertNotNull(requestBuilder.buildCreateIndexRequest());
    }

    @Test
    public void testCrateTable() throws Exception {
        execStatement("crate table phrases (pk_col int primary key, phrase string)");

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

        assertEquals(expectedSettings, stmt.indexSettings);
        assertEquals(expectedMapping, stmt.indexMapping);

        assertNotNull(requestBuilder.buildCreateIndexRequest());
    }

    @Test
    public void testCreateTableWithTableProperties() throws Exception {
        execStatement("create table phrases (pk_col int primary key, " +
                "phrase string) replicas 2 clustered by(pk_col) into 10 shards");

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

        assertEquals(expectedSettings, stmt.indexSettings);
        assertEquals(expectedMapping, stmt.indexMapping);

        assertNotNull(requestBuilder.buildCreateIndexRequest());
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
        assertNotNull(requestBuilder.buildDeleteIndexRequest());
    }

    @Test
    public void testCreateTableThrowRoutingColumnNotInPrimaryKeysException() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Only columns declared as primary key can be used for routing");
        execStatement("create table phrases (pk_col int primary key, col2 string)" +
                "clustered by(col2)");
    }

    @Test
    public void testCreateTableWithInlineDefaultIndex() throws Exception {
        execStatement("create table phrases (phrase string index using fulltext)");

        Map<String, Object> expectedMapping = new HashMap<String, Object>(){{
            put("properties", new HashMap<String, Object>(){{
                put("phrase", new HashMap<String, Object>(){{
                    put("type", "string");
                    put("index", "analyzed");
                    put("analyzer", "standard");
                    put("store", "false");
                }});
            }});
        }};

        assertEquals(expectedMapping, stmt.indexMapping);
    }

    @Test
    public void testCreateTableWithInlineIndexOff() throws Exception {
        execStatement("create table phrases (phrase string index off)");

        Map<String, Object> expectedMapping = new HashMap<String, Object>(){{
            put("properties", new HashMap<String, Object>(){{
                put("phrase", new HashMap<String, Object>(){{
                    put("type", "string");
                    put("index", "not_analyzed");
                    put("store", "false");
                }});
            }});
        }};

        assertEquals(expectedMapping, stmt.indexMapping);
    }

    @Test
    public void testCreateTableWithInlineIndexProperties() throws Exception {
        execStatement("create table phrases (phrase string index using fulltext " +
                "with (analyzer='german'))");

        Map<String, Object> expectedMapping = new HashMap<String, Object>(){{
            put("properties", new HashMap<String, Object>(){{
                put("phrase", new HashMap<String, Object>(){{
                    put("type", "string");
                    put("index", "analyzed");
                    put("analyzer", "german");
                    put("store", "false");
                }});
            }});
        }};

        assertEquals(expectedMapping, stmt.indexMapping);
    }

    @Test
    public void testCreateTableWithInlineIndexPropertiesWithCustomAnalyzer() throws Exception {
        execStatement("create table phrases (phrase string index using fulltext " +
                "with (analyzer='tabletest'))");

        Map<String, Object> expectedMapping = new HashMap<String, Object>(){{
            put("properties", new HashMap<String, Object>(){{
                put("phrase", new HashMap<String, Object>(){{
                    put("type", "string");
                    put("index", "analyzed");
                    put("analyzer", "tabletest");
                    put("store", "false");
                }});
            }});
        }};

        assertEquals(expectedMapping, stmt.indexMapping);
        assertThat(
            stmt.indexSettings,
            hasEntry("index.analysis.analyzer.tabletest.type", (Object)"custom")
        );
        assertThat(
                stmt.indexSettings,
                hasEntry("index.analysis.analyzer.tabletest.tokenizer", (Object)"mytok")
        );
        assertThat(
                stmt.indexSettings,
                hasEntry("index.analysis.analyzer.tabletest.filter.0", (Object)"asciifolding")
        );
        assertThat(
                stmt.indexSettings,
                hasEntry("index.analysis.tokenizer.mytok.type", (Object)"standard")
        );
        assertThat(
                stmt.indexSettings,
                hasEntry("index.analysis.tokenizer.mytok.max_token_length", (Object)"100")
        );
    }

    @Test
    public void testCreateTableWithDefaultIndex() throws Exception {
        execStatement("create table phrases (phrase string index off, " +
                "index phrase_fulltext using fulltext(phrase))");

        Map<String, Object> expectedMapping = new HashMap<String, Object>(){{
            put("properties", new HashMap<String, Object>(){{
                put("phrase", new HashMap<String, Object>(){{
                    put("type", "multi_field");
                    put("path", "just_name");
                    put("fields", new HashMap<String, Object>(){{
                        put("phrase", new HashMap<String, Object>(){{
                            put("type", "string");
                            put("index", "not_analyzed");
                            put("store", "false");
                        }});
                        put("phrase_fulltext", new HashMap<String, Object>(){{
                            put("type", "string");
                            put("index", "analyzed");
                            put("analyzer", "standard");
                            put("store", "false");
                        }});
                    }});
                }});
            }});
        }};

        assertEquals(expectedMapping, stmt.indexMapping);
    }

    @Test
    public void testCreateTableWithIndexProperties() throws Exception {
        execStatement("create table phrases (phrase string index off, " +
                "index phrase_fulltext using fulltext(phrase) with(analyzer='german'))");

        Map<String, Object> expectedMapping = new HashMap<String, Object>(){{
            put("properties", new HashMap<String, Object>(){{
                put("phrase", new HashMap<String, Object>(){{
                    put("type", "multi_field");
                    put("path", "just_name");
                    put("fields", new HashMap<String, Object>(){{
                        put("phrase", new HashMap<String, Object>(){{
                            put("type", "string");
                            put("index", "not_analyzed");
                            put("store", "false");
                        }});
                        put("phrase_fulltext", new HashMap<String, Object>(){{
                            put("type", "string");
                            put("index", "analyzed");
                            put("analyzer", "german");
                            put("store", "false");
                        }});
                    }});
                }});
            }});
        }};

        assertEquals(expectedMapping, stmt.indexMapping);
    }

    @Test
    public void testCreateTableWithIndexPropertiesAndCustomAnalyzer() throws Exception {
        execStatement("create table phrases (phrase string index off, " +
                "index phrase_fulltext using fulltext(phrase) with(analyzer='tabletest'))");

        Map<String, Object> expectedMapping = new HashMap<String, Object>(){{
            put("properties", new HashMap<String, Object>(){{
                put("phrase", new HashMap<String, Object>(){{
                    put("type", "multi_field");
                    put("path", "just_name");
                    put("fields", new HashMap<String, Object>(){{
                        put("phrase", new HashMap<String, Object>(){{
                            put("type", "string");
                            put("index", "not_analyzed");
                            put("store", "false");
                        }});
                        put("phrase_fulltext", new HashMap<String, Object>(){{
                            put("type", "string");
                            put("index", "analyzed");
                            put("analyzer", "tabletest");
                            put("store", "false");
                        }});
                    }});
                }});
            }});
        }};
        assertEquals(expectedMapping, stmt.indexMapping);

        assertThat(
                stmt.indexSettings,
                hasEntry("index.analysis.analyzer.tabletest.type", (Object)"custom")
        );
        assertThat(
                stmt.indexSettings,
                hasEntry("index.analysis.analyzer.tabletest.tokenizer", (Object)"mytok")
        );
        assertThat(
                stmt.indexSettings,
                hasEntry("index.analysis.analyzer.tabletest.filter.0", (Object)"asciifolding")
        );
        assertThat(
                stmt.indexSettings,
                hasEntry("index.analysis.tokenizer.mytok.type", (Object)"standard")
        );
        assertThat(
                stmt.indexSettings,
                hasEntry("index.analysis.tokenizer.mytok.max_token_length", (Object)"100")
        );
    }

    @Test
    public void testCreateTableWithIndexPropertiesReverse() throws Exception {
        execStatement("create table phrases (" +
                "index phrase_fulltext using fulltext(phrase) with(analyzer='german')," +
                "phrase string index off)");

        Map<String, Object> expectedMapping = new HashMap<String, Object>(){{
            put("properties", new HashMap<String, Object>(){{
                put("phrase", new HashMap<String, Object>(){{
                    put("type", "multi_field");
                    put("path", "just_name");
                    put("fields", new HashMap<String, Object>(){{
                        put("phrase", new HashMap<String, Object>(){{
                            put("type", "string");
                            put("index", "not_analyzed");
                            put("store", "false");
                        }});
                        put("phrase_fulltext", new HashMap<String, Object>(){{
                            put("type", "string");
                            put("index", "analyzed");
                            put("analyzer", "german");
                            put("store", "false");
                        }});
                    }});
                }});
            }});
        }};

        assertEquals(expectedMapping, stmt.indexMapping);
    }

    @Test
    public void testCreateTableWithCompositeIndex() throws Exception {
        execStatement("create table chapters (title string index off, " +
                "description string index off, " +
                "index title_desc_fulltext using fulltext(title, description) " +
                "with(analyzer='german'))");

        Map<String, Object> expectedMapping = new HashMap<String, Object>(){{
            put("properties", new HashMap<String, Object>(){{
                put("title", new HashMap<String, Object>(){{
                    put("type", "multi_field");
                    put("path", "just_name");
                    put("fields", new HashMap<String, Object>(){{
                        put("title", new HashMap<String, Object>(){{
                            put("type", "string");
                            put("index", "not_analyzed");
                            put("store", "false");
                        }});
                        put("title_desc_fulltext", new HashMap<String, Object>(){{
                            put("type", "string");
                            put("index", "analyzed");
                            put("analyzer", "german");
                            put("store", "false");
                        }});
                    }});
                }});
                put("description", new HashMap<String, Object>(){{
                    put("type", "multi_field");
                    put("path", "just_name");
                    put("fields", new HashMap<String, Object>(){{
                        put("description", new HashMap<String, Object>(){{
                            put("type", "string");
                            put("index", "not_analyzed");
                            put("store", "false");
                        }});
                        put("title_desc_fulltext", new HashMap<String, Object>(){{
                            put("type", "string");
                            put("index", "analyzed");
                            put("analyzer", "german");
                            put("store", "false");
                        }});
                    }});
                }});
            }});
        }};

        assertEquals(expectedMapping, stmt.indexMapping);
    }

}

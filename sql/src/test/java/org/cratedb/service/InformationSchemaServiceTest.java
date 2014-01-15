package org.cratedb.service;

import com.google.common.base.Joiner;
import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.integrationtests.Setup;
import org.cratedb.sql.SQLParseException;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;

@CrateIntegrationTest.ClusterScope(numNodes = 2, scope = CrateIntegrationTest.Scope.SUITE)
public class InformationSchemaServiceTest extends SQLTransportIntegrationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static InformationSchemaService informationSchemaService;
    private static SQLParseService parseService;
    private SQLResponse response;

    private void serviceSetup() {
        execute("create table t1 (col1 integer primary key, " +
                    "col2 string) clustered into 7 " +
                    "shards");
        execute(
            "create table t2 (col1 integer primary key, " +
                "col2 string) clustered into " +
                "10 shards");
        execute(
            "create table t3 (col1 integer, col2 string) replicas 8");
        refresh();
    }

    @Before
    public void informationSchemaServiceSetup() throws Exception {
        parseService = cluster().getInstance(SQLParseService.class);
        informationSchemaService = cluster().getInstance(InformationSchemaService.class);
    }

    @After
    public void cleanUp() throws Exception {
        Set<String> indices = client().admin().cluster().prepareState().execute().actionGet()
                .getState().metaData().getIndices().keySet();
        client().admin().indices()
                .prepareDelete(indices.toArray(new String[indices.size()]))
                .execute()
                .actionGet();
    }

    @AfterClass
    public static void shutdownNode() throws Exception {
        parseService = null;
        informationSchemaService = null;
    }

    @Test
    public void testSearchInformationSchemaTablesRefresh() throws Exception {
        serviceSetup();

        exec("select * from information_schema.tables");
        assertEquals(3L, response.rowCount());

        client().execute(SQLAction.INSTANCE,
            new SQLRequest("create table t4 (col1 integer, col2 string)")).actionGet();

        // create table causes a cluster event that will then cause to rebuild the information schema
        // wait until it's rebuild
        Thread.sleep(10);

        exec("select * from information_schema.tables");
        assertEquals(4L, response.rowCount());
    }

    private void exec(String statement) throws Exception {
        exec(statement, new Object[0]);
    }


    /**
     * execUsingClient the statement using the informationSchemaService directly
     * @param statement
     * @param args
     * @throws Exception
     */
    private void exec(String statement, Object[] args) throws Exception {
        ParsedStatement stmt = parseService.parse(statement, args);
        response = informationSchemaService.execute(stmt, System.currentTimeMillis()).actionGet();
    }

    /**
     * execUsingClient the statement using the transportClient
     * @param statement
     * @param args
     * @throws Exception
     */
    private void execUsingClient(String statement, Object[] args) throws Exception {
        response = execute(statement, args);
    }

    private void execUsingClient(String statement) throws Exception {
        execUsingClient(statement, new Object[0]);
    }



    @Test
    public void testExecuteThreadSafety() throws Exception {
        serviceSetup();
        final ParsedStatement stmt = parseService.parse("select * from information_schema.tables");

        int numThreads = 30;
        final CountDownLatch countDownLatch = new CountDownLatch(numThreads);
        ThreadPool pool = new ThreadPool();
        for (int i = 0; i < numThreads; i++) {

            if (i > 4 && i % 3 == 0) {
                client().execute(SQLAction.INSTANCE,
                    new SQLRequest("create table t" + i + " (col1 integer, col2 string) replicas 8")).actionGet();
            }

            pool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        SQLResponse response = informationSchemaService.execute(stmt,
                                System.currentTimeMillis()
                                ).actionGet();
                        assertTrue(response.rowCount() >= 3L);
                        countDownLatch.countDown();
                    } catch (IOException e) {
                        assertTrue(false); // fail test
                    }
                }
            });
        }

        countDownLatch.await(10, TimeUnit.SECONDS);
    }


    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderBy() throws Exception {
        execUsingClient("create table test (col1 integer primary key, col2 string)");
        execUsingClient("create table foo (col1 integer primary key, " +
                "col2 string) clustered by(col1) into 3 shards");
        ensureGreen();
        execUsingClient("select * from INFORMATION_SCHEMA.Tables order by table_name asc");
        assertEquals(2L, response.rowCount());
        assertEquals("foo", response.rows()[0][0]);
        assertEquals(3, response.rows()[0][1]);
        assertEquals(1, response.rows()[0][2]);
        assertEquals("col1", response.rows()[0][3]);

        assertEquals("test", response.rows()[1][0]);
        assertEquals(5, response.rows()[1][1]);
        assertEquals(1, response.rows()[1][2]);
        assertEquals("col1", response.rows()[0][3]);
    }

    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderByAndLimit() throws Exception {
        execUsingClient("create table test (col1 integer primary key, col2 string)");
        execUsingClient("create table foo (col1 integer primary key, col2 string) clustered into 3 shards");
        ensureGreen();
        execUsingClient("select * from INFORMATION_SCHEMA.Tables order by table_name asc limit 1");
        assertEquals(1L, response.rowCount());
        assertEquals("foo", response.rows()[0][0]);
        assertEquals(3, response.rows()[0][1]);
        assertEquals(1, response.rows()[0][2]);
    }

    @Test
    public void testUpdateInformationSchema() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage(
                "INFORMATION_SCHEMA tables are virtual and read-only. Only SELECT statements are supported");
        execUsingClient("update INFORMATION_SCHEMA.Tables set table_name = 'x'");
    }

    @Test
    public void testDeleteInformationSchema() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage(
                "INFORMATION_SCHEMA tables are virtual and read-only. Only SELECT statements are supported");
        execUsingClient("delete from INFORMATION_SCHEMA.Tables");
    }

    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderByTwoColumnsAndLimit() throws Exception {
        execUsingClient("create table test (col1 integer primary key, col2 string) clustered into 1 shards");
        execUsingClient("create table foo (col1 integer primary key, col2 string) clustered into 3 shards");
        execUsingClient("create table bar (col1 integer primary key, col2 string) clustered into 3 shards");
        ensureGreen();
        execUsingClient("select table_name, number_of_shards from INFORMATION_SCHEMA.Tables order by number_of_shards desc, table_name asc limit 2");
        assertEquals(2L, response.rowCount());

        assertEquals("bar", response.rows()[0][0]);
        assertEquals(3, response.rows()[0][1]);
        assertEquals("foo", response.rows()[1][0]);
        assertEquals(3, response.rows()[1][1]);
    }

    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderByAndLimitOffset() throws Exception {
        execUsingClient("create table test (col1 integer primary key, col2 string)");
        execUsingClient("create table foo (col1 integer primary key, col2 string) clustered into 3 shards");
        ensureGreen();
        execUsingClient("select * from INFORMATION_SCHEMA.Tables order by table_name asc limit 1 offset 1");
        assertEquals(1L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);
        assertEquals(5, response.rows()[0][1]);
        assertEquals(1, response.rows()[0][2]);
        assertEquals("col1", response.rows()[0][3]);
    }

    @Test
    public void testSelectFromInformationSchemaTable() throws Exception {
        execUsingClient("select TABLE_NAME from INFORMATION_SCHEMA.Tables");
        assertEquals(0L, response.rowCount());

        execUsingClient("create table test (col1 integer primary key, col2 string)");
        ensureGreen();

        execUsingClient("select table_name, number_of_shards, number_of_replicas, " +
                "routing_column from INFORMATION_SCHEMA.Tables");
        assertEquals(1L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);
        assertEquals(5, response.rows()[0][1]);
        assertEquals(1, response.rows()[0][2]);
        assertEquals("col1", response.rows()[0][3]);
    }

    @Test
    public void testSelectStarFromInformationSchemaTable() throws Exception {
        execUsingClient("create table test (col1 integer, col2 string)");
        ensureGreen();
        execUsingClient("select * from INFORMATION_SCHEMA.Tables");
        assertEquals(1L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);
        assertEquals(5, response.rows()[0][1]);
        assertEquals(1, response.rows()[0][2]);
        assertEquals("_id", response.rows()[0][3]);
    }

    @Test
    public void testSelectFromTableConstraints() throws Exception {

        execUsingClient("select * from INFORMATION_SCHEMA.table_constraints");
        assertEquals(0L, response.rowCount());
        assertThat(response.cols(), arrayContaining("table_name", "constraint_name",
                "constraint_type"));

        execUsingClient("create table test (col1 integer primary key, col2 string)");
        ensureGreen();
        execUsingClient("select constraint_type, constraint_name, " +
                "table_name from information_schema.table_constraints");
        assertEquals(1L, response.rowCount());
        assertEquals("PRIMARY_KEY", response.rows()[0][0]);
        assertEquals("col1", response.rows()[0][1]);
        assertEquals("test", response.rows()[0][2]);
    }

    @Test
    public void testRefreshTableConstraints() throws Exception {
        execUsingClient("create table test (col1 integer primary key, col2 string)");
        ensureGreen();
        execUsingClient("select table_name, constraint_name from INFORMATION_SCHEMA" +
                ".table_constraints");
        assertEquals(1L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);
        assertEquals("col1", response.rows()[0][1]);

        execUsingClient("create table test2 (col1a string primary key, col2a timestamp)");
        ensureGreen();
        execUsingClient("select * from INFORMATION_SCHEMA.table_constraints");

        assertEquals(2L, response.rowCount());
        assertEquals("test2", response.rows()[1][0]);
        assertEquals("col1a", response.rows()[1][1]);
    }

    @Test
    public void testSelectFromRoutines() throws Exception {
        String stmt1 = "CREATE ANALYZER myAnalyzer WITH (" +
                "  TOKENIZER whitespace," +
                "  TOKEN_FILTERS (" +
                "     myTokenFilter WITH (" +
                "      type='snowball'," +
                "      language='german'" +
                "    )," +
                "    kstem" +
                "  )" +
                ")";
        execUsingClient(stmt1);
        execUsingClient("CREATE ANALYZER myOtherAnalyzer extends german (" +
                "  stopwords=[?, ?, ?]" +
                ")", new Object[]{"der", "die", "das"});
        ensureGreen();
        execUsingClient("SELECT * from INFORMATION_SCHEMA.routines where routine_definition != " +
                "'BUILTIN' order by routine_name asc");
        assertEquals(2L, response.rowCount());

        assertEquals("myanalyzer", response.rows()[0][0]);
        assertEquals("ANALYZER", response.rows()[0][1]);
        assertEquals("CREATE ANALYZER myanalyzer WITH (TOKENIZER whitespace, " +
                "TOKEN_FILTERS WITH (" +
                "mytokenfilter WITH (\"language\"='german',\"type\"='snowball'), kstem)" +
                ")", response.rows()[0][2]);

        assertEquals("myotheranalyzer", response.rows()[1][0]);
        assertEquals("ANALYZER", response.rows()[1][1]);
        assertEquals(
                "CREATE ANALYZER myotheranalyzer EXTENDS german WITH (\"stopwords\"=['der','die','das'])",
                response.rows()[1][2]
        );
    }

    @Test
    public void testSelectBuiltinAnalyzersFromRoutines() throws Exception {
        execUsingClient("SELECT routine_name from INFORMATION_SCHEMA.routines WHERE " +
               "\"routine_type\"='ANALYZER' AND \"routine_definition\"='BUILTIN' order by " +
                "routine_name desc");
        assertEquals(42L, response.rowCount());
        String[] analyzerNames = new String[response.rows().length];
        for (int i=0; i<response.rowCount(); i++) {
            analyzerNames[i] = (String)response.rows()[i][0];
        }
        assertEquals(
                "whitespace, turkish, thai, swedish, stop, standard_html_strip, standard, spanish, " +
                "snowball, simple, russian, romanian, portuguese, persian, pattern, " +
                "norwegian, latvian, keyword, italian, irish, indonesian, hungarian, " +
                "hindi, greek, german, galician, french, finnish, english, dutch, default, " +
                "danish, czech, classic, cjk, chinese, catalan, bulgarian, brazilian, " +
                "basque, armenian, arabic",
                Joiner.on(", ").join(analyzerNames)
        );
    }

    @Test
    public void testSelectBuiltinTokenizersFromRoutines() throws Exception {
        execUsingClient("SELECT routine_name from INFORMATION_SCHEMA.routines WHERE " +
                "\"routine_type\"='TOKENIZER' AND \"routine_definition\"='BUILTIN' order by " +
                "routine_name asc");
        assertEquals(13L, response.rowCount());
        String[] tokenizerNames = new String[response.rows().length];
        for (int i=0; i<response.rowCount(); i++) {
            tokenizerNames[i] = (String)response.rows()[i][0];
        }
        assertEquals(
                "classic, edgeNGram, edge_ngram, keyword, letter, lowercase, nGram, ngram, " +
                        "path_hierarchy, pattern, standard, uax_url_email, whitespace",
                Joiner.on(", ").join(tokenizerNames)
        );
    }

    @Test
    public void testSelectBuiltinTokenFiltersFromRoutines() throws Exception {
        execUsingClient("SELECT routine_name from INFORMATION_SCHEMA.routines WHERE " +
                "\"routine_type\"='TOKEN_FILTER' AND \"routine_definition\"='BUILTIN' order by " +
                "routine_name asc");
        assertEquals(43L, response.rowCount());
        String[] tokenFilterNames = new String[response.rows().length];
        for (int i=0; i<response.rowCount(); i++) {
            tokenFilterNames[i] = (String)response.rows()[i][0];
        }
        assertEquals(
                "arabic_normalization, arabic_stem, asciifolding, brazilian_stem, cjk_bigram, " +
                "cjk_width, classic, common_grams, czech_stem, dictionary_decompounder, " +
                "dutch_stem, edgeNGram, edge_ngram, elision, french_stem, german_stem, hunspell, " +
                "hyphenation_decompounder, keep, keyword_marker, keyword_repeat, kstem, " +
                "length, lowercase, nGram, ngram, pattern_capture, pattern_replace, " +
                "persian_normalization, porter_stem, reverse, russian_stem, shingle, " +
                "snowball, standard, stemmer, stemmer_override, stop, synonym, trim, " +
                "truncate, unique, word_delimiter",
                Joiner.on(", ").join(tokenFilterNames)
        );
    }

    @Test
    public void testSelectBuiltinCharFiltersFromRoutines() throws Exception {
        execUsingClient("SELECT routine_name from INFORMATION_SCHEMA.routines WHERE " +
                "\"routine_type\"='CHAR_FILTER' AND \"routine_definition\"='BUILTIN' order by " +
                "routine_name asc");
        assertEquals(4L, response.rowCount());
        String[] charFilterNames = new String[response.rows().length];
        for (int i=0; i<response.rowCount(); i++) {
            charFilterNames[i] = (String)response.rows()[i][0];
        }
        assertEquals(
                "htmlStrip, html_strip, mapping, pattern_replace",
                Joiner.on(", ").join(charFilterNames)
        );
    }

    @Test
    public void testTableConstraintsWithOrderBy() throws Exception {
        execUsingClient("create table test1 (col11 integer primary key, col12 float)");
        execUsingClient("create table test2 (col21 double primary key, col22 string)");
        execUsingClient("create table \"äbc\" (col31 integer primary key, col32 string)");

        ensureGreen();
        execUsingClient("select table_name from INFORMATION_SCHEMA.table_constraints ORDER BY " +
                "table_name");
        assertEquals(3L, response.rowCount());
        assertEquals(response.rows()[0][0], "test1");
        assertEquals(response.rows()[1][0], "test2");
        assertEquals(response.rows()[2][0], "äbc");
    }

    @Test
    public void testSelectFromTableColumns() throws Exception {
        execUsingClient("create table test (col1 integer, col2 string index off, age integer)");
        ensureGreen();
        execUsingClient("select * from INFORMATION_SCHEMA.Columns");
        assertEquals(3L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);
        assertEquals("age", response.rows()[0][1]);
        assertEquals(1, response.rows()[0][2]);
        assertEquals("integer", response.rows()[0][3]);

        assertEquals("col1", response.rows()[1][1]);

        assertEquals("col2", response.rows()[2][1]);
    }

    @Test
    public void testSelectFromTableColumnsRefresh() throws Exception {
        execUsingClient("create table test (col1 integer, col2 string, age integer)");
        ensureGreen();
        execUsingClient("select table_name, column_name, " +
                "ordinal_position, data_type from INFORMATION_SCHEMA.Columns");
        assertEquals(3L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);

        execUsingClient("create table test2 (col1 integer, col2 string, age integer)");
        ensureGreen();
        execUsingClient("select table_name, column_name, " +
                "ordinal_position, data_type from INFORMATION_SCHEMA.Columns " +
                "order by table_name");

        assertEquals(6L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);
        assertEquals("test2", response.rows()[4][0]);
    }

    @Test
    public void testSelectFromTableColumnsMultiField() throws Exception {
        execUsingClient("create table test (col1 string, col2 string," +
                "index col1_col2_ft using fulltext(col1, col2))");
        ensureGreen();
        execUsingClient("select table_name, column_name," +
                "ordinal_position, data_type from INFORMATION_SCHEMA.Columns");
        assertEquals(2L, response.rowCount());

        assertEquals("test", response.rows()[0][0]);
        assertEquals("col1", response.rows()[0][1]);
        assertEquals(1, response.rows()[0][2]);
        assertEquals("string", response.rows()[0][3]);

        assertEquals("test", response.rows()[1][0]);
        assertEquals("col2", response.rows()[1][1]);
        assertEquals(2, response.rows()[1][2]);
        assertEquals("string", response.rows()[1][3]);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSelectFromTableIndices() throws Exception {
        execUsingClient("create table test (col1 string, col2 string, " +
                "col3 string index using fulltext, " +
                "col4 string index off, " +
                "index col1_col2_ft using fulltext(col1, col2) with(analyzer='english'))");
        ensureGreen();
        execUsingClient("select table_name, index_name, method, columns, properties " +
                "from INFORMATION_SCHEMA.Indices");
        assertEquals(4L, response.rowCount());

        assertEquals("test", response.rows()[0][0]);
        assertEquals("col1", response.rows()[0][1]);
        assertEquals("plain", response.rows()[0][2]);
        assertTrue(response.rows()[0][3] instanceof List);
        assertThat((List<String>) response.rows()[0][3], contains("col1"));
        assertEquals("", response.rows()[0][4]);

        assertEquals("test", response.rows()[1][0]);
        assertEquals("col2", response.rows()[1][1]);
        assertEquals("plain", response.rows()[1][2]);
        assertThat((List<String>) response.rows()[1][3], contains("col2"));
        assertEquals("", response.rows()[1][4]);

        assertEquals("test", response.rows()[2][0]);
        assertEquals("col1_col2_ft", response.rows()[2][1]);
        assertEquals("fulltext", response.rows()[2][2]);
        assertThat((List<String>) response.rows()[2][3], contains("col1", "col2"));
        assertEquals("analyzer=english", response.rows()[2][4]);

        assertEquals("test", response.rows()[3][0]);
        assertEquals("col3", response.rows()[3][1]);
        assertEquals("fulltext", response.rows()[3][2]);
        assertThat((List<String>) response.rows()[3][3], contains("col3"));
        assertEquals("analyzer=standard", response.rows()[3][4]);
    }

    @Test
    public void testGlobalAggregation() throws Exception {
        execUsingClient("select max(ordinal_position) from information_schema.columns");
        assertEquals(1, response.rowCount());

        assertNull(response.rows()[0][0]);

        execUsingClient("create table t1 (id integer, col1 string)");
        ensureGreen();
        execUsingClient("select max(ordinal_position) from information_schema.columns");
        assertEquals(1, response.rowCount());

        assertEquals(2, response.rows()[0][0]);

    }

    @Test
    public void testGlobalAggregationMany() throws Exception {
        execUsingClient("create table t1 (id integer, col1 string) clustered into 10 shards replicas 14");
        execUsingClient("create table t2 (id integer, col1 string) clustered into 5 shards replicas 7");
        execUsingClient("create table t3 (id integer, col1 string) clustered into 3 shards replicas 2");
        ensureYellow();
        execUsingClient("select min(number_of_replicas), max(number_of_replicas), avg(number_of_replicas)," +
                "sum(number_of_shards) from information_schema.tables");
        assertEquals(1, response.rowCount());

        assertEquals(2, response.rows()[0][0]);
        assertEquals(14, response.rows()[0][1]);
        assertEquals(7.666666666666667d, response.rows()[0][2]);
        assertEquals(18.0d, response.rows()[0][3]);
    }

    @Test
    public void testGlobalAggregationWithWhere() throws Exception {
        execUsingClient("create table t1 (id integer, col1 string) clustered into 10 shards replicas 14");
        execUsingClient("create table t2 (id integer, col1 string) clustered into 5 shards replicas 7");
        execUsingClient("create table t3 (id integer, col1 string) clustered into 3 shards replicas 2");
        ensureYellow();
        execUsingClient("select min(number_of_replicas), max(number_of_replicas), avg(number_of_replicas)," +
                "sum(number_of_shards) from information_schema.tables where table_name != 't1'");
        assertEquals(1, response.rowCount());

        assertEquals(2, response.rows()[0][0]);
        assertEquals(7, response.rows()[0][1]);
        assertEquals(4.5d, response.rows()[0][2]);
        assertEquals(8.0d, response.rows()[0][3]);
    }

    @Test
    public void testGlobalAggregationWithAlias() throws Exception {
        execUsingClient("create table t1 (id integer, col1 string) clustered into 10 shards replicas 14");
        execUsingClient("create table t2 (id integer, col1 string) clustered into 5 shards replicas 7");
        execUsingClient("create table t3 (id integer, col1 string) clustered into 3 shards replicas 2");
        ensureYellow();
        execUsingClient("select min(number_of_replicas) as min_replicas from information_schema.tables where table_name = 't1'");
        assertEquals(1, response.rowCount());

        assertEquals(14, response.rows()[0][0]);
    }

    @Test
    public void testGlobalCount() throws Exception {
        execUsingClient("create table t1 (id integer, col1 string) clustered into 10 shards replicas 14");
        execUsingClient("create table t2 (id integer, col1 string) clustered into 5 shards replicas 7");
        execUsingClient("create table t3 (id integer, col1 string) clustered into 3 shards replicas 2");
        ensureYellow();
        execUsingClient("select count(*) from information_schema.tables");
        assertEquals(1, response.rowCount());
        assertEquals(3L, response.rows()[0][0]);
    }

    @Test
    public void testGlobalCountDistinct() throws Exception {
        execUsingClient("select count(distinct routine_type) from information_schema.routines order by count(distinct routine_type)");
        assertEquals(1, response.rowCount());
        assertEquals(4L, response.rows()[0][0]);
    }

    @Test
    public void selectGlobalExpressionGlobalAggregate() throws Exception {
        serviceSetup();
        execUsingClient("select count(*), sys.cluster.name from information_schema.tables");
        assertEquals(1, response.rowCount());
        assertEquals(3L, response.rows()[0][0]);
        assertEquals(cluster().clusterName(), response.rows()[0][1]);
    }

    @Test
    public void selectGlobalExpressionGroupBy() throws Exception {
        serviceSetup();
        execUsingClient("select table_name, count(column_name), sys.cluster.name " +
                        "from information_schema.columns group by table_name, sys.cluster.name " +
                        "order by table_name");
        assertEquals(3, response.rowCount());

        assertEquals("t1", response.rows()[0][0]);
        assertEquals(2L, response.rows()[0][1]);
        assertEquals(cluster().clusterName(), response.rows()[0][2]);

        assertEquals("t2", response.rows()[1][0]);
        assertEquals(2L, response.rows()[1][1]);
        assertEquals(cluster().clusterName(), response.rows()[0][2]);

        assertEquals("t3", response.rows()[2][0]);
        assertEquals(2L, response.rows()[2][1]);
        assertEquals(cluster().clusterName(), response.rows()[0][2]);
    }

    @Test
    public void selectDynamicObjectAddsSubColumn() throws Exception {
        execUsingClient("create table t4 (" +
                "  title string," +
                "  stuff object(dynamic) as (" +
                "    first_name string," +
                "    last_name string" +
                "  )" +
                ") replicas 0");
        ensureGreen();
        execUsingClient("select column_name, ordinal_position from information_schema.columns where table_name='t4'");
        assertEquals(4, response.rowCount());
        assertEquals("stuff", response.rows()[0][0]);
        assertEquals(1, response.rows()[0][1]);

        assertEquals("stuff.first_name", response.rows()[1][0]);
        assertEquals(2, response.rows()[1][1]);

        assertEquals("stuff.last_name", response.rows()[2][0]);
        assertEquals(3, response.rows()[2][1]);

        assertEquals("title", response.rows()[3][0]);
        assertEquals(4, response.rows()[3][1]);

        execUsingClient("insert into t4 (stuff) values (?)", new Object[]{
                new HashMap<String, Object>() {{
                    put("first_name", "Douglas");
                    put("middle_name", "Noel");
                    put("last_name", "Adams");
                }}
        });
        refresh();

        execUsingClient("select column_name, ordinal_position from information_schema.columns where table_name='t4'");
        assertEquals(5, response.rowCount());
        assertEquals("stuff", response.rows()[0][0]);
        assertEquals(1, response.rows()[0][1]);

        assertEquals("stuff.first_name", response.rows()[1][0]);
        assertEquals(2, response.rows()[1][1]);

        assertEquals("stuff.last_name", response.rows()[2][0]);
        assertEquals(3, response.rows()[2][1]);

        assertEquals("stuff.middle_name", response.rows()[3][0]);
        assertEquals(4, response.rows()[3][1]);


        assertEquals("title", response.rows()[4][0]);
        assertEquals(5, response.rows()[4][1]);
    }

    @Test
    public void testAddColumnToIgnoredObject() throws Exception {
        execUsingClient("create table t4 (" +
                "  title string," +
                "  stuff object(ignored) as (" +
                "    first_name string," +
                "    last_name string" +
                "  )" +
                ")");
        ensureYellow();
        execUsingClient("select column_name, ordinal_position from information_schema.columns where table_name='t4'");
        assertEquals(4, response.rowCount());
        assertEquals("stuff", response.rows()[0][0]);
        assertEquals(1, response.rows()[0][1]);

        assertEquals("stuff.first_name", response.rows()[1][0]);
        assertEquals(2, response.rows()[1][1]);

        assertEquals("stuff.last_name", response.rows()[2][0]);
        assertEquals(3, response.rows()[2][1]);

        assertEquals("title", response.rows()[3][0]);
        assertEquals(4, response.rows()[3][1]);

        execUsingClient("insert into t4 (stuff) values (?)", new Object[]{
                new HashMap<String, Object>() {{
                    put("first_name", "Douglas");
                    put("middle_name", "Noel");
                    put("last_name", "Adams");
                }}
        });

        execUsingClient("select column_name, ordinal_position from information_schema.columns where table_name='t4'");
        assertEquals(4, response.rowCount());
        assertEquals("stuff", response.rows()[0][0]);
        assertEquals(1, response.rows()[0][1]);

        assertEquals("stuff.first_name", response.rows()[1][0]);
        assertEquals(2, response.rows()[1][1]);

        assertEquals("stuff.last_name", response.rows()[2][0]);
        assertEquals(3, response.rows()[2][1]);

        assertEquals("title", response.rows()[3][0]);
        assertEquals(4, response.rows()[3][1]);
    }

    @Test
    public void testUnknownTypes() throws Exception {
        new Setup(this).setUpObjectMappingWithUnknownTypes();
        execUsingClient("select * from information_schema.columns where table_name='ut' order by column_name");
        assertEquals(2, response.rowCount());

        assertEquals("name", response.rows()[0][1]);
        assertEquals(1, response.rows()[0][2]);
        assertEquals("string", response.rows()[0][3]);

        assertEquals("population", response.rows()[1][1]);
        assertEquals(2, response.rows()[1][2]);
        assertEquals("long", response.rows()[1][3]);

        execUsingClient("select * from information_schema.indices where table_name='ut' order by index_name");
        assertEquals(2, response.rowCount());
        assertEquals("name", response.rows()[0][1]);
        assertEquals("population", response.rows()[1][1]);

        execUsingClient("select sum(number_of_shards) from information_schema.tables");
        assertEquals(1, response.rowCount());
    }
}

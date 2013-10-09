package org.cratedb.module.sql.benchmark;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.google.common.base.Charsets;
import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.test.integration.AbstractCrateNodesTests;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "benchmark-insert")
public class InsertBenchmark extends AbstractCrateNodesTests {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

        @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    public static final String SINGLE_INSERT_SQL_STMT = "INSERT INTO countries " +
            "(\"countryName\", \"countryCode\", \"isoNumeric\", \"east\", \"north\", \"west\", \"south\"," +
            "\"isoAlpha3\", \"currencyCode\", \"continent\", \"continentName\", \"languages\", \"fipsCode\", \"capital\", \"population\") " +
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    public static final String BULK_INSERT_SQL_STMT = "INSERT INTO countries " +
            "(\"countryName\", \"countryCode\", \"isoNumeric\", \"east\", \"north\", \"west\", \"south\"," +
            "\"isoAlpha3\", \"currencyCode\", \"continent\", \"continentName\", \"languages\", \"fipsCode\", \"capital\", \"population\") " +
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?), (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?), (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?), (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    public static String singleApiInsertSource;
    public static List<Node> startedNodes = new ArrayList<>(2);

    @BeforeClass
    public static void prepareIndexSource() throws IOException {
        singleApiInsertSource = XContentFactory.jsonBuilder().startObject()
                .field("countryName", "Mordor")
                .field("countryCode", "MO")
                .field("isoNumeric", "666")
                .field("east", 0.0)
                .field("north", 180.0)
                .field("west", 90.0)
                .field("south", 0.0)
                .field("isoAlpha3", "MOR")
                .field("currencyCode", "NAZ")
                .field("continent", "ME")
                .field("continentName", "Mittelerde")
                .field("languages", "naz")
                .field("fipsCode", "MOR")
                .field("capital", "Schicksalsberg")
                .field("population", 1000)
                .endObject().string();
    }

    @Before
    public void prepareIndex() throws Exception {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.builder();
        for (String nodeId : new String[]{"insert1", "insert2"}) {
            Node insertNode = node(nodeId);
            if (insertNode==null) {
                startedNodes.add(startNode(nodeId));
                if (!client(nodeId).admin().indices().exists(new IndicesExistsRequest("countries")).actionGet().isExists()) {
                    // only create index if not already there
                    client(nodeId).admin().indices().prepareCreate("countries").setSettings(settingsBuilder.build()).setSettings(
                        settingsBuilder.loadFromClasspath("/essetup/settings/bench.json").build())
                        .addMapping("default", stringFromPath("/essetup/mappings/bench.json", InsertBenchmark.class)).execute().actionGet();

                }
            }
        }
    }

    @AfterClass
    public static void shutDownNodes() {
        for (Node node: startedNodes) {
            if (node != null && !node.isClosed()) {
                node.close();
            }
        }
        startedNodes.clear();
    }

    private SQLRequest getSingleSqlInsertRequest() {
        return new SQLRequest(SINGLE_INSERT_SQL_STMT,
                new Object[]{"Mordor", "MO", "666", 0.0, 180.0, 90.0, 0.0,
                             "MOR", "NAZ", "ME", "Mittelerde", "naz", "MOR",
                             "Schicksalsberg", 1000 }
        );
    }

    private SQLRequest getBulkSqlInsertRequest() {
        return new SQLRequest(BULK_INSERT_SQL_STMT,
                new Object[]{
                        "Mordor", "MO", "666", 0.0, 180.0, 90.0, 0.0,
                        "MOR", "NAZ", "ME", "Mittelerde", "naz", "MOR",
                        "Schicksalsberg", 1000,

                        "Auenland", "AU", "123", 1.1, 2.2, 3.3, 4.4,
                        "AUL", "BOC", "ME", "Mittelerde", "boc", "AUL",
                        "Hobbingen", 200,

                        "Mordor", "MO", "666", 0.0, 180.0, 90.0, 0.0,
                        "MOR", "NAZ", "ME", "Mittelerde", "naz", "MOR",
                        "Schicksalsberg", 1000,

                        "Auenland", "AU", "123", 1.1, 2.2, 3.3, 4.4,
                        "AUL", "BOC", "ME", "Mittelerde", "boc", "AUL",
                        "Hobbingen", 200
                }
        );
    }

    private IndexRequest getSingleApiInsertRequest() {
        IndexRequest request = new IndexRequest("countries", "default");
        request.create(true);
        request.source(singleApiInsertSource);
        return request;
    }

    private BulkRequest getBulkApiInsertRequest() {
        BulkRequest request = new BulkRequest();
        for (int i=0; i<4;i++) {
            request.add(
                getSingleApiInsertRequest()
            );
        }
        return request;
    }

    // TODO: COPYPASTE from AbstractSHaredCrateClusterTest --> UtilityClass
    public String stringFromPath(String path, Class<?> aClass) throws IOException {
        return Streams.copyToString(new InputStreamReader(
                getInputStream(path, aClass),
                Charsets.UTF_8));
    }

    public InputStream getInputStream(String path, Class<?> aClass) throws FileNotFoundException {
        InputStream is = aClass.getResourceAsStream(path);
        if (is == null) {
            throw new FileNotFoundException("Resource [" + path + "] not found in classpath");
        }
        return is;
    }

    @BenchmarkOptions(benchmarkRounds = 100, warmupRounds = 1)
    @Test
    public void testInsertSingleSql() {
        client().execute(SQLAction.INSTANCE, getSingleSqlInsertRequest()).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = 100, warmupRounds = 1)
    @Test
    public void testInsertBulkSql() {
        client().execute(SQLAction.INSTANCE, getBulkSqlInsertRequest()).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = 100, warmupRounds = 1)
    @Test
    public void testInsertSingleApi() {
        client().execute(IndexAction.INSTANCE, getSingleApiInsertRequest()).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = 100, warmupRounds = 1)
    @Test
    public void testInsertBulkApi() {
         client().execute(BulkAction.INSTANCE, getBulkApiInsertRequest()).actionGet();
    }
}

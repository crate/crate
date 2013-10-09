package org.cratedb.module.sql.benchmark;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.test.integration.AbstractSharedCrateClusterTest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;

@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "benchmark-insert")
public class InsertBenchmark extends AbstractSharedCrateClusterTest {

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

    @Override
    protected int numberOfNodes() {
        return 2;
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


    @Before
    public void prepareIndex() throws Exception {
        prepareCreate("countries").setSettings(
                randomSettingsBuilder().loadFromClasspath("/essetup/settings/bench.json").build())
                .addMapping("default", stringFromPath("/essetup/mappings/bench.json", InsertBenchmark.class))
                .execute().actionGet();
        refresh();
    }

    @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 1)
    @Test
    public void testInsertSingleSql() {
        client().execute(SQLAction.INSTANCE, getSingleSqlInsertRequest()).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 1)
    @Test
    public void testInsertBulkSql() {
        client().execute(SQLAction.INSTANCE, getBulkSqlInsertRequest()).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 1)
    @Test
    public void testInsertSingleApi() {
        client().execute(IndexAction.INSTANCE, getSingleApiInsertRequest()).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 1)
    @Test
    public void testInsertBulkApi() {

    }
}

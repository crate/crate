package org.cratedb.integrationtests;

import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.action.sql.analyzer.AnalyzerService;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.test.integration.AbstractCrateNodesTests;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsMapContaining.hasEntry;

public class CrateClusterSettingsActionTest extends AbstractCrateNodesTests {
    public static final int NUM_NODES = 2;
    public static boolean nodesRunning = false;
    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Before
    public void startNodes() {
        if (!nodesRunning) {
            for (int i = 0; i< NUM_NODES; i++) {
                startNode(this.getTestName() + i);
            }
            nodesRunning = true;
        }
    }

    private SQLResponse execute(String stmt) {
        return execute(stmt, new Object[0]);
    }

    private SQLResponse execute(String stmt, Object[] args) {
        return client().execute(SQLAction.INSTANCE, new SQLRequest(stmt, args)).actionGet();
    }

    public Settings getPersistentClusterSettings() {
        ClusterStateResponse response = client().admin().cluster().prepareState().execute().actionGet();
        return response.getState().metaData().persistentSettings();
    }

    @Test
    public void createSimpleAnalyzer() throws IOException {
        execute("CREATE ANALYZER a1 WITH (" +
                "  TOKENIZER standard" +
                ")");
        Settings customAnalyzerSettings = getPersistentClusterSettings();

        assertThat(
                customAnalyzerSettings.getAsMap(),
                hasKey("crate.analyzer.custom.analyzer.a1")
        );
        Settings analyzerSettings = AnalyzerService.decodeSettings(customAnalyzerSettings.get("crate.analyzer.custom.analyzer.a1"));
        assertThat(
            analyzerSettings.getAsMap(),
            allOf(
                    hasEntry("index.analysis.analyzer.a1.type", "custom"),
                    hasEntry("index.analysis.analyzer.a1.tokenizer", "standard")
            )
        );
    }

    @Test
    public void createAnalyzerWithCustomTokenizer() throws IOException {
        execute("CREATE ANALYZER a2 WITH (" +
                "  TOKENIZER custom WITH (" +
                "    type='keyword'," +
                "    \"buffer_size\"=1024" +
                "  )" +
                ")");
        Settings settings = getPersistentClusterSettings();
        assertThat(
                settings.getAsMap(),
                hasKey("crate.analyzer.custom.analyzer.a2")
        );
        assertThat(
                settings.getAsMap(),
                hasKey("crate.analyzer.custom.tokenizer.custom")
        );
        Settings analyzerSettings = AnalyzerService.decodeSettings(settings.get("crate.analyzer.custom.analyzer.a2"));
        assertThat(
                analyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a2.type", "custom")
        );
        assertThat(
                analyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a2.tokenizer", "custom")
        );

        Settings tokenizerSettings = AnalyzerService.decodeSettings(settings.get("crate.analyzer.custom.tokenizer.custom"));
        assertThat(
                tokenizerSettings.getAsMap(),
                hasEntry("index.analysis.tokenizer.custom.type", "keyword")
        );
        assertThat(tokenizerSettings.getAsMap(), hasEntry("index.analysis.tokenizer.custom.buffer_size", "1024"));
    }

    @Test
    public void createExtendingCustomAnalyzer() throws IOException {
        execute("CREATE ANALYZER a3 (" +
                "  token_filters (" +
                "    greeklowercase with (" +
                "      type='lowercase'," +
                "      language='greek'" +
                "    )," +
                "    ngram" +
                "  )," +
                "  tokenizer standard" +
                ")");
        Settings settings = getPersistentClusterSettings();
        assertThat(
                settings.getAsMap(),
                allOf(
                        hasKey("crate.analyzer.custom.analyzer.a3"),
                        hasKey("crate.analyzer.custom.filter.greeklowercase")
                )
        );
        Settings analyzerSettings = AnalyzerService.decodeSettings(settings.get("crate.analyzer.custom.analyzer.a3"));
        assertThat(
                analyzerSettings.getAsArray("index.analysis.analyzer.a3.filter"),
                arrayContainingInAnyOrder("ngram", "greeklowercase")
        );
        assertThat(analyzerSettings.getAsMap(), hasEntry("index.analysis.analyzer.a3.tokenizer", "standard"));

        execute("CREATE ANALYZER a4 EXTENDS a3 WITH (" +
                "  char_filters WITH (" +
                "    html_strip" +
                "  )," +
                "  tokenizer whitespace" +
                ")");
        Settings extendedSettings = getPersistentClusterSettings();
        assertThat(
                extendedSettings.getAsMap(),
                allOf(
                        hasKey("crate.analyzer.custom.analyzer.a3"),
                        hasKey("crate.analyzer.custom.analyzer.a4")
                )
        );

        Settings extendedAnalyzerSettings = AnalyzerService.decodeSettings(extendedSettings.get("crate.analyzer.custom.analyzer.a4"));
        assertThat(extendedAnalyzerSettings.getAsArray("index.analysis.analyzer.a4.char_filter"), arrayContainingInAnyOrder("html_strip"));
        assertThat(extendedAnalyzerSettings.getAsArray("index.analysis.analyzer.a4.filter"), arrayContainingInAnyOrder("ngram", "greeklowercase"));
        assertThat(extendedAnalyzerSettings.getAsMap(), hasEntry("index.analysis.analyzer.a4.tokenizer", "whitespace"));

    }

    @Test
    public void createExtendingBuiltinAnalyzer() throws IOException {
        execute("CREATE ANALYZER a5 EXTENDS stop WITH (" +
                "   stopwords=['foo', 'bar', 'baz']" +
                ")");
        Settings settings = getPersistentClusterSettings();
        assertThat(
                settings.getAsMap(),
                hasKey("crate.analyzer.custom.analyzer.a5")
        );
        Settings analyzerSettings = AnalyzerService.decodeSettings(settings.get("crate.analyzer.custom.analyzer.a5"));
        assertThat(
                analyzerSettings.getAsMap(), hasEntry("index.analysis.analyzer.a5.type", "stop")
        );
        assertThat(analyzerSettings.getAsArray("index.analysis.analyzer.a5.stopwords"), arrayContainingInAnyOrder("foo", "bar", "baz"));
    }


    @Test(expected = SQLParseException.class)
    public void createAnalyzerWithoutTokenizer() throws IOException {
        execute("CREATE ANALYZER a6 WITH (" +
                "  char_filters WITH (" +
                "    \"html_strip\"" +
                "  )," +
                "  token_filters WITH (" +
                "    lowercase" +
                "  )" +
                ")");
    }

    @Test
    public void createAndExtendFullCustomAnalyzer() throws IOException {
        execute("CREATE ANALYZER a7 (" +
                "  char_filters (" +
                "     mypattern WITH (" +
                "       type='pattern_replace'," +
                "      \"pattern\" ='sample(.*)',\n" +
                "      \"replacement\" = 'replacedSample $1'" +
                "     )," +
                "     \"html_strip\"" +
                "  )," +
                "  tokenizer mytok WITH (" +
                "    type='edgeNGram'," +
                "    \"min_gram\" = 2," +
                "    \"max_gram\" = 5," +
                "    \"token_chars\" = [ 'letter', 'digit' ]" +
                "  )," +
                "  token_filters WITH (" +
                "    myshingle WITH (" +
                "      type='shingle'," +
                "      \"output_unigrams\"=false," +
                "      \"max_shingle_size\"=10" +
                "    )," +
                "    lowercase," +
                "    \"my_stemmer\" WITH (" +
                "      type='stemmer'," +
                "      language='german'" +
                "    )" +
                "  )" +
                ")");
        Settings settings = getPersistentClusterSettings();

        assertThat(
                settings.getAsMap(),
                allOf(
                        hasKey("crate.analyzer.custom.analyzer.a7"),
                        hasKey("crate.analyzer.custom.tokenizer.mytok"),
                        hasKey("crate.analyzer.custom.char_filter.mypattern"),
                        hasKey("crate.analyzer.custom.filter.myshingle"),
                        hasKey("crate.analyzer.custom.filter.my_stemmer")
                )
        );
        Settings analyzerSettings = AnalyzerService.decodeSettings(settings.get("crate.analyzer.custom.analyzer.a7"));
        assertThat(
                analyzerSettings.getAsArray("index.analysis.analyzer.a7.char_filter"),
                arrayContainingInAnyOrder("mypattern", "html_strip")
        );
        assertThat(
                analyzerSettings.getAsArray("index.analysis.analyzer.a7.filter"),
                arrayContainingInAnyOrder("myshingle", "lowercase", "my_stemmer")
        );
        assertThat(
                analyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a7.tokenizer", "mytok")
        );
        execute("CREATE ANALYZER a8 EXTENDS a7 WITH (" +
                "  token_filters (" +
                "    lowercase," +
                "    kstem" +
                "  )" +
                ")");
        Settings extendedSettings = getPersistentClusterSettings();
        assertThat(
            extendedSettings.getAsMap(),
            allOf(
                    hasKey("crate.analyzer.custom.analyzer.a8"),
                    hasKey("crate.analyzer.custom.tokenizer.mytok")
            )
        );
        Settings extendedAnalyzerSettings = AnalyzerService.decodeSettings(extendedSettings.get("crate.analyzer.custom.analyzer.a8"));
        assertThat(
                extendedAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a8.type", "custom")
        );
        assertThat(
                extendedAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a8.tokenizer", "mytok")
        );
        assertThat(
                extendedAnalyzerSettings.getAsArray("index.analysis.analyzer.a8.filter"),
                arrayContainingInAnyOrder("lowercase", "kstem")
        );
        assertThat(
                extendedAnalyzerSettings.getAsArray("index.analysis.analyzer.a8.char_filter"),
                arrayContainingInAnyOrder("mypattern", "html_strip")
        );

    }

    @Test
    public void reuseExistingTokenizer() throws StandardException, IOException {
        execute("CREATE ANALYZER a9 (" +
                "  TOKENIZER a9tok WITH (" +
                "    type='nGram'," +
                "    \"token_chars\"=['letter', 'digit']" +
                "  )" +
                ")");

        execute("CREATE ANALYZER a10 (" +
                "  TOKENIZER a9tok" +
                ")");

        Settings settings = getPersistentClusterSettings();
        Settings a10Settings = AnalyzerService.decodeSettings(settings.get("crate.analyzer.custom.analyzer.a10"));
        assertThat(
                a10Settings.getAsMap(),
                hasEntry("index.analysis.analyzer.a10.tokenizer", "a9tok")
        );
    }

}

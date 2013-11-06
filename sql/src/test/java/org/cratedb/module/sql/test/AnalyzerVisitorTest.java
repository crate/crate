package org.cratedb.module.sql.test;

import org.cratedb.action.parser.visitors.AnalyzerVisitor;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.analyzer.AnalyzerService;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.TestCase.assertEquals;
import static org.cratedb.action.sql.analyzer.AnalyzerService.decodeSettings;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AnalyzerVisitorTest {

    private ClusterService mockedClusterService = mock(ClusterService.class);

    /**
     * Execute the CREATE ANALYZER statement and return the built settings
     * @param stmt
     * @param args
     * @return
     * @throws StandardException
     */
    public Settings executeStatement(String stmt, Object[] args) throws StandardException {
        AnalyzerService analyzerService = new AnalyzerService(
                mockedClusterService,
                new IndicesAnalysisService(ImmutableSettings.EMPTY)
        );
        NodeExecutionContext nodeExecutionContext = mock(NodeExecutionContext.class);
        when(nodeExecutionContext.analyzerService()).thenReturn(analyzerService);
        SQLParseService parseService = new SQLParseService(nodeExecutionContext);

        Settings settings = null;
        try {
            ParsedStatement parsedStatement = parseService.parse(stmt, args);
            settings = parsedStatement.createAnalyzerSettings;
        } catch (SQLParseException e) {
            System.err.println(stmt);
            throw e;
        }
        return settings;
    }

    @Test
    public void createAnalyzerWithBuiltinTokenizer() throws StandardException, IOException {
        Settings settings = executeStatement("CREATE ANALYZER a1a WITH (" +
                "  TOKENIZER standard" +
                ")", null);

        assertThat(
                settings.getAsMap(),
                not(Matchers.hasKey(AnalyzerVisitor.getPrefixedSettingsKey("tokenizer.standard")))
        );
        Settings analyzerSettings = decodeSettings(settings.get(
                AnalyzerVisitor.getPrefixedSettingsKey("analyzer.a1a")
        ));
        assertEquals(
                "custom",
                analyzerSettings.get(AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a1a.type"))
        );
        assertThat(
                analyzerSettings.getAsMap(),
                Matchers.hasEntry(
                        AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a1a.tokenizer"),
                        "standard"
                )
        );
    }

    @Test
    public void createAnalyzerWithExtendedBuiltinTokenizer() throws StandardException, IOException {
        Settings settings = executeStatement("CREATE ANALYZER a1b WITH (" +
                "  TOKENIZER myextendedone WITH (" +
                "    type='standard'," +
                "    \"max_token_length\"=8" +
                "  )" +
                ")", null);
        Settings analyzerSettings = decodeSettings(settings.get(AnalyzerVisitor.getPrefixedSettingsKey("analyzer.a1b")));
        assertEquals(
                "a1b_myextendedone",
                analyzerSettings.get(AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a1b.tokenizer"))
        );
        assertEquals(
                "custom",
                analyzerSettings.get(AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a1b.type"))
        );
        Settings tokenizerSettings = decodeSettings(settings.get(AnalyzerVisitor
                .getPrefixedSettingsKey("tokenizer.a1b_myextendedone")));
        assertThat(
                tokenizerSettings.getAsMap(),
                allOf(
                        hasEntry(AnalyzerVisitor.getSettingsKey("index.analysis.tokenizer" +
                                ".a1b_myextendedone.type"), "standard"),
                        hasEntry(AnalyzerVisitor.getSettingsKey("index.analysis.tokenizer" +
                                ".a1b_myextendedone.max_token_length"), "8")
                )
        );
    }

    @Test( expected = SQLParseException.class)
    public void createAnalyzerWithoutTokenizer() throws StandardException {
        executeStatement("CREATE ANALYZER a1c WITH (" +
                "  CHAR_FILTERS (" +
                "     \"html_strip\"" +
                "  )" +
                ")", null);
    }

    @Test
    public void createAnalyzerWithTokenizerAndTokenFilters() throws StandardException, IOException {
        Settings settings = executeStatement("CREATE ANALYZER a2 WITH (" +
                "  TOKENIZER tokenizer2 (" +
                "    type=?," +
                "    \"max_token_length\"=?" +
                "  )," +
                "  token_filters (" +
                "    myfilter with (" +
                "      type='edgeNGram'," +
                "      side='back'," +
                "      \"max_gram\"=4" +
                "    )," +
                "    myotherfilter (" +
                "      type='stop'," +
                "      stopwords=['foo', 'bar', 'baz']" +
                "    )" +
                "  )" +
                ")",
                new Object[]{"standard", 100}
        );
        Settings analyzerSettings = decodeSettings(settings.get(AnalyzerVisitor.getPrefixedSettingsKey("analyzer.a2")));
        assertThat(analyzerSettings.getAsMap(), hasEntry(AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a2.type"), "custom"));
        assertThat(
                analyzerSettings.getAsMap(),
                hasEntry(AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a2.tokenizer"),
                        "a2_tokenizer2")
        );
        assertThat(
                analyzerSettings.getAsArray(AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a2.filter")),
                arrayContainingInAnyOrder("a2_myfilter", "a2_myotherfilter")
        );

        Settings myFilterSettings = decodeSettings(settings.get(AnalyzerVisitor
                .getPrefixedSettingsKey("filter.a2_myfilter")));
        assertThat(
                myFilterSettings.getAsMap(),
                hasEntry(AnalyzerVisitor.getSettingsKey("index.analysis.filter.a2_myfilter.type"),
                        "edgeNGram")
        );

        Settings myOtherFilterSettings = decodeSettings(settings.get(AnalyzerVisitor
                .getPrefixedSettingsKey("filter.a2_myotherfilter")));
        assertThat(
                myOtherFilterSettings.getAsArray(AnalyzerVisitor.getSettingsKey("index.analysis" +
                        ".filter.a2_myotherfilter.stopwords")),
                arrayContainingInAnyOrder("foo", "bar", "baz")
        );
    }

    @Test
    public void createFullCustomAnalyzer() throws StandardException, IOException {
        Settings settings = executeStatement("CREATE ANALYZER a3 WITH (" +
                "  TOKEN_FILTERS WITH (" +
                "    standard," +
                "    germanlowercase WITH (" +
                "       type='lowercase'," +
                "       language='german'" +
                "    )," +
                "    \"trim\"" +
                "  )," +
                "  CHAR_FILTERS WITH (" +
                "    \"html_strip\"," +
                "    mymapping WITH (" +
                "      type=?," +
                "      mapping = [?, ?, ?]" +
                "    )" +
                "  )," +
                "  TOKENIZER tok3 WITH (" +
                "    type='nGram'," +
                "    \"token_chars\"=['letter', 'digit']" +
                "  )" +
                ")", new Object[]{"mapping", "ph=>f", "qu=>q", "foo=>bar"});
        Settings analyzerSettings = decodeSettings(settings.get(AnalyzerVisitor.getPrefixedSettingsKey("analyzer.a3")));
        assertThat(
                analyzerSettings.getAsMap(),
                hasEntry(AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a3.type"), "custom"));
        assertThat(
                analyzerSettings.getAsMap(),
                hasEntry(AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a3.tokenizer"),
                        "a3_tok3")
        );
        assertThat(
                analyzerSettings.getAsArray(AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a3.char_filter")),
                arrayContainingInAnyOrder("html_strip", "a3_mymapping")
        );

        assertThat(settings.getAsMap(), not(hasKey(startsWith(AnalyzerVisitor.getPrefixedSettingsKey("char_filter.html_strip")))));

        Settings myMappingSettings = decodeSettings(settings.get(AnalyzerVisitor
                .getPrefixedSettingsKey("char_filter.a3_mymapping")));
        assertThat(
                myMappingSettings.getAsMap(),
                hasEntry(AnalyzerVisitor.getSettingsKey("index.analysis.char_filter.a3_mymapping" +
                        ".type"), "mapping")
        );
        assertThat(
                myMappingSettings.getAsArray(AnalyzerVisitor.getSettingsKey("index.analysis" +
                        ".char_filter.a3_mymapping.mapping")),
                arrayContainingInAnyOrder("ph=>f", "qu=>q", "foo=>bar")
        );

        assertThat(
                analyzerSettings.getAsArray(AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a3.filter")),
                arrayContainingInAnyOrder("standard", "a3_germanlowercase", "trim")
        );
        assertThat(settings.getAsMap(), allOf(
                not(hasKey(startsWith(AnalyzerVisitor.getPrefixedSettingsKey("filter.trim")))),
                not(hasKey(startsWith(AnalyzerVisitor.getPrefixedSettingsKey("filter.standard"))))
        ));

        Settings germanLowercaseSettings = decodeSettings(settings.get(AnalyzerVisitor
                .getPrefixedSettingsKey("filter.a3_germanlowercase")));
        assertThat(germanLowercaseSettings.getAsMap(), allOf(
                hasEntry(AnalyzerVisitor.getSettingsKey("index.analysis.filter" +
                        ".a3_germanlowercase.type"), "lowercase"),
                hasEntry(AnalyzerVisitor.getSettingsKey("index.analysis.filter" +
                        ".a3_germanlowercase.language"), "german")
            )
        );
    }

    @Test( expected = SQLParseException.class)
    public void createCustomAnalyzerWithInvalidTokenFilters() throws StandardException {
        executeStatement("CREATE ANALYZER a3 WITH (" +
                "  TOKEN_FILTERS WITH (" +
                "    germanlowercase WITH (" +
                "       language='german'" +
                "    )," +
                "    \"trim\"" +
                "  )," +
                "  TOKENIZER tok3 WITH (" +
                "    type='nGram'," +
                "    \"token_chars\"=['letter', 'digit']" +
                "  )" +
                ")", null);
    }

    @Test( expected = SQLParseException.class)
    public void createCustomAnalyzerWithInvalidTokenFilters2() throws StandardException {
        executeStatement("CREATE ANALYZER a3 WITH (" +
                "  TOKEN_FILTERS WITH (" +
                "    germanlowercase," +
                "    \"trim\"" +
                "  )," +
                "  TOKENIZER tok3 WITH (" +
                "    type='nGram'," +
                "    \"token_chars\"=['letter', 'digit']" +
                "  )" +
                ")", null);
    }

    @Test( expected = SQLParseException.class)
    public void overrideDefaultAnalyzer() throws StandardException {
        executeStatement("CREATE ANALYZER \"default\" WITH (" +
                "  TOKENIZER whitespace" +
                ")", null);
    }

    @Test(expected = SQLParseException.class)
    public void overrideBuiltInAnalyzer() throws StandardException {
        executeStatement("CREATE ANALYZER \"keyword\" WITH (" +
                "  char_filters WITH (" +
                "    html_strip" +
                "  )," +
                "  tokenizer standard" +
                ")", null);
    }

    @Test
    public void testGetStmt() throws StandardException {
        String stmt = "CREATE ANALYZER source1 WITH( TOKENIZER " +
                "whitespace)";
        Settings settings = executeStatement(stmt, new Object[0]);
        assertThat(
                settings.getAsMap(),
                hasEntry(
                        AnalyzerVisitor.getPrefixedSettingsKey("analyzer.source1._sql_stmt"),
                        "CREATE ANALYZER source1 WITH (TOKENIZER whitespace)"
                )
        );
    }

    @Test
    public void testGetStmtWithParameters() throws StandardException {
        String stmt = "CREATE ANALYZER source1 WITH( " +
                "   TOKENIZER whitespace," +
                "   CHAR_FILTERS WITH(" +
                "       a WITH (" +
                "           type=?," +
                "           b=[?, ?, ?]" +
                "       )" +
                "   )"  +
                ")";
        Settings settings = executeStatement(stmt, new Object[]{"html_strip", 1, "2", Math.PI});
        String source = settings.get(AnalyzerVisitor.getPrefixedSettingsKey("analyzer.source1" +
                "._sql_stmt"));
        assertEquals(
                "CREATE ANALYZER source1 WITH (TOKENIZER whitespace, CHAR_FILTERS WITH " +
                        "(a WITH (\"b\"=[1,'2',3.141592653589793],\"type\"='html_strip')))",
                source
        );
    }
}

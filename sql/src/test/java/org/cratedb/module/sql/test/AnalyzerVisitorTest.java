package org.cratedb.module.sql.test;

import org.cratedb.action.AnalyzerService;
import org.cratedb.action.parser.AnalyzerVisitor;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.SQLParserException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.hamcrest.Matchers;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
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
        AnalyzerService analyzerService = new AnalyzerService(mockedClusterService, new IndicesAnalysisService(ImmutableSettings.EMPTY));
        NodeExecutionContext nodeExecutionContext = mock(NodeExecutionContext.class);
        when(nodeExecutionContext.analyzerService()).thenReturn(analyzerService);
        ParsedStatement parsedStatement;
        try {
            parsedStatement = new ParsedStatement(stmt, args, nodeExecutionContext);
        } catch (SQLParserException e){
            System.err.println(stmt);
            throw e;
        }
        assert parsedStatement.visitor() instanceof AnalyzerVisitor;
        return ((AnalyzerVisitor)parsedStatement.visitor()).buildSettings();
    }

    @Test
    public void createAnalyzerWithBuiltinTokenizer() throws StandardException {
        Settings settings = executeStatement("CREATE ANALYZER a1a WITH (" +
                "  TOKENIZER standard" +
                ")", null);

        assertEquals(
                "standard",
                settings.get(AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a1a.tokenizer"))
        );
        assertEquals(
                "custom",
                settings.get(AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a1a.type"))
        );
        assertThat(
                settings.getAsMap(),
                not(hasKey(startsWith(AnalyzerVisitor.getSettingsKey("index.analysis.tokenizer.standard"))))
        );
    }

    @Test
    public void createAnalyzerWithExtendedBuiltinTokenizer() throws StandardException {
        Settings settings = executeStatement("CREATE ANALYZER a1b WITH (" +
                "  TOKENIZER myextendedone WITH (" +
                "    type='standard'," +
                "    \"max_token_length\"=8" +
                "  )" +
                ")", null);
        assertEquals(
                "myextendedone",
                settings.get(AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a1b.tokenizer"))
        );
        assertEquals(
                "custom",
                settings.get(AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a1b.type"))
        );
        assertThat(
                settings.getAsMap(),
                allOf(
                        hasEntry(AnalyzerVisitor.getSettingsKey("index.analysis.tokenizer.myextendedone.type"), "standard"),
                        hasEntry(AnalyzerVisitor.getSettingsKey("index.analysis.tokenizer.myextendedone.max_token_length"), "8")
                )
        );
    }

    @Test( expected = StandardException.class)
    public void createAnalyzerWithoutTokenizer() throws StandardException {
        executeStatement("CREATE ANALYZER a1c WITH (" +
                "  CHAR_FILTERS (" +
                "     \"html_strip\"" +
                "  )" +
                ")", null);
    }

    @Test
    public void createAnalyzerWithTokenizerAndTokenFilters() throws StandardException {
        Settings settings = executeStatement("CREATE ANALYZER a2 WITH (" +
                "  TOKENIZER tokenizer2 (" +
                "    type='standard'," +
                "    \"max_token_length\"=100" +
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
                null
        );
        assertThat(settings.getAsMap(), hasEntry(AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a2.type"), "custom"));
        assertThat(settings.getAsMap(), hasEntry(AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a2.tokenizer"), "tokenizer2"));
        assertThat(settings.getAsArray(AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a2.filter")), arrayContainingInAnyOrder("myfilter", "myotherfilter"));

        assertThat(settings.getAsMap(), hasEntry(AnalyzerVisitor.getSettingsKey("index.analysis.filter.myfilter.type"), "edgeNGram"));
        assertThat(settings.getAsArray(AnalyzerVisitor.getSettingsKey("index.analysis.filter.myotherfilter.stopwords")), arrayContainingInAnyOrder("foo", "bar", "baz"));
    }

    @Test
    public void createFullCustomAnalyzer() throws StandardException {
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
                "      type='mapping'," +
                "      mapping = ['ph=>f', 'qu=>q', 'foo=>bar']" +
                "    )" +
                "  )," +
                "  TOKENIZER tok3 WITH (" +
                "    type='nGram'," +
                "    \"token_chars\"=['letter', 'digit']" +
                "  )" +
                ")", null);
        assertThat(settings.getAsMap(), hasEntry(AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a3.type"), "custom"));
        assertThat(settings.getAsMap(), hasEntry(AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a3.tokenizer"), "tok3"));

        assertThat(settings.getAsArray(AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a3.char_filter")), arrayContainingInAnyOrder("html_strip", "mymapping"));
        assertThat(settings.getAsMap(), not(hasKey(startsWith(AnalyzerVisitor.getSettingsKey("index.analysis.char_filter.html_strip")))));
        assertThat(settings.getAsMap(), hasEntry(AnalyzerVisitor.getSettingsKey("index.analysis.char_filter.mymapping.type"), "mapping"));
        assertThat(settings.getAsArray(AnalyzerVisitor.getSettingsKey("index.analysis.char_filter.mymapping.mapping")), arrayContainingInAnyOrder("ph=>f", "qu=>q", "foo=>bar"));

        assertThat(settings.getAsArray(AnalyzerVisitor.getSettingsKey("index.analysis.analyzer.a3.filter")), arrayContainingInAnyOrder("standard", "germanlowercase", "trim"));
        assertThat(settings.getAsMap(), allOf(
                not(hasKey(startsWith(AnalyzerVisitor.getSettingsKey("index.analysis.filter.trim")))),
                not(hasKey(startsWith(AnalyzerVisitor.getSettingsKey("index.analysis.filter.standard"))))
        ));
        assertThat(settings.getAsMap(), Matchers.hasEntry(AnalyzerVisitor.getSettingsKey("index.analysis.filter.germanlowercase.type"), "lowercase"));
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
    public void createCustomAnalyzerWithInvalidTOkenFilters2() throws StandardException {
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
}

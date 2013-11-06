package org.cratedb.service;

import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.TableUnknownException;
import org.elasticsearch.cluster.AbstractZenNodesTests;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.internal.InternalNode;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class SQLParseServiceTest extends AbstractZenNodesTests {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }
    private static InternalNode node = null;
    private static SQLParseService sqlParseService = null;

    @Before
    public void startNode() {
        if (node == null) {
            node = (InternalNode) startNode("node1", ImmutableSettings.EMPTY);
            sqlParseService = node.injector().getInstance(SQLParseService.class);
        }
    }

    @AfterClass
    public static void stopNode() {
        if (node != null) {
            node.close();
            node = null;
            sqlParseService = null;
        }
    }

    @Test(expected = TableUnknownException.class)
    public void testParse() throws Exception {
        ParsedStatement stmt = sqlParseService.parse("select mycol from mytable where mycol = 1");

        // this test just ensures that the ParseService calls a Visitor and begins to parse the statement
        // the actual result isn't really enforced. Feel free to change the expected TableUnknownException
        // later on if something else makes more sense.
    }

    @Test
    public void testUnparseSelect() throws Exception {
        String unparsed = sqlParseService.unparse("SelECT mycol froM mytable wheRe a IS not" +
                " null order by mycol", null);
        assertEquals(
                "SELECT mycol FROM mytable WHERE NOT (a IS NULL) ORDER BY mycol",
                unparsed
        );
    }

    @Test
    public void testUnparseSelectGroupBy() throws Exception {
        String unparsed = sqlParseService.unparse("SELECT sum(mycol) FROM mytable group by " +
                "mycol having a > ?",
                new Object[]{Math.PI});
        assertEquals(
                "SELECT SUM(mycol) FROM mytable GROUP BY mycol HAVING a > 3.141592653589793",
                unparsed
        );
    }

    @Test
    public void testUnparseCreateAnalyzer() throws Exception {
        String unparsed = sqlParseService.unparse("CREATE ANALYZER mygerman EXTENDS german WITH" +
                "( stopwords=['der', ?, ?, 'wer', 'wie', 'was'])", new Object[]{"die", "das"});
        assertEquals(
                "CREATE ANALYZER mygerman EXTENDS german WITH (\"stopwords\"=['der','die','das','wer','wie','was'])",
                unparsed
        );
    }

    @Test
    public void testUnparseCreateAnalyzer2() throws Exception {
        String unparsed = sqlParseService.unparse("CREaTE ANALYZER mycustom (" +
                "tokenizer whitespace," +
                "char_filters (" +
                "  html_strip," +
                "  mymapping (" +
                "    type='mapping'," +
                "    mappings=['a=>b', ?]" +
                "  )" +
                ")," +
                "token_filters WITH (" +
                "  snowball," +
                "  myfilter with (" +
                "    type='ngram'," +
                "    max_ngram=?" +
                "  )" +
                ")" +
                ")", new Object[]{"b=>c", 4});
        assertEquals(
                "CREATE ANALYZER mycustom WITH (" +
                        "TOKENIZER whitespace, " +
                        "TOKEN_FILTERS WITH (snowball, myfilter WITH (\"max_ngram\"=4,\"type\"='ngram')), " +
                        "CHAR_FILTERS WITH (html_strip, mymapping WITH (\"mappings\"=['a=>b','b=>c'],\"type\"='mapping')))",
                unparsed
        );
    }
}

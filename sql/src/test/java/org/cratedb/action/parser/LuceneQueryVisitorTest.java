package org.cratedb.action.parser;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.SQLParser;
import org.cratedb.sql.parser.parser.StatementNode;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentFieldMappers;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperTestUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LuceneQueryVisitorTest {

    DocumentFieldMappers documentFieldMappers;

    @Before
    public void setUp() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("default")
                .startObject("properties")
                    .startObject("long_field").field("type", "long").endObject()
                    .startObject("int_field").field("type", "integer").endObject()
                    .startObject("float_field").field("type", "float").endObject()
                .endObject()
            .endObject()
            .endObject()
            .string();
        DocumentMapper documentMapper = MapperTestUtils.newParser().parse(mapping);
        documentFieldMappers = documentMapper.mappers();
    }

    public void tearDown() throws Exception {
    }

    @Test
    public void testMatchAllQueryGeneration() throws Exception {
        WhereClauseVisitor visitor = getLuceneQueryVisitor(
            "select mycol from mytable"
        );

        Query query = visitor.query();
        assertTrue(query instanceof MatchAllDocsQuery);
    }

    @Test
    public void testSimpleTermQueryGeneration() throws Exception {
        WhereClauseVisitor visitor = getLuceneQueryVisitor(
            "select mycol from mytable where othercol = 1"
        );

        Query query = visitor.query();
        assertTrue(query instanceof TermQuery);
        Term term = ((TermQuery) query).getTerm();
        assertEquals("othercol", term.field());
        assertEquals("1", term.text());

    }

    //@Test
    //public void testIsNullQueryGeneration() throws Exception {
    //    WhereClauseVisitor visitor = getLuceneQueryVisitor(
    //        "select mycol from mytable where othercol is null"
    //    );

    //    Query query = visitor.query();
    //    String tree = createTree(query);
    //    String expected = "NumericRangeQuery: null to 1\n";
    //    assertEquals(expected, tree);
    //}

    @Test
    public void testBoolQueryGeneration() throws Exception {
        WhereClauseVisitor visitor = getLuceneQueryVisitor(
            "select 1 from t where x = 1 and (y = 2 or y = 3 or y = 4)"
        );

        Query query = visitor.query();
        String tree = createTree(query);
        String expected = "BooleanQuery/1:\n" +
            "  MUST\n" +
            "  TermQuery: x:1\n" +
            "  MUST\n" +
            "  BooleanQuery/1:\n" +
            "    SHOULD\n" +
            "    BooleanQuery/1:\n" +
            "      SHOULD\n" +
            "      TermQuery: y:2\n" +
            "      SHOULD\n" +
            "      TermQuery: y:3\n" +
            "    SHOULD\n" +
            "    TermQuery: y:4\n";
        assertEquals(expected, tree);
    }

    @Test
    public void testBoolQueryGeneration2() throws Exception {
        WhereClauseVisitor visitor = getLuceneQueryVisitor(
            "select 1 from t where x = 1 and y = 2 or y = 3 or y = 4"
        );

        Query query = visitor.query();
        String tree = createTree(query);
        String expected = "BooleanQuery/1:\n" +
            "  SHOULD\n" +
            "  BooleanQuery/1:\n" +
            "    SHOULD\n" +
            "    BooleanQuery/1:\n" +
            "      MUST\n" +
            "      TermQuery: x:1\n" +
            "      MUST\n" +
            "      TermQuery: y:2\n" +
            "    SHOULD\n" +
            "    TermQuery: y:3\n" +
            "  SHOULD\n" +
            "  TermQuery: y:4\n";
        assertEquals(expected, tree);
    }

    @Test
    public void testRangeQueryGenerationLt() throws Exception {
        WhereClauseVisitor visitor = getLuceneQueryVisitor(
            "select 1 from t where long_field < 1"
        );

        Query query = visitor.query();
        String tree = createTree(query);
        String expected = "NumericRangeQuery: null to 1\n";
        assertEquals(expected, tree);
    }

    @Test
    public void testRangeQueryGenerationInt() throws Exception {
        WhereClauseVisitor visitor = getLuceneQueryVisitor(
            "select 1 from t where int_field < 1"
        );

        Query query = visitor.query();
        String tree = createTree(query);
        String expected = "NumericRangeQuery: null to 1\n";
        assertEquals(expected, tree);
    }

    @Test
    public void testRangeQueryGenerationFloat() throws Exception {
        WhereClauseVisitor visitor = getLuceneQueryVisitor(
            "select 1 from t where float_field < 1.2"
        );

        Query query = visitor.query();
        String tree = createTree(query);
        String expected = "NumericRangeQuery: null to 1.2\n";
        assertEquals(expected, tree);
    }

    @Test
    public void testRangeQueryGenerationLte() throws Exception {
        WhereClauseVisitor visitor = getLuceneQueryVisitor(
            "select 1 from t where long_field <= 1"
        );

        Query query = visitor.query();
        String tree = createTree(query);
        String expected = "NumericRangeQuery: null to (incl) 1\n";
        assertEquals(expected, tree);
    }

    @Test
    public void testRangeQueryGenerationGt() throws Exception {
        WhereClauseVisitor visitor = getLuceneQueryVisitor(
            "select 1 from t where long_field > 1"
        );

        Query query = visitor.query();
        String tree = createTree(query);
        String expected = "NumericRangeQuery: 1 to null\n";
        assertEquals(expected, tree);
    }

    @Test
    public void testRangeQueryGenerationGte() throws Exception {
        WhereClauseVisitor visitor = getLuceneQueryVisitor(
            "select 1 from t where long_field >= 1"
        );

        Query query = visitor.query();
        String tree = createTree(query);
        String expected = "NumericRangeQuery: (incl) 1 to null\n";
        assertEquals(expected, tree);
    }

    @Test
    public void testRangeQueryGenerationLtYoda() throws Exception {
        WhereClauseVisitor visitor = getLuceneQueryVisitor(
            "select 1 from t where 1 > long_field"
        );

        Query query = visitor.query();
        String tree = createTree(query);
        String expected = "NumericRangeQuery: null to 1\n";
        assertEquals(expected, tree);
    }

    @Test
    public void testRangeQueryGenerationLteYoda() throws Exception {
        WhereClauseVisitor visitor = getLuceneQueryVisitor(
            "select 1 from t where 1 >= long_field"
        );

        Query query = visitor.query();
        String tree = createTree(query);
        String expected = "NumericRangeQuery: null to (incl) 1\n";
        assertEquals(expected, tree);
    }

    @Test
    public void testRangeQueryGenerationGtYoda() throws Exception {
        WhereClauseVisitor visitor = getLuceneQueryVisitor(
            "select 1 from t where 1 < long_field"
        );

        Query query = visitor.query();
        String tree = createTree(query);
        String expected = "NumericRangeQuery: 1 to null\n";
        assertEquals(expected, tree);
    }

    @Test
    public void testRangeQueryGenerationGteYoda() throws Exception {
        WhereClauseVisitor visitor = getLuceneQueryVisitor(
            "select 1 from t where 1 <= long_field"
        );

        Query query = visitor.query();
        String tree = createTree(query);
        String expected = "NumericRangeQuery: (incl) 1 to null\n";
        assertEquals(expected, tree);
    }

    private WhereClauseVisitor getLuceneQueryVisitor(String statement) throws StandardException {
        WhereClauseVisitor visitor = new WhereClauseVisitor(documentFieldMappers);
        SQLParser parser = new SQLParser();
        StatementNode statementNode = parser.parseStatement(statement);
        statementNode.accept(visitor);
        return visitor;
    }

    private String createTree(Query query) {
        return createTree(query, 0);
    }

    private String createTree(Query query, int indent) {
        StringBuilder sb = new StringBuilder();
        sb.append(i(indent));

        if (query instanceof TermQuery) {
            sb.append("TermQuery: ");
            sb.append( ((TermQuery) query).getTerm().toString());
            sb.append("\n");
        } else if (query instanceof NumericRangeQuery) {
            NumericRangeQuery rangeQuery = (NumericRangeQuery)query;
            sb.append("NumericRangeQuery: ");
            if (rangeQuery.includesMin()) {
                sb.append("(incl) ");
            }
            sb.append(rangeQuery.getMin());
            sb.append(" to ");
            if (rangeQuery.includesMax()) {
                sb.append("(incl) ");
            }
            sb.append(rangeQuery.getMax());
            sb.append("\n");
        } else if (query instanceof BooleanQuery) {
            BooleanQuery booleanQuery = (BooleanQuery)query;
            sb.append("BooleanQuery/");
            sb.append(booleanQuery.getMinimumNumberShouldMatch());
            sb.append(":\n");

            for (BooleanClause clause : booleanQuery.getClauses()) {
                sb.append(i(indent + 2));
                sb.append(o(clause.getOccur()));
                sb.append("\n");
                sb.append(createTree(clause.getQuery(), indent + 2));
            }
        } else {
            sb.append(query.toString());
        }
        return sb.toString();
    }

    private String o(BooleanClause.Occur occur) {
        switch (occur) {
            case MUST:
                return "MUST";
            case SHOULD:
                return "SHOULD";
            case MUST_NOT:
                return "MUST_NOT";
        }

        return "INVALID";
    }

    private String i(int num) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < num; i++) {
            sb.append(" ");
        }

        return sb.toString();
    }
}

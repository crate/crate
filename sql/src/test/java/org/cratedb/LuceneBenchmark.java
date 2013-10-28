package org.cratedb;


import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.bytebuffer.ByteBufferDirectory;
import org.apache.lucene.util.Version;
import org.cratedb.sql.parser.parser.SQLParser;
import org.elasticsearch.ElasticsearchLuceneTestCase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.ArrayList;
import java.util.List;

public class LuceneBenchmark extends ElasticsearchLuceneTestCase {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);


    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @BenchmarkOptions(benchmarkRounds = 500)
    @Test
    public void testReCreateAfterDirty() throws Exception {

        Directory index = new RAMDirectory();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_44, null);
        IndexWriter iw = new IndexWriter(index, indexWriterConfig);

        List<Document> documents = new ArrayList<>(20);
        documents.add(getDoc(iw, "test1", 5, 3));
        documents.add(getDoc(iw, "foo", 6, 8));
        documents.add(getDoc(iw, "bar", 2, 3));
        documents.add(getDoc(iw, "bar3", 2, 3));
        documents.add(getDoc(iw, "foobar", 2, 3));
        documents.add(getDoc(iw, "characters", 2, 3));
        documents.add(getDoc(iw, "locations", 2, 3));
        documents.add(getDoc(iw, "names", 33, 2));
        documents.add(getDoc(iw, "addresses", 2, 3));
        documents.add(getDoc(iw, "whatever", 7, 9));
        documents.add(getDoc(iw, "anothertable", 5, 2));
        documents.add(getDoc(iw, "out_of_ideas", 10, 2));
        documents.add(getDoc(iw, "one_more", 20, 5));
        documents.add(getDoc(iw, "almost_there", 30, 4));
        documents.add(getDoc(iw, "not_quite", 100, 9));
        documents.add(getDoc(iw, "just_a_few_more", 4, 2));
        documents.add(getDoc(iw, "almost_done", 4, 2));
        documents.add(getDoc(iw, "and_finally", 5, 1));
        documents.add(getDoc(iw, "the_last", 5, 1));
        documents.add(getDoc(iw, "one", 11, 8));

        iw.addDocuments(documents);
        iw.close(false);

        Query q = new TermQuery(new Term("tableName", "one_more"));

        DirectoryReader reader = DirectoryReader.open(index);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopScoreDocCollector collector = TopScoreDocCollector.create(10, true);
        searcher.search(q, collector);
        ScoreDoc[] hits = collector.topDocs().scoreDocs;

        assertEquals(1, hits.length);

        index = new RAMDirectory();
        indexWriterConfig = new IndexWriterConfig(Version.LUCENE_44, null);
        iw = new IndexWriter(index, indexWriterConfig);
        iw.addDocuments(documents);
        iw.close(false);

        q = new TermQuery(new Term("tableName", "one_more"));

        reader = DirectoryReader.open(index);
        searcher = new IndexSearcher(reader);
        collector = TopScoreDocCollector.create(10, true);
        searcher.search(q, collector);
        hits = collector.topDocs().scoreDocs;

        assertEquals(1, hits.length);
    }

    @BenchmarkOptions(benchmarkRounds = 500)
    @Test
    public void testDeleteAndWriteAgain() throws Exception {

        Directory index = new ByteBufferDirectory();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_44, null);
        IndexWriter iw = new IndexWriter(index, indexWriterConfig);

        List<Document> documents = new ArrayList<>(20);
        documents.add(getDoc(iw, "test1", 5, 3));
        documents.add(getDoc(iw, "foo", 6, 8));
        documents.add(getDoc(iw, "bar", 2, 3));
        documents.add(getDoc(iw, "bar3", 2, 3));
        documents.add(getDoc(iw, "foobar", 2, 3));
        documents.add(getDoc(iw, "characters", 2, 3));
        documents.add(getDoc(iw, "locations", 2, 3));
        documents.add(getDoc(iw, "names", 33, 2));
        documents.add(getDoc(iw, "addresses", 2, 3));
        documents.add(getDoc(iw, "whatever", 7, 9));
        documents.add(getDoc(iw, "anothertable", 5, 2));
        documents.add(getDoc(iw, "out_of_ideas", 10, 2));
        documents.add(getDoc(iw, "one_more", 20, 5));
        documents.add(getDoc(iw, "almost_there", 30, 4));
        documents.add(getDoc(iw, "not_quite", 100, 9));
        documents.add(getDoc(iw, "just_a_few_more", 4, 2));
        documents.add(getDoc(iw, "almost_done", 4, 2));
        documents.add(getDoc(iw, "and_finally", 5, 1));
        documents.add(getDoc(iw, "the_last", 5, 1));
        documents.add(getDoc(iw, "one", 11, 8));

        iw.addDocuments(documents);
        iw.commit();

        Query q = new TermQuery(new Term("tableName", "one_more"));

        DirectoryReader reader = DirectoryReader.open(index);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopScoreDocCollector collector = TopScoreDocCollector.create(10, true);
        searcher.search(q, collector);
        ScoreDoc[] hits = collector.topDocs().scoreDocs;

        assertEquals(1, hits.length);

        iw.deleteAll();
        iw.addDocuments(documents);

        q = new TermQuery(new Term("tableName", "one_more"));

        reader = DirectoryReader.open(index);
        searcher = new IndexSearcher(reader);
        collector = TopScoreDocCollector.create(10, true);
        searcher.search(q, collector);
        hits = collector.topDocs().scoreDocs;

        assertEquals(1, hits.length);
    }

    private static Document getDoc(IndexWriter w, String tableName,
                               int numberOfShards, int numberOfReplicas) throws Exception {
        Document doc = new Document();
        doc.add(new StringField("tableName", tableName, Field.Store.NO));
        doc.add(new NumericDocValuesField("numberOfShards", numberOfShards));
        doc.add(new IntField("numberOfReplicas", numberOfReplicas, Field.Store.NO));
        return doc;
    }
}

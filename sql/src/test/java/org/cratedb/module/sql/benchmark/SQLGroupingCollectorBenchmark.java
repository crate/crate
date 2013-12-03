package org.cratedb.module.sql.benchmark;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import org.apache.lucene.index.AtomicReaderContext;
import org.cratedb.action.GroupByFieldLookup;
import org.cratedb.action.groupby.SQLGroupingCollector;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.GroupByOnArrayUnsupportedException;
import org.cratedb.stubs.HitchhikerMocks;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;

public class SQLGroupingCollectorBenchmark extends AbstractBenchmark {

    private static int NUM_DOCS = 300000;
    private static int NUM_TERMS = 100000;

    private ParsedStatement stmt;
    private DummyLookup dummyLookup;
    private static int[] fakeDocs;
    private static String[] terms;
    private static SecureRandom random = new SecureRandom();

    @BeforeClass
    public static void prepareData(){
        terms = new String[NUM_TERMS];
        for (int i = 0; i < terms.length; i++) {
            terms[i] = new BigInteger(130, random).toString(32);
        }
        fakeDocs = new int[NUM_DOCS];
        for (int i = 0; i < fakeDocs.length; i++) {
            fakeDocs[i] = random.nextInt(terms.length);
        }
    }

    @Before
    public void prepare() throws Exception {
        SQLParseService parseService = new SQLParseService(HitchhikerMocks.nodeExecutionContext());
        //stmt = parseService.parse("select count(*), min(age) from characters group by race " +
        //        "order by count(*) limit 4");
        stmt = parseService.parse("select race from characters group by race limit 4");
        dummyLookup = new DummyLookup(terms, fakeDocs);

    }

    @BenchmarkOptions(benchmarkRounds = 15)
    @Test
    public void testGroupingCollector() throws Exception {
        SQLGroupingCollector collector = new SQLGroupingCollector(
            stmt,
            dummyLookup,
            HitchhikerMocks.aggFunctionMap,
            new String[] {"r1", "r2", "r3", "r4" }
        );
        for (int i = 0; i < fakeDocs.length; i++) {
            collector.collect(i);
        }
    }

    private class DummyLookup implements GroupByFieldLookup {

        private final int[] docs;
        private int docId;
        private final String[] terms;

        public DummyLookup(String[] terms, int[] docs) {
            this.docs = docs;
            this.terms = terms;
        }

        @Override
        public void setNextDocId(int doc) {
            this.docId = doc;
        }

        @Override
        public void setNextReader(AtomicReaderContext context) {
        }

        @Override
        public Object lookupField(String columnName) throws IOException, GroupByOnArrayUnsupportedException {
            return terms[docs[docId]];
        }
    }
}

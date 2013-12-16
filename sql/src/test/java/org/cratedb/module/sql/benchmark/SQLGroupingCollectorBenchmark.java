package org.cratedb.module.sql.benchmark;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.cratedb.action.FieldLookup;
import org.cratedb.action.groupby.SQLGroupingCollector;
import org.cratedb.action.groupby.grouping.GroupNode;
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
import java.util.HashMap;
import java.util.Map;

public class SQLGroupingCollectorBenchmark extends AbstractBenchmark {

    private static int NUM_DOCS = 300000;
    private static int NUM_TERMS = 100000;
    private static BytesRef[] bterms;

    private ParsedStatement stmt;
    private DummyLookup dummyLookup;
    private static int[] fakeDocs;
    private static String[] terms;
    private static SecureRandom random = new SecureRandom();

    @BeforeClass
    public static void prepareData(){
        bterms = new BytesRef[NUM_TERMS];
        terms = new String[NUM_TERMS];
        for (int i = 0; i < terms.length; i++) {
            terms[i] = new BigInteger(130, random).toString(32);
            bterms[i] = new BytesRef(terms[i]);
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
        stmt = parseService.parse("select a,b from characters group by a,b limit 4");
        dummyLookup = new DummyLookup(terms, fakeDocs);

    }



    @Test
    public void testHierarchicalGroupKey() throws Exception {

        Map<BytesRef, GroupNode<BytesRef, Object, Object>> nodes = new HashMap<>();

        for (String term: terms){
            for (BytesRef bterm: bterms){

            }
        }


    }

//    @BenchmarkOptions(benchmarkRounds = 10)
//    @Test
//    public void testStringGroupingCollector() throws Exception {
//        SQLGroupingCollector collector = new SQLGroupingCollector(
//            stmt,
//            dummyLookup,
//            HitchhikerMocks.aggFunctionMap,
//            new String[] {"r1", "r2", "r3", "r4" }
//        );
//        for (int i = 0; i < fakeDocs.length; i++) {
//            collector.collect(i);
//        }
//    }

    private class DummyLookup implements FieldLookup {

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
            switch (columnName) {
                case "a": return terms[docs[docId]];
                default: return bterms[bterms.length - docs[docId]-1];
            }
        }
    }
}

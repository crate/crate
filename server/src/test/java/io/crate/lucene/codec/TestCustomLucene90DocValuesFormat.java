/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.lucene.codec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.codecs.asserting.AssertingCodec;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.index.codec.CrateCodec;

public class TestCustomLucene90DocValuesFormat extends BaseDocValuesFormatTestCase {

    private final Codec codec = new CrateCodec(Lucene101Codec.Mode.BEST_SPEED);

    @Override
    protected Codec getCodec() {
        return codec;
    }

    // TODO: these big methods can easily blow up some of the other ram-hungry codecs...
    // for now just keep them here, as we want to test this for this format.

    public void testSortedSetVariableLengthBigVsStoredFields() throws Exception {
        int numIterations = atLeast(1);
        for (int i = 0; i < numIterations; i++) {
            int numDocs = TEST_NIGHTLY ? atLeast(100) : atLeast(10);
            doTestSortedSetVsStoredFields(numDocs, 1, 32766, 16, 100);
        }
    }

    @Nightly
    public void testSortedSetVariableLengthManyVsStoredFields() throws Exception {
        int numIterations = atLeast(1);
        for (int i = 0; i < numIterations; i++) {
            doTestSortedSetVsStoredFields(TestUtil.nextInt(random(), 1024, 2049), 1, 500, 16, 100);
        }
    }

    public void testSortedVariableLengthBigVsStoredFields() throws Exception {
        int numIterations = atLeast(1);
        for (int i = 0; i < numIterations; i++) {
            doTestSortedVsStoredFields(atLeast(100), 1d, 1, 32766);
        }
    }

    @Nightly
    public void testSortedVariableLengthManyVsStoredFields() throws Exception {
        int numIterations = atLeast(1);
        for (int i = 0; i < numIterations; i++) {
            doTestSortedVsStoredFields(TestUtil.nextInt(random(), 1024, 2049), 1d, 1, 500);
        }
    }

    @Nightly
    public void testTermsEnumFixedWidth() throws Exception {
        int numIterations = atLeast(1);
        for (int i = 0; i < numIterations; i++) {
            doTestTermsEnumRandom(
                TestUtil.nextInt(random(), 1025, 5121),
                () -> TestUtil.randomSimpleString(random(), 10, 10));
        }
    }

    @Nightly
    public void testTermsEnumVariableWidth() throws Exception {
        int numIterations = atLeast(1);
        for (int i = 0; i < numIterations; i++) {
            doTestTermsEnumRandom(
                TestUtil.nextInt(random(), 1025, 5121),
                () -> TestUtil.randomSimpleString(random(), 1, 500));
        }
    }

    @Nightly
    public void testTermsEnumRandomMany() throws Exception {
        int numIterations = atLeast(1);
        for (int i = 0; i < numIterations; i++) {
            doTestTermsEnumRandom(
                TestUtil.nextInt(random(), 1025, 8121),
                () -> TestUtil.randomSimpleString(random(), 1, 500));
        }
    }

    @Nightly
    public void testTermsEnumLongSharedPrefixes() throws Exception {
        int numIterations = atLeast(1);
        for (int i = 0; i < numIterations; i++) {
            doTestTermsEnumRandom(
                TestUtil.nextInt(random(), 1025, 5121),
                () -> {
                    char[] chars = new char[random().nextInt(500)];
                    Arrays.fill(chars, 'a');
                    if (chars.length > 0) {
                        chars[random().nextInt(chars.length)] = 'b';
                    }
                    return new String(chars);
                });
        }
    }

    public void testSparseDocValuesVsStoredFields() throws Exception {
        int numIterations = atLeast(1);
        for (int i = 0; i < numIterations; i++) {
            doTestSparseDocValuesVsStoredFields();
        }
    }

    private void doTestSparseDocValuesVsStoredFields() throws Exception {
        final long[] values = new long[TestUtil.nextInt(random(), 1, 500)];
        for (int i = 0; i < values.length; ++i) {
            values[i] = random().nextLong();
        }

        Directory dir = newFSDirectory(createTempDir());
        IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
        conf.setMergeScheduler(new SerialMergeScheduler());
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);

        // sparse compression is only enabled if less than 1% of docs have a value
        final int avgGap = 100;

        final int numDocs = atLeast(200);
        for (int i = random().nextInt(avgGap * 2); i >= 0; --i) {
            writer.addDocument(new Document());
        }
        final int maxNumValuesPerDoc = random().nextBoolean() ? 1 : TestUtil.nextInt(random(), 2, 5);
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();

            // single-valued
            long docValue = values[random().nextInt(values.length)];
            doc.add(new NumericDocValuesField("numeric", docValue));
            doc.add(new SortedDocValuesField("sorted", new BytesRef(Long.toString(docValue))));
            doc.add(new BinaryDocValuesField("binary", new BytesRef(Long.toString(docValue))));
            doc.add(new StoredField("value", docValue));

            // multi-valued
            final int numValues = TestUtil.nextInt(random(), 1, maxNumValuesPerDoc);
            for (int j = 0; j < numValues; ++j) {
                docValue = values[random().nextInt(values.length)];
                doc.add(new SortedNumericDocValuesField("sorted_numeric", docValue));
                doc.add(new SortedSetDocValuesField("sorted_set", new BytesRef(Long.toString(docValue))));
                doc.add(new StoredField("values", docValue));
            }

            writer.addDocument(doc);

            // add a gap
            for (int j = TestUtil.nextInt(random(), 0, avgGap * 2); j >= 0; --j) {
                writer.addDocument(new Document());
            }
        }

        if (random().nextBoolean()) {
            writer.forceMerge(1);
        }

        final IndexReader indexReader = writer.getReader();
        writer.close();

        for (LeafReaderContext context : indexReader.leaves()) {
            final LeafReader reader = context.reader();
            final NumericDocValues numeric = DocValues.getNumeric(reader, "numeric");

            final SortedDocValues sorted = DocValues.getSorted(reader, "sorted");

            final BinaryDocValues binary = DocValues.getBinary(reader, "binary");

            final SortedNumericDocValues sortedNumeric =
                DocValues.getSortedNumeric(reader, "sorted_numeric");

            final SortedSetDocValues sortedSet = DocValues.getSortedSet(reader, "sorted_set");

            StoredFields storedFields = reader.storedFields();
            for (int i = 0; i < reader.maxDoc(); ++i) {
                final Document doc = storedFields.document(i);
                final IndexableField valueField = doc.getField("value");
                final Long value = valueField == null ? null : valueField.numericValue().longValue();

                if (value == null) {
                    assertTrue(numeric.docID() + " vs " + i, numeric.docID() < i);
                } else {
                    assertEquals(i, numeric.nextDoc());
                    assertEquals(i, binary.nextDoc());
                    assertEquals(i, sorted.nextDoc());
                    assertEquals(value.longValue(), numeric.longValue());
                    assertTrue(sorted.ordValue() >= 0);
                    assertEquals(new BytesRef(Long.toString(value)), sorted.lookupOrd(sorted.ordValue()));
                    assertEquals(new BytesRef(Long.toString(value)), binary.binaryValue());
                }

                final IndexableField[] valuesFields = doc.getFields("values");
                if (valuesFields.length == 0) {
                    assertTrue(sortedNumeric.docID() + " vs " + i, sortedNumeric.docID() < i);
                } else {
                    final Set<Long> valueSet = new HashSet<>();
                    for (IndexableField sf : valuesFields) {
                        valueSet.add(sf.numericValue().longValue());
                    }

                    assertEquals(i, sortedNumeric.nextDoc());
                    assertEquals(valuesFields.length, sortedNumeric.docValueCount());
                    for (int j = 0; j < sortedNumeric.docValueCount(); ++j) {
                        assertTrue(valueSet.contains(sortedNumeric.nextValue()));
                    }
                    assertEquals(i, sortedSet.nextDoc());

                    assertEquals(valueSet.size(), sortedSet.docValueCount());
                    for (int j = 0; j < sortedSet.docValueCount(); ++j) {
                        long ord = sortedSet.nextOrd();
                        assertTrue(valueSet.contains(Long.parseLong(sortedSet.lookupOrd(ord).utf8ToString())));
                    }
                }
            }
        }

        indexReader.close();
        dir.close();
    }

    // TODO: try to refactor this and some termsenum tests into the base class.
    // to do this we need to fix the test class to get a DVF not a Codec so we can setup
    // the postings format correctly.
    private void doTestTermsEnumRandom(int numDocs, Supplier<String> valuesProducer)
        throws Exception {
        Directory dir = newFSDirectory(createTempDir());
        IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
        conf.setMergeScheduler(new SerialMergeScheduler());
        // set to duel against a codec which has ordinals:
        final PostingsFormat pf = TestUtil.getPostingsFormatWithOrds(random());
        final DocValuesFormat dv =
            ((PerFieldDocValuesFormat) getCodec().docValuesFormat())
                .getDocValuesFormatForField("random_field_name");
        conf.setCodec(
            new AssertingCodec() {
                @Override
                public PostingsFormat getPostingsFormatForField(String field) {
                    return pf;
                }

                @Override
                public DocValuesFormat getDocValuesFormatForField(String field) {
                    return dv;
                }
            });
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);

        // index some docs
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            Field idField = new StringField("id", Integer.toString(i), Field.Store.NO);
            doc.add(idField);
            int numValues = random().nextInt(17);
            // create a random list of strings
            List<String> values = new ArrayList<>();
            for (int v = 0; v < numValues; v++) {
                values.add(valuesProducer.get());
            }

            // add in any order to the indexed field
            ArrayList<String> unordered = new ArrayList<>(values);
            Collections.shuffle(unordered, random());
            for (String v : values) {
                doc.add(newStringField("indexed", v, Field.Store.NO));
            }

            // add in any order to the dv field
            ArrayList<String> unordered2 = new ArrayList<>(values);
            Collections.shuffle(unordered2, random());
            for (String v : unordered2) {
                doc.add(new SortedSetDocValuesField("dv", new BytesRef(v)));
            }

            writer.addDocument(doc);
            if (random().nextInt(31) == 0) {
                writer.commit();
            }
        }

        // delete some docs
        int numDeletions = random().nextInt(numDocs / 10);
        for (int i = 0; i < numDeletions; i++) {
            int id = random().nextInt(numDocs);
            writer.deleteDocuments(new Term("id", Integer.toString(id)));
        }

        // compare per-segment
        DirectoryReader ir = writer.getReader();
        for (LeafReaderContext context : ir.leaves()) {
            LeafReader r = context.reader();
            Terms terms = r.terms("indexed");
            if (terms != null) {
                SortedSetDocValues ssdv = r.getSortedSetDocValues("dv");
                assertEquals(terms.size(), ssdv.getValueCount());
                TermsEnum expected = terms.iterator();
                TermsEnum actual = r.getSortedSetDocValues("dv").termsEnum();
                assertEquals(terms.size(), expected, actual);

                doTestSortedSetEnumAdvanceIndependently(ssdv);
            }
        }
        ir.close();

        writer.forceMerge(1);

        // now compare again after the merge
        ir = writer.getReader();
        LeafReader ar = getOnlyLeafReader(ir);
        Terms terms = ar.terms("indexed");
        if (terms != null) {
            assertEquals(terms.size(), ar.getSortedSetDocValues("dv").getValueCount());
            TermsEnum expected = terms.iterator();
            TermsEnum actual = ar.getSortedSetDocValues("dv").termsEnum();
            assertEquals(terms.size(), expected, actual);
        }
        ir.close();

        writer.close();
        dir.close();
    }

    private void assertEquals(long numOrds, TermsEnum expected, TermsEnum actual) throws Exception {
        BytesRef ref;

        // sequential next() through all terms
        while ((ref = expected.next()) != null) {
            assertEquals(ref, actual.next());
            assertEquals(expected.ord(), actual.ord());
            assertEquals(expected.term(), actual.term());
        }
        assertNull(actual.next());

        // sequential seekExact(ord) through all terms
        for (long i = 0; i < numOrds; i++) {
            expected.seekExact(i);
            actual.seekExact(i);
            assertEquals(expected.ord(), actual.ord());
            assertEquals(expected.term(), actual.term());
        }

        // sequential seekExact(BytesRef) through all terms
        for (long i = 0; i < numOrds; i++) {
            expected.seekExact(i);
            assertTrue(actual.seekExact(expected.term()));
            assertEquals(expected.ord(), actual.ord());
            assertEquals(expected.term(), actual.term());
        }

        // sequential seekCeil(BytesRef) through all terms
        for (long i = 0; i < numOrds; i++) {
            expected.seekExact(i);
            assertEquals(TermsEnum.SeekStatus.FOUND, actual.seekCeil(expected.term()));
            assertEquals(expected.ord(), actual.ord());
            assertEquals(expected.term(), actual.term());
        }

        // random seekExact(ord)
        for (long i = 0; i < numOrds; i++) {
            long randomOrd = TestUtil.nextLong(random(), 0, numOrds - 1);
            expected.seekExact(randomOrd);
            actual.seekExact(randomOrd);
            assertEquals(expected.ord(), actual.ord());
            assertEquals(expected.term(), actual.term());
        }

        // random seekExact(BytesRef)
        for (long i = 0; i < numOrds; i++) {
            long randomOrd = TestUtil.nextLong(random(), 0, numOrds - 1);
            expected.seekExact(randomOrd);
            actual.seekExact(expected.term());
            assertEquals(expected.ord(), actual.ord());
            assertEquals(expected.term(), actual.term());
        }

        // random seekCeil(BytesRef)
        for (long i = 0; i < numOrds; i++) {
            BytesRef target = new BytesRef(TestUtil.randomUnicodeString(random()));
            TermsEnum.SeekStatus expectedStatus = expected.seekCeil(target);
            assertEquals(expectedStatus, actual.seekCeil(target));
            if (expectedStatus != TermsEnum.SeekStatus.END) {
                assertEquals(expected.ord(), actual.ord());
                assertEquals(expected.term(), actual.term());
            }
        }
    }

    @Nightly
    public void testSortedSetAroundBlockSize() throws IOException {
        final int frontier = 1 << CustomLucene90DocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
        for (int maxDoc = frontier - 1; maxDoc <= frontier + 1; ++maxDoc) {
            final Directory dir = newDirectory();
            IndexWriter w =
                new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()));
            ByteBuffersDataOutput out = new ByteBuffersDataOutput();
            Document doc = new Document();
            SortedSetDocValuesField field1 = new SortedSetDocValuesField("sset", new BytesRef());
            doc.add(field1);
            SortedSetDocValuesField field2 = new SortedSetDocValuesField("sset", new BytesRef());
            doc.add(field2);
            for (int i = 0; i < maxDoc; ++i) {
                BytesRef s1 = new BytesRef(TestUtil.randomSimpleString(random(), 2));
                BytesRef s2 = new BytesRef(TestUtil.randomSimpleString(random(), 2));
                field1.setBytesValue(s1);
                field2.setBytesValue(s2);
                w.addDocument(doc);
                Set<BytesRef> set = new TreeSet<>(Arrays.asList(s1, s2));
                out.writeVInt(set.size());
                for (BytesRef ref : set) {
                    out.writeVInt(ref.length);
                    out.writeBytes(ref.bytes, ref.offset, ref.length);
                }
            }

            w.forceMerge(1);
            DirectoryReader r = DirectoryReader.open(w);
            w.close();
            LeafReader sr = getOnlyLeafReader(r);
            assertEquals(maxDoc, sr.maxDoc());
            SortedSetDocValues values = sr.getSortedSetDocValues("sset");
            assertNotNull(values);
            ByteBuffersDataInput in = out.toDataInput();
            BytesRefBuilder b = new BytesRefBuilder();
            for (int i = 0; i < maxDoc; ++i) {
                assertEquals(i, values.nextDoc());
                final int numValues = in.readVInt();
                assertEquals(numValues, values.docValueCount());

                for (int j = 0; j < numValues; ++j) {
                    b.setLength(in.readVInt());
                    b.grow(b.length());
                    in.readBytes(b.bytes(), 0, b.length());
                    assertEquals(b.get(), values.lookupOrd(values.nextOrd()));
                }
            }
            r.close();
            dir.close();
        }
    }

    @Nightly
    public void testSortedNumericAroundBlockSize() throws IOException {
        final int frontier = 1 << CustomLucene90DocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
        for (int maxDoc = frontier - 1; maxDoc <= frontier + 1; ++maxDoc) {
            final Directory dir = newDirectory();
            IndexWriter w =
                new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()));
            ByteBuffersDataOutput buffer = new ByteBuffersDataOutput();

            Document doc = new Document();
            SortedNumericDocValuesField field1 = new SortedNumericDocValuesField("snum", 0L);
            doc.add(field1);
            SortedNumericDocValuesField field2 = new SortedNumericDocValuesField("snum", 0L);
            doc.add(field2);
            for (int i = 0; i < maxDoc; ++i) {
                long s1 = random().nextInt(100);
                long s2 = random().nextInt(100);
                field1.setLongValue(s1);
                field2.setLongValue(s2);
                w.addDocument(doc);
                buffer.writeVLong(Math.min(s1, s2));
                buffer.writeVLong(Math.max(s1, s2));
            }

            w.forceMerge(1);
            DirectoryReader r = DirectoryReader.open(w);
            w.close();
            LeafReader sr = getOnlyLeafReader(r);
            assertEquals(maxDoc, sr.maxDoc());
            SortedNumericDocValues values = sr.getSortedNumericDocValues("snum");
            assertNotNull(values);
            ByteBuffersDataInput dataInput = buffer.toDataInput();
            for (int i = 0; i < maxDoc; ++i) {
                assertEquals(i, values.nextDoc());
                assertEquals(2, values.docValueCount());
                assertEquals(dataInput.readVLong(), values.nextValue());
                assertEquals(dataInput.readVLong(), values.nextValue());
            }
            r.close();
            dir.close();
        }
    }

    @Nightly
    public void testSortedNumericBlocksOfVariousBitsPerValue() throws Exception {
        doTestSortedNumericBlocksOfVariousBitsPerValue(() -> TestUtil.nextInt(random(), 1, 3));
    }

    @Nightly
    public void testSparseSortedNumericBlocksOfVariousBitsPerValue() throws Exception {
        doTestSortedNumericBlocksOfVariousBitsPerValue(() -> TestUtil.nextInt(random(), 0, 2));
    }

    @Nightly
    public void testNumericBlocksOfVariousBitsPerValue() throws Exception {
        doTestSparseNumericBlocksOfVariousBitsPerValue(1);
    }

    @Nightly
    public void testSparseNumericBlocksOfVariousBitsPerValue() throws Exception {
        doTestSparseNumericBlocksOfVariousBitsPerValue(random().nextDouble());
    }

    // The LUCENE-8585 jump-tables enables O(1) skipping of IndexedDISI blocks, DENSE block lookup
    // and numeric multi blocks. This test focuses on testing these jumps.
    @Nightly
    public void testNumericFieldJumpTables() throws Exception {
        // IndexedDISI block skipping only activated if target >= current+2, so we need at least 5
        // blocks to
        // trigger consecutive block skips
        final int maxDoc = atLeast(5 * 65536);

        Directory dir = newDirectory();
        IndexWriter iw = createFastIndexWriter(dir, maxDoc);

        Field idField = newStringField("id", "", Field.Store.NO);
        Field storedField = newStringField("stored", "", Field.Store.YES);
        Field dvField = new NumericDocValuesField("dv", 0);

        for (int i = 0; i < maxDoc; i++) {
            Document doc = new Document();
            idField.setStringValue(Integer.toBinaryString(i));
            doc.add(idField);
            if (random().nextInt(100) > 10) { // Skip 10% to make DENSE blocks
                int value = random().nextInt(100000);
                storedField.setStringValue(Integer.toString(value));
                doc.add(storedField);
                dvField.setLongValue(value);
                doc.add(dvField);
            }
            iw.addDocument(doc);
        }
        iw.flush();
        iw.forceMerge(1, true); // Single segment to force large enough structures
        iw.commit();
        iw.close();

        assertDVIterate(dir);
        assertDVAdvance(
            dir, rarely() ? 1 : 7); // 1 is heavy (~20 s), so we do it rarely. 7 is a lot faster (8 s)

        dir.close();
    }

    private IndexWriter createFastIndexWriter(Directory dir, int maxBufferedDocs) throws IOException {
        IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
        conf.setMaxBufferedDocs(maxBufferedDocs);
        conf.setRAMBufferSizeMB(-1);
        conf.setMergePolicy(newLogMergePolicy(random().nextBoolean()));
        return new IndexWriter(dir, conf);
    }

    private static LongSupplier blocksOfVariousBPV() {
        // this helps exercise GCD compression:
        final long mul = TestUtil.nextInt(random(), 1, 100);
        final long min = random().nextInt();
        return new LongSupplier() {
            int i = CustomLucene90DocValuesFormat.NUMERIC_BLOCK_SIZE;
            int maxDelta;

            @Override
            public long getAsLong() {
                if (i == CustomLucene90DocValuesFormat.NUMERIC_BLOCK_SIZE) {
                    // change the range of the random generated values on block boundaries, so we exercise
                    // different bits-per-value for each block, and encourage block compression
                    maxDelta = 1 << random().nextInt(5);
                    i = 0;
                }
                i++;
                return min + mul * random().nextInt(maxDelta);
            }
        };
    }

    private void doTestSortedNumericBlocksOfVariousBitsPerValue(LongSupplier counts)
        throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
        conf.setMaxBufferedDocs(atLeast(CustomLucene90DocValuesFormat.NUMERIC_BLOCK_SIZE));
        conf.setRAMBufferSizeMB(-1);
        conf.setMergePolicy(newLogMergePolicy(random().nextBoolean()));
        IndexWriter writer = new IndexWriter(dir, conf);

        final int numDocs = atLeast(CustomLucene90DocValuesFormat.NUMERIC_BLOCK_SIZE * 3);
        final LongSupplier values = blocksOfVariousBPV();
        List<long[]> writeDocValues = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();

            int valueCount = (int) counts.getAsLong();
            long[] valueArray = new long[valueCount];
            for (int j = 0; j < valueCount; j++) {
                long value = values.getAsLong();
                valueArray[j] = value;
                doc.add(new SortedNumericDocValuesField("dv", value));
            }
            Arrays.sort(valueArray);
            writeDocValues.add(valueArray);
            for (int j = 0; j < valueCount; j++) {
                doc.add(new StoredField("stored", Long.toString(valueArray[j])));
            }
            writer.addDocument(doc);
            if (random().nextInt(31) == 0) {
                writer.commit();
            }
        }
        writer.forceMerge(1);

        writer.close();

        // compare
        DirectoryReader ir = DirectoryReader.open(dir);
        TestUtil.checkReader(ir);
        for (LeafReaderContext context : ir.leaves()) {
            LeafReader r = context.reader();
            SortedNumericDocValues docValues = DocValues.getSortedNumeric(r, "dv");
            StoredFields storedFields = r.storedFields();
            for (int i = 0; i < r.maxDoc(); i++) {
                if (i > docValues.docID()) {
                    docValues.nextDoc();
                }
                String[] expectedStored = storedFields.document(i).getValues("stored");
                if (i < docValues.docID()) {
                    assertEquals(0, expectedStored.length);
                } else {
                    long[] readValueArray = new long[docValues.docValueCount()];
                    String[] actualDocValue = new String[docValues.docValueCount()];
                    for (int j = 0; j < docValues.docValueCount(); ++j) {
                        long actualDV = docValues.nextValue();
                        readValueArray[j] = actualDV;
                        actualDocValue[j] = Long.toString(readValueArray[j]);
                    }
                    long[] writeValueArray = writeDocValues.get(i);
                    // compare write values and read values
                    assertArrayEquals(readValueArray, writeValueArray);

                    // compare dv and stored values
                    assertArrayEquals(expectedStored, actualDocValue);
                }
            }
        }
        ir.close();
        dir.close();
    }

    private void doTestSparseNumericBlocksOfVariousBitsPerValue(double density) throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
        conf.setMaxBufferedDocs(atLeast(CustomLucene90DocValuesFormat.NUMERIC_BLOCK_SIZE));
        conf.setRAMBufferSizeMB(-1);
        conf.setMergePolicy(newLogMergePolicy(random().nextBoolean()));
        IndexWriter writer = new IndexWriter(dir, conf);
        Document doc = new Document();
        Field storedField = newStringField("stored", "", Field.Store.YES);
        Field dvField = new NumericDocValuesField("dv", 0);
        doc.add(storedField);
        doc.add(dvField);

        final int numDocs = atLeast(CustomLucene90DocValuesFormat.NUMERIC_BLOCK_SIZE * 3);
        final LongSupplier longs = blocksOfVariousBPV();
        for (int i = 0; i < numDocs; i++) {
            if (random().nextDouble() > density) {
                writer.addDocument(new Document());
                continue;
            }
            long value = longs.getAsLong();
            storedField.setStringValue(Long.toString(value));
            dvField.setLongValue(value);
            writer.addDocument(doc);
        }

        writer.forceMerge(1);

        writer.close();

        // compare
        assertDVIterate(dir);
        assertDVAdvance(
            dir, 1); // Tests all jump-lengths from 1 to maxDoc (quite slow ~= 1 minute for 200K docs)

        dir.close();
    }

    // Tests that advanceExact does not change the outcome
    private void assertDVAdvance(Directory dir, int jumpStep) throws IOException {
        DirectoryReader ir = DirectoryReader.open(dir);
        TestUtil.checkReader(ir);
        for (LeafReaderContext context : ir.leaves()) {
            LeafReader r = context.reader();
            StoredFields storedFields = r.storedFields();

            for (int jump = jumpStep; jump < r.maxDoc(); jump += jumpStep) {
                // Create a new instance each time to ensure jumps from the beginning
                NumericDocValues docValues = DocValues.getNumeric(r, "dv");
                for (int docID = 0; docID < r.maxDoc(); docID += jump) {
                    String base =
                        "document #"
                            + docID
                            + "/"
                            + r.maxDoc()
                            + ", jumping "
                            + jump
                            + " from #"
                            + (docID - jump);
                    String storedValue = storedFields.document(docID).get("stored");
                    if (storedValue == null) {
                        assertFalse("There should be no DocValue for " + base, docValues.advanceExact(docID));
                    } else {
                        assertTrue("There should be a DocValue for " + base, docValues.advanceExact(docID));
                        assertEquals(
                            "The doc value should be correct for " + base,
                            Long.parseLong(storedValue),
                            docValues.longValue());
                    }
                }
            }
        }
        ir.close();
    }

    public void testReseekAfterSkipDecompression() throws IOException {
        final int CARDINALITY = (CustomLucene90DocValuesFormat.TERMS_DICT_BLOCK_LZ4_SIZE << 1) + 11;
        Set<String> valueSet = CollectionUtil.newHashSet(CARDINALITY);
        for (int i = 0; i < CARDINALITY; i++) {
            valueSet.add(TestUtil.randomSimpleString(random(), 64));
        }
        List<String> values = new ArrayList<>(valueSet);
        Collections.sort(values);
        // Create one non-existent value just between block-1 and block-2.
        String nonexistentValue =
            values.get(CustomLucene90DocValuesFormat.TERMS_DICT_BLOCK_LZ4_SIZE - 1)
                + TestUtil.randomSimpleString(random(), 64, 128);
        int docValues = values.size();

        try (Directory directory = newDirectory()) {
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            config.setCodec(getCodec());
            config.setUseCompoundFile(false);
            IndexWriter writer = new IndexWriter(directory, config);
            for (int i = 0; i < 280; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", "Doc" + i, Field.Store.NO));
                doc.add(new SortedDocValuesField("sdv", new BytesRef(values.get(i % docValues))));
                writer.addDocument(doc);
            }
            writer.commit();
            writer.forceMerge(1);
            DirectoryReader dReader = DirectoryReader.open(writer);
            writer.close();

            LeafReader reader = getOnlyLeafReader(dReader);
            // Check values count.
            SortedDocValues ssdvMulti = reader.getSortedDocValues("sdv");
            assertEquals(docValues, ssdvMulti.getValueCount());

            // Seek to first block.
            int ord1 = ssdvMulti.lookupTerm(new BytesRef(values.get(0)));
            assertTrue(ord1 >= 0);
            int ord2 = ssdvMulti.lookupTerm(new BytesRef(values.get(1)));
            assertTrue(ord2 >= ord1);
            // Ensure re-seek logic is correct after skip-decompression.
            int nonexistentOrd2 = ssdvMulti.lookupTerm(new BytesRef(nonexistentValue));
            assertTrue(nonexistentOrd2 < 0);
            dReader.close();
        }
    }

    public void testLargeTermsCompression() throws IOException {
        final int CARDINALITY = 64;
        Set<String> valuesSet = new HashSet<>();
        for (int i = 0; i < CARDINALITY; ++i) {
            final int length = TestUtil.nextInt(random(), 512, 1024);
            valuesSet.add(TestUtil.randomSimpleString(random(), length));
        }
        int valuesCount = valuesSet.size();
        List<String> values = new ArrayList<>(valuesSet);

        try (Directory directory = newDirectory()) {
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            config.setCodec(getCodec());
            config.setUseCompoundFile(false);
            IndexWriter writer = new IndexWriter(directory, config);
            for (int i = 0; i < 256; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", "Doc" + i, Field.Store.NO));
                doc.add(new SortedDocValuesField("sdv", new BytesRef(values.get(i % valuesCount))));
                writer.addDocument(doc);
            }
            writer.commit();
            writer.forceMerge(1);
            DirectoryReader ireader = DirectoryReader.open(writer);
            writer.close();

            LeafReader reader = getOnlyLeafReader(ireader);
            // Check values count.
            SortedDocValues ssdvMulti = reader.getSortedDocValues("sdv");
            assertEquals(valuesCount, ssdvMulti.getValueCount());
            ireader.close();
        }
    }

    public void testSortedTermsDictLookupOrd() throws IOException {
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
        Document doc = new Document();
        SortedDocValuesField field = new SortedDocValuesField("foo", new BytesRef());
        doc.add(field);
        final int numDocs = atLeast(CustomLucene90DocValuesFormat.TERMS_DICT_BLOCK_LZ4_SIZE + 1);
        for (int i = 0; i < numDocs; ++i) {
            field.setBytesValue(new BytesRef("" + i));
            writer.addDocument(doc);
        }
        writer.forceMerge(1);
        IndexReader reader = DirectoryReader.open(writer);
        LeafReader leafReader = getOnlyLeafReader(reader);
        doTestTermsDictLookupOrd(leafReader.getSortedDocValues("foo").termsEnum());
        reader.close();
        writer.close();
        dir.close();
    }

    public void testSortedSetTermsDictLookupOrd() throws IOException {
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
        Document doc = new Document();
        SortedSetDocValuesField field = new SortedSetDocValuesField("foo", new BytesRef());
        doc.add(field);
        final int numDocs = atLeast(2 * CustomLucene90DocValuesFormat.TERMS_DICT_BLOCK_LZ4_SIZE + 1);
        for (int i = 0; i < numDocs; ++i) {
            field.setBytesValue(new BytesRef("" + i));
            writer.addDocument(doc);
        }
        writer.forceMerge(1);
        IndexReader reader = DirectoryReader.open(writer);
        LeafReader leafReader = getOnlyLeafReader(reader);
        doTestTermsDictLookupOrd(leafReader.getSortedSetDocValues("foo").termsEnum());
        reader.close();
        writer.close();
        dir.close();
    }

    private void doTestTermsDictLookupOrd(TermsEnum te) throws IOException {
        List<BytesRef> terms = new ArrayList<>();
        for (BytesRef term = te.next(); term != null; term = te.next()) {
            terms.add(BytesRef.deepCopyOf(term));
        }

        // iterate in order
        for (int i = 0; i < terms.size(); ++i) {
            te.seekExact(i);
            assertEquals(terms.get(i), te.term());
        }

        // iterate in reverse order
        for (int i = terms.size() - 1; i >= 0; --i) {
            te.seekExact(i);
            assertEquals(terms.get(i), te.term());
        }

        // iterate in forward order with random gaps
        for (int i = random().nextInt(5); i < terms.size(); i += random().nextInt(5)) {
            te.seekExact(i);
            assertEquals(terms.get(i), te.term());
        }
    }

    // Exercise the logic that leverages the first term of a block as a dictionary for suffixes of
    // other terms
    public void testTermsEnumDictionary() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig();
        RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
        Document doc = new Document();
        SortedDocValuesField field = new SortedDocValuesField("field", new BytesRef("abc0defghijkl"));
        doc.add(field);
        iwriter.addDocument(doc);
        field.setBytesValue(new BytesRef("abc1defghijkl"));
        iwriter.addDocument(doc);
        field.setBytesValue(new BytesRef("abc2defghijkl"));
        iwriter.addDocument(doc);
        iwriter.forceMerge(1);
        iwriter.close();

        IndexReader reader = DirectoryReader.open(directory);
        LeafReader leafReader = getOnlyLeafReader(reader);
        SortedDocValues values = leafReader.getSortedDocValues("field");
        TermsEnum termsEnum = values.termsEnum();
        assertEquals(new BytesRef("abc0defghijkl"), termsEnum.next());
        assertEquals(new BytesRef("abc1defghijkl"), termsEnum.next());
        assertEquals(new BytesRef("abc2defghijkl"), termsEnum.next());
        assertNull(termsEnum.next());

        reader.close();
        directory.close();
    }

    // Testing termsEnum seekCeil edge case, where inconsistent internal state led to
    // IndexOutOfBoundsException
    // see https://github.com/apache/lucene/pull/12555 for details
    public void testTermsEnumConsistency() throws IOException {
        int numTerms =
            CustomLucene90DocValuesFormat.TERMS_DICT_BLOCK_LZ4_SIZE
                + 10; // need more than one block of unique terms.
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig();
        RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
        Document doc = new Document();

        // for simplicity, we will generate sorted list of terms which are a) unique b) all greater than
        // the term that we want to use for the test
        char termA = 'A';
        Function<Integer, String> stringSupplier =
            (Integer n) -> {
                assert n < 25 * 25;
                char[] chars = new char[] {(char) (termA + 1 + n / 25), (char) (termA + 1 + n % 25)};
                return new String(chars);
            };
        SortedDocValuesField field =
            new SortedDocValuesField("field", new BytesRef(stringSupplier.apply(0)));
        doc.add(field);
        iwriter.addDocument(doc);
        for (int i = 1; i < numTerms; i++) {
            field.setBytesValue(new BytesRef(stringSupplier.apply(i)));
            iwriter.addDocument(doc);
        }
        // merging to one segment to make sure we have more than one block (TERMS_DICT_BLOCK_LZ4_SIZE)
        // in a segment, to trigger next block decompression.
        iwriter.forceMerge(1);
        iwriter.close();

        IndexReader reader = DirectoryReader.open(directory);
        LeafReader leafReader = getOnlyLeafReader(reader);
        SortedDocValues values = leafReader.getSortedDocValues("field");
        TermsEnum termsEnum = values.termsEnum();

        // Position terms enum at 0
        termsEnum.seekExact(0L);
        assertEquals(0, termsEnum.ord());
        // seekCeil to a term which doesn't exist in the index
        assertEquals(TermsEnum.SeekStatus.NOT_FOUND, termsEnum.seekCeil(new BytesRef("A")));
        // ... and before any other term in the index
        assertEquals(0, termsEnum.ord());

        assertEquals(new BytesRef(stringSupplier.apply(0)), termsEnum.term());
        // read more than one block of terms to trigger next block decompression
        for (int i = 1; i < numTerms; i++) {
            assertEquals(new BytesRef(stringSupplier.apply(i)), termsEnum.next());
        }
        assertNull(termsEnum.next());
        reader.close();
        directory.close();
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.crate.lucene;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.VirtualMethod;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * A {@link FilterLeafReader} that can be used to apply
 * additional checks for tests.
 */
public class PatchedAssertingLeafReader extends FilterLeafReader {

    private static void assertThread(String object, Thread creationThread) {
        // CRATE PATCH: pause/resume logic in crate can cause thread switches
        /*
         * if (creationThread != Thread.currentThread()) { throw new
         * AssertionError(object + " are only supposed to be consumed in " +
         * "the thread in which they have been acquired. But was acquired in " +
         * creationThread + " and consumed in " + Thread.currentThread() + "."); }
         */
    }

    public PatchedAssertingLeafReader(LeafReader in) {
        super(in);
        // check some basic reader sanity
        assert in.maxDoc() >= 0;
        assert in.numDocs() <= in.maxDoc();
        assert in.numDeletedDocs() + in.numDocs() == in.maxDoc();
        assert !in.hasDeletions() || in.numDeletedDocs() > 0 && in.numDocs() < in.maxDoc();

        CacheHelper coreCacheHelper = in.getCoreCacheHelper();
        if (coreCacheHelper != null) {
            coreCacheHelper.addClosedListener(cacheKey -> {
                final Object expectedKey = coreCacheHelper.getKey();
                assert expectedKey == cacheKey
                    : "Core closed listener called on a different key " + expectedKey + " <> " + cacheKey;
            });
        }

        CacheHelper readerCacheHelper = in.getReaderCacheHelper();
        if (readerCacheHelper != null) {
            readerCacheHelper.addClosedListener(cacheKey -> {
                final Object expectedKey = readerCacheHelper.getKey();
                assert expectedKey == cacheKey
                    : "Core closed listener called on a different key " + expectedKey + " <> " + cacheKey;
            });
        }
    }

    @Override
    public Terms terms(String field) throws IOException {
        Terms terms = super.terms(field);
        return terms == null ? null : new AssertingTerms(terms);
    }

    /**
     * Wraps a Terms but with additional asserts
     */
    public static class AssertingTerms extends FilterTerms {
        public AssertingTerms(Terms in) {
            super(in);
        }

        @Override
        public TermsEnum intersect(CompiledAutomaton automaton, BytesRef bytes) throws IOException {
            TermsEnum termsEnum = in.intersect(automaton, bytes);
            assert termsEnum != null;
            assert bytes == null || bytes.isValid();
            return new AssertingTermsEnum(termsEnum, hasFreqs());
        }

        @Override
        public BytesRef getMin() throws IOException {
            BytesRef v = in.getMin();
            assert v == null || v.isValid();
            return v;
        }

        @Override
        public BytesRef getMax() throws IOException {
            BytesRef v = in.getMax();
            assert v == null || v.isValid();
            return v;
        }

        @Override
        public int getDocCount() throws IOException {
            final int docCount = in.getDocCount();
            assert docCount > 0;
            return docCount;
        }

        @Override
        public long getSumDocFreq() throws IOException {
            final long sumDf = in.getSumDocFreq();
            assert sumDf >= getDocCount();
            return sumDf;
        }

        @Override
        public long getSumTotalTermFreq() throws IOException {
            final long sumTtf = in.getSumTotalTermFreq();
            if (hasFreqs() == false) {
                assert sumTtf == in.getSumDocFreq();
            }
            assert sumTtf >= getSumDocFreq();
            return sumTtf;
        }

        @Override
        public TermsEnum iterator() throws IOException {
            TermsEnum termsEnum = super.iterator();
            assert termsEnum != null;
            return new AssertingTermsEnum(termsEnum, hasFreqs());
        }

        @Override
        public String toString() {
            return "AssertingTerms(" + in + ")";
        }
    }

    static final VirtualMethod<TermsEnum> SEEK_EXACT = new VirtualMethod<>(TermsEnum.class, "seekExact",
            BytesRef.class);

    static class AssertingTermsEnum extends FilterTermsEnum {
        private final Thread creationThread = Thread.currentThread();

        private enum State {
            INITIAL, POSITIONED, UNPOSITIONED
        }

        private State state = State.INITIAL;
        private final boolean delegateOverridesSeekExact;
        private final boolean hasFreqs;

        public AssertingTermsEnum(TermsEnum in, boolean hasFreqs) {
            super(in);
            delegateOverridesSeekExact = SEEK_EXACT.isOverriddenAsOf(in.getClass());
            this.hasFreqs = hasFreqs;
        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
            assertThread("Terms enums", creationThread);
            assert state == State.POSITIONED : "docs(...) called on unpositioned TermsEnum";

            // reuse if the codec reused
            final PostingsEnum actualReuse;
            if (reuse instanceof AssertingPostingsEnum postingsEnum) {
                actualReuse = postingsEnum.unwrap();
            } else {
                actualReuse = null;
            }
            PostingsEnum docs = super.postings(actualReuse, flags);
            assert docs != null;
            if (docs == actualReuse) {
                // codec reused, reset asserting state
                ((AssertingPostingsEnum) reuse).reset();
                return reuse;
            } else {
                return new AssertingPostingsEnum(docs);
            }
        }

        @Override
        public ImpactsEnum impacts(int flags) throws IOException {
            assertThread("Terms enums", creationThread);
            assert state == State.POSITIONED : "docs(...) called on unpositioned TermsEnum";
            assert (flags & PostingsEnum.FREQS) != 0 : "Freqs should be requested on impacts";

            return new AssertingImpactsEnum(super.impacts(flags));
        }

        // TODO: we should separately track if we are 'at the end' ?
        // someone should not call next() after it returns null!!!!
        @Override
        public BytesRef next() throws IOException {
            assertThread("Terms enums", creationThread);
            assert state == State.INITIAL || state == State.POSITIONED : "next() called on unpositioned TermsEnum";
            BytesRef result = super.next();
            if (result == null) {
                state = State.UNPOSITIONED;
            } else {
                assert result.isValid();
                state = State.POSITIONED;
            }
            return result;
        }

        @Override
        public long ord() throws IOException {
            assertThread("Terms enums", creationThread);
            assert state == State.POSITIONED : "ord() called on unpositioned TermsEnum";
            return super.ord();
        }

        @Override
        public int docFreq() throws IOException {
            assertThread("Terms enums", creationThread);
            assert state == State.POSITIONED : "docFreq() called on unpositioned TermsEnum";
            final int df = super.docFreq();
            assert df > 0;
            return df;
        }

        @Override
        public long totalTermFreq() throws IOException {
            assertThread("Terms enums", creationThread);
            assert state == State.POSITIONED : "totalTermFreq() called on unpositioned TermsEnum";
            final long ttf = super.totalTermFreq();
            if (hasFreqs) {
                assert ttf >= docFreq();
            } else {
                assert ttf == docFreq();
            }
            return ttf;
        }

        @Override
        public BytesRef term() throws IOException {
            assertThread("Terms enums", creationThread);
            assert state == State.POSITIONED : "term() called on unpositioned TermsEnum";
            BytesRef ret = super.term();
            assert ret == null || ret.isValid();
            return ret;
        }

        @Override
        public void seekExact(long ord) throws IOException {
            assertThread("Terms enums", creationThread);
            super.seekExact(ord);
            state = State.POSITIONED;
        }

        @Override
        public SeekStatus seekCeil(BytesRef term) throws IOException {
            assertThread("Terms enums", creationThread);
            assert term.isValid();
            SeekStatus result = super.seekCeil(term);
            if (result == SeekStatus.END) {
                state = State.UNPOSITIONED;
            } else {
                state = State.POSITIONED;
            }
            return result;
        }

        @Override
        public boolean seekExact(BytesRef text) throws IOException {
            assertThread("Terms enums", creationThread);
            assert text.isValid();
            boolean result;
            if (delegateOverridesSeekExact) {
                result = in.seekExact(text);
            } else {
                result = super.seekExact(text);
            }
            if (result) {
                state = State.POSITIONED;
            } else {
                state = State.UNPOSITIONED;
            }
            return result;
        }

        @Override
        public TermState termState() throws IOException {
            assertThread("Terms enums", creationThread);
            assert state == State.POSITIONED : "termState() called on unpositioned TermsEnum";
            return in.termState();
        }

        @Override
        public void seekExact(BytesRef term, TermState state) throws IOException {
            assertThread("Terms enums", creationThread);
            assert term.isValid();
            in.seekExact(term, state);
            this.state = State.POSITIONED;
        }

        @Override
        public String toString() {
            return "AssertingTermsEnum(" + in + ")";
        }

        void reset() {
            state = State.INITIAL;
        }
    }

    enum DocsEnumState {
        START, ITERATING, FINISHED
    }

    /** Wraps a docsenum with additional checks */
    public static class AssertingPostingsEnum extends FilterPostingsEnum {
        private final Thread creationThread = Thread.currentThread();
        private DocsEnumState state = DocsEnumState.START;
        int positionCount = 0;
        int positionMax = 0;
        private int doc;

        public AssertingPostingsEnum(PostingsEnum in) {
            super(in);
            this.doc = in.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            assertThread("Docs enums", creationThread);
            assert state != DocsEnumState.FINISHED : "nextDoc() called after NO_MORE_DOCS";
            int nextDoc = super.nextDoc();
            assert nextDoc > doc : "backwards nextDoc from " + doc + " to " + nextDoc + " " + in;
            if (nextDoc == DocIdSetIterator.NO_MORE_DOCS) {
                state = DocsEnumState.FINISHED;
                positionMax = 0;
            } else {
                state = DocsEnumState.ITERATING;
                positionMax = super.freq();
            }
            positionCount = 0;
            assert super.docID() == nextDoc;
            return doc = nextDoc;
        }

        @Override
        public int advance(int target) throws IOException {
            assertThread("Docs enums", creationThread);
            assert state != DocsEnumState.FINISHED : "advance() called after NO_MORE_DOCS";
            assert target > doc : "target must be > docID(), got " + target + " <= " + doc;
            int advanced = super.advance(target);
            assert advanced >= target : "backwards advance from: " + target + " to: " + advanced;
            if (advanced == DocIdSetIterator.NO_MORE_DOCS) {
                state = DocsEnumState.FINISHED;
                positionMax = 0;
            } else {
                state = DocsEnumState.ITERATING;
                positionMax = super.freq();
            }
            positionCount = 0;
            assert super.docID() == advanced;
            return doc = advanced;
        }

        @Override
        public int docID() {
            assertThread("Docs enums", creationThread);
            assert doc == super.docID()
                : " invalid docID() in " + in.getClass() + " " + super.docID() + " instead of " + doc;
            return doc;
        }

        @Override
        public int freq() throws IOException {
            assertThread("Docs enums", creationThread);
            assert state != DocsEnumState.START : "freq() called before nextDoc()/advance()";
            assert state != DocsEnumState.FINISHED : "freq() called after NO_MORE_DOCS";
            int freq = super.freq();
            assert freq > 0;
            return freq;
        }

        @Override
        public int nextPosition() throws IOException {
            assert state != DocsEnumState.START : "nextPosition() called before nextDoc()/advance()";
            assert state != DocsEnumState.FINISHED : "nextPosition() called after NO_MORE_DOCS";
            assert positionCount < positionMax : "nextPosition() called more than freq() times!";
            int position = super.nextPosition();
            assert position >= 0 || position == -1 : "invalid position: " + position;
            positionCount++;
            return position;
        }

        @Override
        public int startOffset() throws IOException {
            assert state != DocsEnumState.START : "startOffset() called before nextDoc()/advance()";
            assert state != DocsEnumState.FINISHED : "startOffset() called after NO_MORE_DOCS";
            assert positionCount > 0 : "startOffset() called before nextPosition()!";
            return super.startOffset();
        }

        @Override
        public int endOffset() throws IOException {
            assert state != DocsEnumState.START : "endOffset() called before nextDoc()/advance()";
            assert state != DocsEnumState.FINISHED : "endOffset() called after NO_MORE_DOCS";
            assert positionCount > 0 : "endOffset() called before nextPosition()!";
            return super.endOffset();
        }

        @Override
        public BytesRef getPayload() throws IOException {
            assert state != DocsEnumState.START : "getPayload() called before nextDoc()/advance()";
            assert state != DocsEnumState.FINISHED : "getPayload() called after NO_MORE_DOCS";
            assert positionCount > 0 : "getPayload() called before nextPosition()!";
            BytesRef payload = super.getPayload();
            assert payload == null || payload.length > 0 : "getPayload() returned payload with invalid length!";
            return payload;
        }

        void reset() {
            state = DocsEnumState.START;
            doc = in.docID();
            positionCount = positionMax = 0;
        }
    }

    /** Wraps a {@link ImpactsEnum} with additional checks */
    public static class AssertingImpactsEnum extends ImpactsEnum {

        private final AssertingPostingsEnum assertingPostings;
        private final ImpactsEnum in;
        private int lastShallowTarget = -1;

        AssertingImpactsEnum(ImpactsEnum impacts) {
            in = impacts;
            // inherit checks from AssertingPostingsEnum
            assertingPostings = new AssertingPostingsEnum(impacts);
        }

        @Override
        public void advanceShallow(int target) throws IOException {
            assert target >= lastShallowTarget
                : "called on decreasing targets: target = " + target + " < last target = " + lastShallowTarget;
            assert target >= docID() : "target = " + target + " < docID = " + docID();
            lastShallowTarget = target;
            in.advanceShallow(target);
        }

        @Override
        public Impacts getImpacts() throws IOException {
            assert docID() >= 0 || lastShallowTarget >= 0
                : "Cannot get impacts until the iterator is positioned or advanceShallow has been called";
            Impacts impacts = in.getImpacts();

            // Package private, can't use:
            // CheckIndex.checkImpacts(impacts, Math.max(docID(), lastShallowTarget));
            return new AssertingImpacts(impacts, this);
        }

        @Override
        public int freq() throws IOException {
            return assertingPostings.freq();
        }

        @Override
        public int nextPosition() throws IOException {
            return assertingPostings.nextPosition();
        }

        @Override
        public int startOffset() throws IOException {
            return assertingPostings.startOffset();
        }

        @Override
        public int endOffset() throws IOException {
            return assertingPostings.endOffset();
        }

        @Override
        public BytesRef getPayload() throws IOException {
            return assertingPostings.getPayload();
        }

        @Override
        public int docID() {
            return assertingPostings.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            assert docID() + 1 >= lastShallowTarget
                : "target = " + (docID() + 1) + " < last shallow target = " + lastShallowTarget;
            return assertingPostings.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            assert target >= lastShallowTarget : "target = " + target + " < last shallow target = " + lastShallowTarget;
            return assertingPostings.advance(target);
        }

        @Override
        public long cost() {
            return assertingPostings.cost();
        }
    }

    static class AssertingImpacts extends Impacts {

        private final Impacts in;
        private final AssertingImpactsEnum impactsEnum;
        private final int validFor;

        AssertingImpacts(Impacts in, AssertingImpactsEnum impactsEnum) {
            this.in = in;
            this.impactsEnum = impactsEnum;
            validFor = Math.max(impactsEnum.docID(), impactsEnum.lastShallowTarget);
        }

        @Override
        public int numLevels() {
            assert validFor == Math.max(impactsEnum.docID(), impactsEnum.lastShallowTarget)
                : "Cannot reuse impacts after advancing the iterator";
            return in.numLevels();
        }

        @Override
        public int getDocIdUpTo(int level) {
            assert validFor == Math.max(impactsEnum.docID(), impactsEnum.lastShallowTarget)
                : "Cannot reuse impacts after advancing the iterator";
            return in.getDocIdUpTo(level);
        }

        @Override
        public List<Impact> getImpacts(int level) {
            assert validFor == Math.max(impactsEnum.docID(), impactsEnum.lastShallowTarget)
                : "Cannot reuse impacts after advancing the iterator";
            return in.getImpacts(level);
        }

    }

    /** Wraps a NumericDocValues but with additional asserts */
    public static class AssertingNumericDocValues extends NumericDocValues {
        private final Thread creationThread = Thread.currentThread();
        private final NumericDocValues in;
        private final int maxDoc;
        private int lastDocID = -1;
        private boolean exists;

        public AssertingNumericDocValues(NumericDocValues in, int maxDoc) {
            this.in = in;
            this.maxDoc = maxDoc;
            // should start unpositioned:
            assert in.docID() == -1;
        }

        @Override
        public int docID() {
            assertThread("Numeric doc values", creationThread);
            return in.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            assertThread("Numeric doc values", creationThread);
            int docID = in.nextDoc();
            assert docID > lastDocID;
            assert docID == NO_MORE_DOCS || docID < maxDoc;
            assert docID == in.docID();
            lastDocID = docID;
            exists = docID != NO_MORE_DOCS;
            return docID;
        }

        @Override
        public int advance(int target) throws IOException {
            assertThread("Numeric doc values", creationThread);
            assert target >= 0;
            assert target > in.docID();
            int docID = in.advance(target);
            assert docID >= target;
            assert docID == NO_MORE_DOCS || docID < maxDoc;
            lastDocID = docID;
            exists = docID != NO_MORE_DOCS;
            return docID;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            assertThread("Numeric doc values", creationThread);
            assert target >= 0;
            assert target >= in.docID();
            assert target < maxDoc;
            exists = in.advanceExact(target);
            assert in.docID() == target;
            lastDocID = target;
            return exists;
        }

        @Override
        public long cost() {
            assertThread("Numeric doc values", creationThread);
            long cost = in.cost();
            assert cost >= 0;
            return cost;
        }

        @Override
        public long longValue() throws IOException {
            assertThread("Numeric doc values", creationThread);
            assert exists;
            return in.longValue();
        }

        @Override
        public String toString() {
            return "AssertingNumericDocValues(" + in + ")";
        }
    }

    /** Wraps a BinaryDocValues but with additional asserts */
    public static class AssertingBinaryDocValues extends BinaryDocValues {
        private final Thread creationThread = Thread.currentThread();
        private final BinaryDocValues in;
        private final int maxDoc;
        private int lastDocID = -1;
        private boolean exists;

        public AssertingBinaryDocValues(BinaryDocValues in, int maxDoc) {
            this.in = in;
            this.maxDoc = maxDoc;
            // should start unpositioned:
            assert in.docID() == -1;
        }

        @Override
        public int docID() {
            assertThread("Binary doc values", creationThread);
            return in.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            assertThread("Binary doc values", creationThread);
            int docID = in.nextDoc();
            assert docID > lastDocID;
            assert docID == NO_MORE_DOCS || docID < maxDoc;
            assert docID == in.docID();
            lastDocID = docID;
            exists = docID != NO_MORE_DOCS;
            return docID;
        }

        @Override
        public int advance(int target) throws IOException {
            assertThread("Binary doc values", creationThread);
            assert target >= 0;
            assert target > in.docID();
            int docID = in.advance(target);
            assert docID >= target;
            assert docID == NO_MORE_DOCS || docID < maxDoc;
            lastDocID = docID;
            exists = docID != NO_MORE_DOCS;
            return docID;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            assertThread("Numeric doc values", creationThread);
            assert target >= 0;
            assert target >= in.docID();
            assert target < maxDoc;
            exists = in.advanceExact(target);
            assert in.docID() == target;
            lastDocID = target;
            return exists;
        }

        @Override
        public long cost() {
            assertThread("Binary doc values", creationThread);
            long cost = in.cost();
            assert cost >= 0;
            return cost;
        }

        @Override
        public BytesRef binaryValue() throws IOException {
            assertThread("Binary doc values", creationThread);
            assert exists;
            return in.binaryValue();
        }

        @Override
        public String toString() {
            return "AssertingBinaryDocValues(" + in + ")";
        }
    }

    /** Wraps a SortedDocValues but with additional asserts */
    public static class AssertingSortedDocValues extends SortedDocValues {
        private final Thread creationThread = Thread.currentThread();
        private final SortedDocValues in;
        private final int maxDoc;
        private final int valueCount;
        private int lastDocID = -1;
        private boolean exists;

        public AssertingSortedDocValues(SortedDocValues in, int maxDoc) {
            this.in = in;
            this.maxDoc = maxDoc;
            this.valueCount = in.getValueCount();
            assert valueCount >= 0 && valueCount <= maxDoc;
        }

        @Override
        public int docID() {
            assertThread("Sorted doc values", creationThread);
            return in.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            assertThread("Sorted doc values", creationThread);
            int docID = in.nextDoc();
            assert docID > lastDocID;
            assert docID == NO_MORE_DOCS || docID < maxDoc;
            assert docID == in.docID();
            lastDocID = docID;
            exists = docID != NO_MORE_DOCS;
            return docID;
        }

        @Override
        public int advance(int target) throws IOException {
            assertThread("Sorted doc values", creationThread);
            assert target >= 0;
            assert target > in.docID();
            int docID = in.advance(target);
            assert docID >= target;
            assert docID == NO_MORE_DOCS || docID < maxDoc;
            lastDocID = docID;
            exists = docID != NO_MORE_DOCS;
            return docID;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            assertThread("Numeric doc values", creationThread);
            assert target >= 0;
            assert target >= in.docID();
            assert target < maxDoc;
            exists = in.advanceExact(target);
            assert in.docID() == target;
            lastDocID = target;
            return exists;
        }

        @Override
        public long cost() {
            assertThread("Sorted doc values", creationThread);
            long cost = in.cost();
            assert cost >= 0;
            return cost;
        }

        @Override
        public int ordValue() throws IOException {
            assertThread("Sorted doc values", creationThread);
            assert exists;
            int ord = in.ordValue();
            assert ord >= -1 && ord < valueCount;
            return ord;
        }

        @Override
        public BytesRef lookupOrd(int ord) throws IOException {
            assertThread("Sorted doc values", creationThread);
            assert ord >= 0 && ord < valueCount;
            final BytesRef result = in.lookupOrd(ord);
            assert result.isValid();
            return result;
        }

        @Override
        public int getValueCount() {
            assertThread("Sorted doc values", creationThread);
            int valueCount = in.getValueCount();
            assert valueCount == this.valueCount; // should not change
            return valueCount;
        }

        @Override
        public int lookupTerm(BytesRef key) throws IOException {
            assertThread("Sorted doc values", creationThread);
            assert key.isValid();
            int result = in.lookupTerm(key);
            assert result < valueCount;
            assert key.isValid();
            return result;
        }
    }

    /** Wraps a SortedNumericDocValues but with additional asserts */
    public static class AssertingSortedNumericDocValues extends SortedNumericDocValues {
        private final Thread creationThread = Thread.currentThread();
        private final SortedNumericDocValues in;
        private final int maxDoc;
        private int lastDocID = -1;
        private int valueUpto;
        private boolean exists;

        private AssertingSortedNumericDocValues(SortedNumericDocValues in, int maxDoc) {
            this.in = in;
            this.maxDoc = maxDoc;
        }

        public static SortedNumericDocValues create(SortedNumericDocValues in, int maxDoc) {
            NumericDocValues singleDocValues = DocValues.unwrapSingleton(in);
            if (singleDocValues == null) {
                return new AssertingSortedNumericDocValues(in, maxDoc);
            } else {
                NumericDocValues assertingDocValues = new AssertingNumericDocValues(singleDocValues, maxDoc);
                return DocValues.singleton(assertingDocValues);
            }
        }

        @Override
        public int docID() {
            return in.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            assertThread("Sorted numeric doc values", creationThread);
            int docID = in.nextDoc();
            assert docID > lastDocID;
            assert docID == NO_MORE_DOCS || docID < maxDoc;
            assert docID == in.docID();
            lastDocID = docID;
            valueUpto = 0;
            exists = docID != NO_MORE_DOCS;
            return docID;
        }

        @Override
        public int advance(int target) throws IOException {
            assertThread("Sorted numeric doc values", creationThread);
            assert target >= 0;
            assert target > in.docID();
            int docID = in.advance(target);
            assert docID == in.docID();
            assert docID >= target;
            assert docID == NO_MORE_DOCS || docID < maxDoc;
            lastDocID = docID;
            valueUpto = 0;
            exists = docID != NO_MORE_DOCS;
            return docID;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            assertThread("Numeric doc values", creationThread);
            assert target >= 0;
            assert target >= in.docID();
            assert target < maxDoc;
            exists = in.advanceExact(target);
            assert in.docID() == target;
            lastDocID = target;
            valueUpto = 0;
            return exists;
        }

        @Override
        public long cost() {
            assertThread("Sorted numeric doc values", creationThread);
            long cost = in.cost();
            assert cost >= 0;
            return cost;
        }

        @Override
        public long nextValue() throws IOException {
            assertThread("Sorted numeric doc values", creationThread);
            assert exists;
            assert valueUpto < in.docValueCount()
                : "valueUpto=" + valueUpto + " in.docValueCount()=" + in.docValueCount();
            valueUpto++;
            return in.nextValue();
        }

        @Override
        public int docValueCount() {
            assertThread("Sorted numeric doc values", creationThread);
            assert exists;
            assert in.docValueCount() > 0;
            return in.docValueCount();
        }
    }

    /** Wraps a SortedSetDocValues but with additional asserts */
    public static class AssertingSortedSetDocValues extends SortedSetDocValues {
        private final Thread creationThread = Thread.currentThread();
        private final SortedSetDocValues in;
        private final int maxDoc;
        private final long valueCount;
        private int lastDocID = -1;
        private long lastOrd = Integer.MAX_VALUE;
        private boolean exists;

        private AssertingSortedSetDocValues(SortedSetDocValues in, int maxDoc) {
            this.in = in;
            this.maxDoc = maxDoc;
            this.valueCount = in.getValueCount();
            assert valueCount >= 0;
        }

        public static SortedSetDocValues create(SortedSetDocValues in, int maxDoc) {
            SortedDocValues singleDocValues = DocValues.unwrapSingleton(in);
            if (singleDocValues == null) {
                return new AssertingSortedSetDocValues(in, maxDoc);
            } else {
                SortedDocValues assertingDocValues = new AssertingSortedDocValues(singleDocValues, maxDoc);
                return DocValues.singleton(assertingDocValues);
            }
        }

        @Override
        public int docID() {
            assertThread("Sorted set doc values", creationThread);
            return in.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            assertThread("Sorted set doc values", creationThread);
            int docID = in.nextDoc();
            assert docID > lastDocID;
            assert docID == NO_MORE_DOCS || docID < maxDoc;
            assert docID == in.docID();
            lastDocID = docID;
            lastOrd = -2;
            exists = docID != NO_MORE_DOCS;
            return docID;
        }

        @Override
        public int advance(int target) throws IOException {
            assertThread("Sorted set doc values", creationThread);
            assert target >= 0;
            assert target > in.docID();
            int docID = in.advance(target);
            assert docID == in.docID();
            assert docID >= target;
            assert docID == NO_MORE_DOCS || docID < maxDoc;
            lastDocID = docID;
            lastOrd = -2;
            exists = docID != NO_MORE_DOCS;
            return docID;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            assertThread("Numeric doc values", creationThread);
            assert target >= 0;
            assert target >= in.docID();
            assert target < maxDoc;
            exists = in.advanceExact(target);
            assert in.docID() == target;
            lastDocID = target;
            lastOrd = -2;
            return exists;
        }

        @Override
        public long cost() {
            assertThread("Sorted set doc values", creationThread);
            long cost = in.cost();
            assert cost >= 0;
            return cost;
        }

        @Override
        public long nextOrd() throws IOException {
            assertThread("Sorted set doc values", creationThread);
            assert exists;
            long ord = in.nextOrd();
            assert ord < valueCount;
            assert ord > lastOrd;
            lastOrd = ord;
            return ord;
        }

        @Override
        public int docValueCount() {
            return in.docValueCount();
        }

        @Override
        public BytesRef lookupOrd(long ord) throws IOException {
            assertThread("Sorted set doc values", creationThread);
            assert ord >= 0 && ord < valueCount;
            final BytesRef result = in.lookupOrd(ord);
            assert result.isValid();
            return result;
        }

        @Override
        public long getValueCount() {
            assertThread("Sorted set doc values", creationThread);
            long valueCount = in.getValueCount();
            assert valueCount == this.valueCount; // should not change
            return valueCount;
        }

        @Override
        public long lookupTerm(BytesRef key) throws IOException {
            assertThread("Sorted set doc values", creationThread);
            assert key.isValid();
            long result = in.lookupTerm(key);
            assert result < valueCount;
            assert key.isValid();
            return result;
        }
    }

    /** Wraps a SortedSetDocValues but with additional asserts */
    public static class AssertingPointValues extends PointValues {
        private final Thread creationThread = Thread.currentThread();
        private final PointValues in;

        /** Sole constructor. */
        public AssertingPointValues(PointValues in, int maxDoc) {
            this.in = in;
            assertStats(maxDoc);
        }

        public PointValues getWrapped() {
            return in;
        }

        private void assertStats(int maxDoc) {
            assert in.size() > 0;
            assert in.getDocCount() > 0;
            assert in.getDocCount() <= in.size();
            assert in.getDocCount() <= maxDoc;
        }

        @Override
        public PointTree getPointTree() throws IOException {
            return in.getPointTree();
        }

        @Override
        public byte[] getMinPackedValue() throws IOException {
            assertThread("Points", creationThread);
            return Objects.requireNonNull(in.getMinPackedValue());
        }

        @Override
        public byte[] getMaxPackedValue() throws IOException {
            assertThread("Points", creationThread);
            return Objects.requireNonNull(in.getMaxPackedValue());
        }

        @Override
        public int getNumDimensions() throws IOException {
            assertThread("Points", creationThread);
            return in.getNumDimensions();
        }

        @Override
        public int getNumIndexDimensions() throws IOException {
            assertThread("Points", creationThread);
            return in.getNumIndexDimensions();
        }

        @Override
        public int getBytesPerDimension() throws IOException {
            assertThread("Points", creationThread);
            return in.getBytesPerDimension();
        }

        @Override
        public long size() {
            assertThread("Points", creationThread);
            return in.size();
        }

        @Override
        public int getDocCount() {
            assertThread("Points", creationThread);
            return in.getDocCount();
        }

    }

    /**
     * Validates in the 1D case that all points are visited in order, and point
     * values are in bounds of the last cell checked
     */
    static class AssertingIntersectVisitor implements IntersectVisitor {
        final IntersectVisitor in;
        final int numDataDims;
        final int numIndexDims;
        final int bytesPerDim;
        final byte[] lastDocValue;
        final byte[] lastMinPackedValue;
        final byte[] lastMaxPackedValue;
        private Relation lastCompareResult;
        private int lastDocID = -1;
        private int docBudget;

        AssertingIntersectVisitor(int numDataDims, int numIndexDims, int bytesPerDim, IntersectVisitor in) {
            this.in = in;
            this.numDataDims = numDataDims;
            this.numIndexDims = numIndexDims;
            this.bytesPerDim = bytesPerDim;
            lastMaxPackedValue = new byte[numDataDims * bytesPerDim];
            lastMinPackedValue = new byte[numDataDims * bytesPerDim];
            if (numDataDims == 1) {
                lastDocValue = new byte[bytesPerDim];
            } else {
                lastDocValue = null;
            }
        }

        @Override
        public void visit(int docID) throws IOException {
            assert --docBudget >= 0 : "called add() more times than the last call to grow() reserved";

            // This method, not filtering each hit, should only be invoked when the cell is
            // inside the query shape:
            assert lastCompareResult == Relation.CELL_INSIDE_QUERY;
            in.visit(docID);
        }

        @Override
        public void visit(int docID, byte[] packedValue) throws IOException {
            assert --docBudget >= 0 : "called add() more times than the last call to grow() reserved";

            // This method, to filter each doc's value, should only be invoked when the cell
            // crosses the query shape:
            assert lastCompareResult == PointValues.Relation.CELL_CROSSES_QUERY;

            // This doc's packed value should be contained in the last cell passed to
            // compare:
            for (int dim = 0; dim < numIndexDims; dim++) {
                assert Arrays.compareUnsigned(lastMinPackedValue, dim * bytesPerDim,
                        dim * bytesPerDim + bytesPerDim, packedValue, dim * bytesPerDim,
                        dim * bytesPerDim + bytesPerDim) <= 0
                        : "dim=" + dim + " of " + numDataDims + " value=" + new BytesRef(packedValue);
                assert Arrays.compareUnsigned(lastMaxPackedValue, dim * bytesPerDim,
                        dim * bytesPerDim + bytesPerDim, packedValue, dim * bytesPerDim,
                        dim * bytesPerDim + bytesPerDim) >= 0
                        : "dim=" + dim + " of " + numDataDims + " value=" + new BytesRef(packedValue);
            }

            // TODO: we should assert that this "matches" whatever relation the last call to
            // compare had returned
            assert packedValue.length == numDataDims * bytesPerDim;
            if (numDataDims == 1) {
                int cmp = Arrays.compareUnsigned(lastDocValue, 0, bytesPerDim, packedValue, 0, bytesPerDim);
                if (cmp < 0) {
                    // ok
                } else if (cmp == 0) {
                    assert lastDocID <= docID : "doc ids are out of order when point values are the same!";
                } else {
                    // out of order!
                    assert false : "point values are out of order";
                }
                System.arraycopy(packedValue, 0, lastDocValue, 0, bytesPerDim);
                lastDocID = docID;
            }
            in.visit(docID, packedValue);
        }

        @Override
        public void grow(int count) {
            in.grow(count);
            docBudget = count;
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            for (int dim = 0; dim < numIndexDims; dim++) {
                assert Arrays.compareUnsigned(minPackedValue, dim * bytesPerDim, dim * bytesPerDim + bytesPerDim,
                        maxPackedValue, dim * bytesPerDim, dim * bytesPerDim + bytesPerDim) <= 0;
            }
            System.arraycopy(maxPackedValue, 0, lastMaxPackedValue, 0, numIndexDims * bytesPerDim);
            System.arraycopy(minPackedValue, 0, lastMinPackedValue, 0, numIndexDims * bytesPerDim);
            lastCompareResult = in.compare(minPackedValue, maxPackedValue);
            return lastCompareResult;
        }
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
        NumericDocValues dv = super.getNumericDocValues(field);
        FieldInfo fi = getFieldInfos().fieldInfo(field);
        if (dv != null) {
            assert fi != null;
            assert fi.getDocValuesType() == DocValuesType.NUMERIC;
            return new AssertingNumericDocValues(dv, maxDoc());
        } else {
            assert fi == null || fi.getDocValuesType() != DocValuesType.NUMERIC;
            return null;
        }
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
        BinaryDocValues dv = super.getBinaryDocValues(field);
        FieldInfo fi = getFieldInfos().fieldInfo(field);
        if (dv != null) {
            assert fi != null;
            assert fi.getDocValuesType() == DocValuesType.BINARY;
            return new AssertingBinaryDocValues(dv, maxDoc());
        } else {
            assert fi == null || fi.getDocValuesType() != DocValuesType.BINARY;
            return null;
        }
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
        SortedDocValues dv = super.getSortedDocValues(field);
        FieldInfo fi = getFieldInfos().fieldInfo(field);
        if (dv != null) {
            assert fi != null;
            assert fi.getDocValuesType() == DocValuesType.SORTED;
            return new AssertingSortedDocValues(dv, maxDoc());
        } else {
            assert fi == null || fi.getDocValuesType() != DocValuesType.SORTED;
            return null;
        }
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        FieldInfo fi = getFieldInfos().fieldInfo(field);
        SortedNumericDocValues dv = super.getSortedNumericDocValues(field);
        if (dv != null) {
            assert fi != null;
            assert fi.getDocValuesType() == DocValuesType.SORTED_NUMERIC;
            return AssertingSortedNumericDocValues.create(dv, maxDoc());
        } else {
            assert fi == null || fi.getDocValuesType() != DocValuesType.SORTED_NUMERIC;
            return null;
        }
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        SortedSetDocValues dv = super.getSortedSetDocValues(field);
        FieldInfo fi = getFieldInfos().fieldInfo(field);
        if (dv != null) {
            assert fi != null;
            assert fi.getDocValuesType() == DocValuesType.SORTED_SET;
            return new AssertingSortedSetDocValues(dv, maxDoc());
        } else {
            assert fi == null || fi.getDocValuesType() != DocValuesType.SORTED_SET;
            return null;
        }
    }

    @Override
    public NumericDocValues getNormValues(String field) throws IOException {
        NumericDocValues dv = super.getNormValues(field);
        FieldInfo fi = getFieldInfos().fieldInfo(field);
        if (dv != null) {
            assert fi != null;
            assert fi.hasNorms();
            return new AssertingNumericDocValues(dv, maxDoc());
        } else {
            assert fi == null || fi.hasNorms() == false;
            return null;
        }
    }

    @Override
    public PointValues getPointValues(String field) throws IOException {
        PointValues values = in.getPointValues(field);
        if (values == null) {
            return null;
        }
        return new AssertingPointValues(values, maxDoc());
    }

    /** Wraps a Bits but with additional asserts */
    public static class AssertingBits implements Bits {
        private final Thread creationThread = Thread.currentThread();
        final Bits in;

        public AssertingBits(Bits in) {
            this.in = in;
        }

        @Override
        public boolean get(int index) {
            assertThread("Bits", creationThread);
            assert index >= 0 && index < length();
            return in.get(index);
        }

        @Override
        public int length() {
            assertThread("Bits", creationThread);
            return in.length();
        }
    }

    @Override
    public Bits getLiveDocs() {
        Bits liveDocs = super.getLiveDocs();
        if (liveDocs != null) {
            assert maxDoc() == liveDocs.length();
            liveDocs = new AssertingBits(liveDocs);
        } else {
            assert maxDoc() == numDocs();
            assert !hasDeletions();
        }
        return liveDocs;
    }

    // we don't change behavior of the reader: just validate the API.

    @Override
    public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }
}

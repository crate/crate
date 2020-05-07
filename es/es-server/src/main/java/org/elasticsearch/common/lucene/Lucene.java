/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Version;
import org.elasticsearch.ExceptionsHelper;
import javax.annotation.Nullable;
import org.elasticsearch.common.Strings;
import io.crate.common.SuppressForbidden;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Lucene {
    public static final String LATEST_DOC_VALUES_FORMAT = "Lucene80";
    public static final String LATEST_POSTINGS_FORMAT = "Lucene50";
    public static final String LATEST_CODEC = "Lucene84";

    static {
        Deprecated annotation = PostingsFormat.forName(LATEST_POSTINGS_FORMAT).getClass().getAnnotation(Deprecated.class);
        assert annotation == null : "PostingsFromat " + LATEST_POSTINGS_FORMAT + " is deprecated" ;
        annotation = DocValuesFormat.forName(LATEST_DOC_VALUES_FORMAT).getClass().getAnnotation(Deprecated.class);
        assert annotation == null : "DocValuesFormat " + LATEST_DOC_VALUES_FORMAT + " is deprecated" ;
    }

    public static final String SOFT_DELETES_FIELD = "__soft_deletes";

    public static final NamedAnalyzer STANDARD_ANALYZER = new NamedAnalyzer("_standard", AnalyzerScope.GLOBAL, new StandardAnalyzer());
    public static final NamedAnalyzer KEYWORD_ANALYZER = new NamedAnalyzer("_keyword", AnalyzerScope.GLOBAL, new KeywordAnalyzer());

    public static Version parseVersion(@Nullable String version, Version defaultVersion, Logger logger) {
        if (version == null) {
            return defaultVersion;
        }
        try {
            return Version.parse(version);
        } catch (ParseException e) {
            logger.warn(() -> new ParameterizedMessage("no version match {}, default to {}", version, defaultVersion), e);
            return defaultVersion;
        }
    }

    /**
     * Reads the segments infos, failing if it fails to load
     */
    public static SegmentInfos readSegmentInfos(Directory directory) throws IOException {
        return SegmentInfos.readLatestCommit(directory);
    }

    /**
     * Returns an iterable that allows to iterate over all files in this segments info
     */
    public static Iterable<String> files(SegmentInfos infos) throws IOException {
        final List<Collection<String>> list = new ArrayList<>();
        list.add(Collections.singleton(infos.getSegmentsFileName()));
        for (SegmentCommitInfo info : infos) {
            list.add(info.files());
        }
        return Iterables.flatten(list);
    }

    /**
     * Returns the number of documents in the index referenced by this {@link SegmentInfos}
     */
    public static int getNumDocs(SegmentInfos info) {
        int numDocs = 0;
        for (SegmentCommitInfo si : info) {
            numDocs += si.info.maxDoc() - si.getDelCount() - si.getSoftDelCount();
        }
        return numDocs;
    }

    /**
     * Reads the segments infos from the given commit, failing if it fails to load
     */
    public static SegmentInfos readSegmentInfos(IndexCommit commit) throws IOException {
        // Using commit.getSegmentsFileName() does NOT work here, have to
        // manually create the segment filename
        String filename = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", commit.getGeneration());
        return SegmentInfos.readCommit(commit.getDirectory(), filename);
    }

    /**
     * Reads the segments infos from the given segments file name, failing if it fails to load
     */
    private static SegmentInfos readSegmentInfos(String segmentsFileName, Directory directory) throws IOException {
        return SegmentInfos.readCommit(directory, segmentsFileName);
    }

    /**
     * This method removes all files from the given directory that are not referenced by the given segments file.
     * This method will open an IndexWriter and relies on index file deleter to remove all unreferenced files. Segment files
     * that are newer than the given segments file are removed forcefully to prevent problems with IndexWriter opening a potentially
     * broken commit point / leftover.
     * <b>Note:</b> this method will fail if there is another IndexWriter open on the given directory. This method will also acquire
     * a write lock from the directory while pruning unused files. This method expects an existing index in the given directory that has
     * the given segments file.
     */
    public static SegmentInfos pruneUnreferencedFiles(String segmentsFileName, Directory directory) throws IOException {
        final SegmentInfos si = readSegmentInfos(segmentsFileName, directory);
        try (Lock writeLock = directory.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
            int foundSegmentFiles = 0;
            for (final String file : directory.listAll()) {
                /**
                 * we could also use a deletion policy here but in the case of snapshot and restore
                 * sometimes we restore an index and override files that were referenced by a "future"
                 * commit. If such a commit is opened by the IW it would likely throw a corrupted index exception
                 * since checksums don's match anymore. that's why we prune the name here directly.
                 * We also want the caller to know if we were not able to remove a segments_N file.
                 */
                if (file.startsWith(IndexFileNames.SEGMENTS) || file.equals(IndexFileNames.OLD_SEGMENTS_GEN)) {
                    foundSegmentFiles++;
                    if (file.equals(si.getSegmentsFileName()) == false) {
                        directory.deleteFile(file); // remove all segment_N files except of the one we wanna keep
                    }
                }
            }
            assert SegmentInfos.getLastCommitSegmentsFileName(directory).equals(segmentsFileName);
            if (foundSegmentFiles == 0) {
                throw new IllegalStateException("no commit found in the directory");
            }
        }
        final CommitPoint cp = new CommitPoint(si, directory);
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(Lucene.STANDARD_ANALYZER)
                .setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
                .setIndexCommit(cp)
                .setCommitOnClose(false)
                .setMergePolicy(NoMergePolicy.INSTANCE)
                .setOpenMode(IndexWriterConfig.OpenMode.APPEND))) {
            // do nothing and close this will kick of IndexFileDeleter which will remove all pending files
        }
        return si;
    }

    /**
     * This method removes all lucene files from the given directory. It will first try to delete all commit points / segments
     * files to ensure broken commits or corrupted indices will not be opened in the future. If any of the segment files can't be deleted
     * this operation fails.
     */
    public static void cleanLuceneIndex(Directory directory) throws IOException {
        try (Lock writeLock = directory.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
            for (final String file : directory.listAll()) {
                if (file.startsWith(IndexFileNames.SEGMENTS) || file.equals(IndexFileNames.OLD_SEGMENTS_GEN)) {
                    directory.deleteFile(file); // remove all segment_N files
                }
            }
        }
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(Lucene.STANDARD_ANALYZER)
                .setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
                .setMergePolicy(NoMergePolicy.INSTANCE) // no merges
                .setCommitOnClose(false) // no commits
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE))) { // force creation - don't append...
            // do nothing and close this will kick of IndexFileDeleter which will remove all pending files
        }
    }

    public static void checkSegmentInfoIntegrity(final Directory directory) throws IOException {
        new SegmentInfos.FindSegmentsFile(directory) {

            @Override
            protected Object doBody(String segmentFileName) throws IOException {
                try (IndexInput input = directory.openInput(segmentFileName, IOContext.READ)) {
                    CodecUtil.checksumEntireFile(input);
                }
                return null;
            }
        }.run();
    }

    /**
     * Check whether there is one or more documents matching the provided query.
     */
    public static boolean exists(IndexSearcher searcher, Query query) throws IOException {
        final Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        // the scorer API should be more efficient at stopping after the first
        // match than the bulk scorer API
        for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
            final Scorer scorer = weight.scorer(context);
            if (scorer == null) {
                continue;
            }
            final Bits liveDocs = context.reader().getLiveDocs();
            final DocIdSetIterator iterator = scorer.iterator();
            for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                if (liveDocs == null || liveDocs.get(doc)) {
                    return true;
                }
            }
        }
        return false;
    }

    private Lucene() {

    }

    public static boolean indexExists(final Directory directory) throws IOException {
        return DirectoryReader.indexExists(directory);
    }

    /**
     * Returns {@code true} iff the given exception or
     * one of it's causes is an instance of {@link CorruptIndexException},
     * {@link IndexFormatTooOldException}, or {@link IndexFormatTooNewException} otherwise {@code false}.
     */
    public static boolean isCorruptionException(Throwable t) {
        return ExceptionsHelper.unwrapCorruption(t) != null;
    }

    /**
     * Parses the version string lenient and returns the default value if the given string is null or empty
     */
    public static Version parseVersionLenient(String toParse, Version defaultValue) {
        return LenientParser.parse(toParse, defaultValue);
    }

    /**
     * Tries to extract a segment reader from the given index reader.
     * If no SegmentReader can be extracted an {@link IllegalStateException} is thrown.
     */
    public static SegmentReader segmentReader(LeafReader reader) {
        if (reader instanceof SegmentReader) {
            return (SegmentReader) reader;
        } else if (reader instanceof FilterLeafReader) {
            final FilterLeafReader fReader = (FilterLeafReader) reader;
            return segmentReader(FilterLeafReader.unwrap(fReader));
        } else if (reader instanceof FilterCodecReader) {
            final FilterCodecReader fReader = (FilterCodecReader) reader;
            return segmentReader(FilterCodecReader.unwrap(fReader));
        }
        // hard fail - we can't get a SegmentReader
        throw new IllegalStateException("Can not extract segment reader from given index reader [" + reader + "]");
    }

    @SuppressForbidden(reason = "Version#parseLeniently() used in a central place")
    private static final class LenientParser {
        public static Version parse(String toParse, Version defaultValue) {
            if (Strings.hasLength(toParse)) {
                try {
                    return Version.parseLeniently(toParse);
                } catch (ParseException e) {
                    // pass to default
                }
            }
            return defaultValue;
        }
    }

    private static final class CommitPoint extends IndexCommit {
        private String segmentsFileName;
        private final Collection<String> files;
        private final Directory dir;
        private final long generation;
        private final Map<String,String> userData;
        private final int segmentCount;

        private CommitPoint(SegmentInfos infos, Directory dir) throws IOException {
            segmentsFileName = infos.getSegmentsFileName();
            this.dir = dir;
            userData = infos.getUserData();
            files = Collections.unmodifiableCollection(infos.files(true));
            generation = infos.getGeneration();
            segmentCount = infos.size();
        }

        @Override
        public String toString() {
            return "DirectoryReader.ReaderCommit(" + segmentsFileName + ")";
        }

        @Override
        public int getSegmentCount() {
            return segmentCount;
        }

        @Override
        public String getSegmentsFileName() {
            return segmentsFileName;
        }

        @Override
        public Collection<String> getFileNames() {
            return files;
        }

        @Override
        public Directory getDirectory() {
            return dir;
        }

        @Override
        public long getGeneration() {
            return generation;
        }

        @Override
        public boolean isDeleted() {
            return false;
        }

        @Override
        public Map<String,String> getUserData() {
            return userData;
        }

        @Override
        public void delete() {
            throw new UnsupportedOperationException("This IndexCommit does not support deletions");
        }
    }

    /**
     * Given a {@link ScorerSupplier}, return a {@link Bits} instance that will match
     * all documents contained in the set. Note that the returned {@link Bits}
     * instance MUST be consumed in order.
     */
    public static Bits asSequentialAccessBits(final int maxDoc, @Nullable ScorerSupplier scorerSupplier) throws IOException {
        if (scorerSupplier == null) {
            return new Bits.MatchNoBits(maxDoc);
        }
        // Since we want bits, we need random-access
        final Scorer scorer = scorerSupplier.get(Long.MAX_VALUE); // this never returns null
        final TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
        final DocIdSetIterator iterator;
        if (twoPhase == null) {
            iterator = scorer.iterator();
        } else {
            iterator = twoPhase.approximation();
        }

        return new Bits() {

            int previous = -1;
            boolean previousMatched = false;

            @Override
            public boolean get(int index) {
                if (index < 0 || index >= maxDoc) {
                    throw new IndexOutOfBoundsException(index + " is out of bounds: [" + 0 + "-" + maxDoc + "[");
                }
                if (index < previous) {
                    throw new IllegalArgumentException("This Bits instance can only be consumed in order. "
                            + "Got called on [" + index + "] while previously called on [" + previous + "]");
                }
                if (index == previous) {
                    // we cache whether it matched because it is illegal to call
                    // twoPhase.matches() twice
                    return previousMatched;
                }
                previous = index;

                int doc = iterator.docID();
                if (doc < index) {
                    try {
                        doc = iterator.advance(index);
                    } catch (IOException e) {
                        throw new IllegalStateException("Cannot advance iterator", e);
                    }
                }
                if (index == doc) {
                    try {
                        return previousMatched = twoPhase == null || twoPhase.matches();
                    } catch (IOException e) {
                        throw new IllegalStateException("Cannot validate match", e);
                    }
                }
                return previousMatched = false;
            }

            @Override
            public int length() {
                return maxDoc;
            }
        };
    }

    /**
     * Wraps a directory reader to make all documents live except those were rolled back
     * or hard-deleted due to non-aborting exceptions during indexing.
     * The wrapped reader can be used to query all documents.
     *
     * @param in the input directory reader
     * @return the wrapped reader
     */
    public static DirectoryReader wrapAllDocsLive(DirectoryReader in) throws IOException {
        return new DirectoryReaderWithAllLiveDocs(in);
    }

    private static final class DirectoryReaderWithAllLiveDocs extends FilterDirectoryReader {

        static final class LeafReaderWithLiveDocs extends FilterLeafReader {
            final Bits liveDocs;
            final int numDocs;

            LeafReaderWithLiveDocs(LeafReader in, Bits liveDocs, int numDocs) {
                super(in);
                this.liveDocs = liveDocs;
                this.numDocs = numDocs;
            }

            @Override
            public Bits getLiveDocs() {
                return liveDocs;
            }

            @Override
            public int numDocs() {
                return numDocs;
            }

            @Override
            public CacheHelper getCoreCacheHelper() {
                return in.getCoreCacheHelper();
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null; // Modifying liveDocs
            }
        }

        DirectoryReaderWithAllLiveDocs(DirectoryReader in) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader leaf) {
                    SegmentReader segmentReader = segmentReader(leaf);
                    Bits hardLiveDocs = segmentReader.getHardLiveDocs();
                    if (hardLiveDocs == null) {
                        return new LeafReaderWithLiveDocs(leaf, null, leaf.maxDoc());
                    }
                    // TODO: Can we avoid calculate numDocs by using SegmentReader#getSegmentInfo with LUCENE-8458?
                    int numDocs = 0;
                    for (int i = 0; i < hardLiveDocs.length(); i++) {
                        if (hardLiveDocs.get(i)) {
                            numDocs++;
                        }
                    }
                    return new LeafReaderWithLiveDocs(segmentReader, hardLiveDocs, numDocs);
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return wrapAllDocsLive(in);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null; // Modifying liveDocs
        }
    }

    /**
     * Returns a numeric docvalues which can be used to soft-delete documents.
     */
    public static NumericDocValuesField newSoftDeletesField() {
        return new NumericDocValuesField(SOFT_DELETES_FIELD, 1);
    }
}

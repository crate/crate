/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.statistics;

import io.crate.common.io.IOUtils;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.TableInfo;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.IntPredicate;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.jetbrains.annotations.Nullable;

/**
 * Holds table statistics that are updated periodically by {@link TableStatsService}.
 */
public class TableStats {

    private final Path dataPath;

    private static final String STATS_LOCATION = "_stats";
    private static final String RELATION_NAME_FIELD = "relationName";

    private volatile Map<RelationName, Stats> tableStats = new HashMap<>();

    public TableStats() {
        this(null);
    }

    public TableStats(Path dataPath) {
        this.dataPath = dataPath;
    }

    public void updateTableStats(Map<RelationName, Stats> tableStats) {
        this.tableStats = tableStats;
    }

    /**
     * Returns the number of docs a table has.
     * <p>
     * <p>
     * The returned number isn't an accurate real-time value but a cached value that is periodically updated
     * </p>
     * Returns -1 if the table isn't in the cache
     */
    public long numDocs(RelationName relationName) {
        return tableStats.getOrDefault(relationName, Stats.EMPTY).numDocs;
    }

    /**
     * Returns an estimation (avg) size of each row of the table in bytes.
     * <p>
     * <p>
     * The returned number isn't an accurate real-time value but a cached value that is periodically updated
     * </p>
     * Returns -1 if the table isn't in the cache
     */
    public long estimatedSizePerRow(RelationName relationName) {
        return tableStats.getOrDefault(relationName, Stats.EMPTY).averageSizePerRowInBytes();
    }

    /**
     * Returns an estimation (avg) size of each row of the table in bytes or if no stats are available
     * for the given table an estimate (avg) based on the column types of the table.
     */
    public long estimatedSizePerRow(TableInfo tableInfo) {
        Stats stats = tableStats.get(tableInfo.ident());
        if (stats == null) {
            // if stats are not available we fall back to estimate the size based on
            // column types. Therefore we need to get the column information.
            return Stats.EMPTY.estimateSizeForColumns(tableInfo);
        } else {
            return stats.averageSizePerRowInBytes();
        }
    }

    public Iterable<ColumnStatsEntry> statsEntries() {
        Set<Map.Entry<RelationName, Stats>> entries = tableStats.entrySet();
        return () -> entries.stream()
            .flatMap(tableEntry -> {
                Stats stats = tableEntry.getValue();
                return stats.statsByColumn().entrySet().stream()
                    .map(columnEntry ->
                        new ColumnStatsEntry(tableEntry.getKey(), columnEntry.getKey(), columnEntry.getValue()));
            }).iterator();
    }

    public Stats getStats(RelationName relationName) {
        return tableStats.getOrDefault(relationName, Stats.EMPTY);
    }

    public Map<RelationName, Stats> values() {
        return Collections.unmodifiableMap(tableStats);
    }

    private Writer createWriter() throws IOException {
        Path path = dataPath.resolve(STATS_LOCATION);
        Directory directory = new NIOFSDirectory(path);
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
        indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        indexWriterConfig.setRAMBufferSizeMB(1.0);
        indexWriterConfig.setMergeScheduler(new SerialMergeScheduler());
        IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
        return new Writer(directory, indexWriter);
    }

    @Nullable
    public Stats loadStats(RelationName relationName) throws IOException {
        Path path = dataPath.resolve(STATS_LOCATION);
        if (Files.exists(path)) {
            try (Directory dir = new NIOFSDirectory(path)) {
                DirectoryReader reader;
                try {
                    reader = DirectoryReader.open(dir);
                } catch (IndexNotFoundException e) {
                    return null;
                }
                IndexSearcher indexSearcher = new IndexSearcher(reader);
                indexSearcher.setQueryCache(null);
                Query query = new TermQuery(new Term(RELATION_NAME_FIELD, relationName.fqn()));
                Weight weight = indexSearcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 0.0f);
                try (IndexReader indexReader = indexSearcher.getIndexReader()) {
                    for (LeafReaderContext leafReaderContext : indexReader.leaves()) {
                        final Scorer scorer = weight.scorer(leafReaderContext);
                        if (scorer != null) {
                            final Bits liveDocs = leafReaderContext.reader().getLiveDocs();
                            final IntPredicate isLiveDoc = liveDocs == null ? i -> true : liveDocs::get;
                            final DocIdSetIterator docIdSetIterator = scorer.iterator();
                            StoredFields storedFields = leafReaderContext.reader().storedFields();
                            while (docIdSetIterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                                if (isLiveDoc.test(docIdSetIterator.docID())) {
                                    BytesRef binaryValue = storedFields.document(docIdSetIterator.docID()).getBinaryValue(STATS_LOCATION);
                                    return new Stats(
                                        new InputStreamStreamInput(
                                            new ByteArrayInputStream(
                                                binaryValue.bytes
                                            )
                                        )
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    private static final class Writer implements Closeable {
        final Directory directory;
        final IndexWriter indexWriter;

        private Writer(Directory directory, IndexWriter indexWriter) {
            this.directory = directory;
            this.indexWriter = indexWriter;
        }

        void updateDoc(RelationName relationName, Document statsDoc) throws IOException {
            indexWriter.updateDocument(new Term(RELATION_NAME_FIELD, relationName.fqn()), statsDoc);
        }

        void flush() throws IOException {
            this.indexWriter.flush();
        }

        void commit() throws IOException {
            indexWriter.commit();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(indexWriter, directory);
        }

        void prepareCommit() throws IOException {
            indexWriter.prepareCommit();
        }

        private Document makeDocument(RelationName relationName, Stats stats) throws IOException {
            BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
            stats.writeTo(bytesStreamOutput);
            Document document = new Document();
            document.add(new StringField(RELATION_NAME_FIELD, relationName.fqn(), Field.Store.NO));
            document.add(new StoredField(STATS_LOCATION, bytesStreamOutput.bytes().toBytesRef()));
            return document;
        }
    }

    public void writeToDisk(TableStats tableStats) {
        try {
            try (Writer writer = createWriter()) {
                Map<RelationName, Stats> values = tableStats.values();
                for (Map.Entry<RelationName, Stats> entry : values.entrySet()) {
                    RelationName relationName = entry.getKey();
                    Stats stats = entry.getValue();
                    Document doc = writer.makeDocument(relationName, stats);
                    writer.updateDoc(relationName, doc);
                }
                writer.prepareCommit();
                writer.flush();
                writer.commit();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void writeToDisk(RelationName relationName, Stats stats) {
        try {
            try (Writer writer = createWriter()) {
                Document doc = writer.makeDocument(relationName, stats);
                writer.updateDoc(relationName, doc);
                writer.prepareCommit();
                writer.flush();
                writer.commit();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

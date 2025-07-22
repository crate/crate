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

package io.crate.statistics;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import io.crate.common.io.IOUtils;
import io.crate.metadata.RelationName;

public final class PersistedStatsService implements StatsService {

    private static final Logger LOGGER = LogManager.getLogger(PersistedStatsService.class);
    private static final String STATS = "_stats";
    private static final String DATA_FIELD = "data";
    private static final String RELATION_NAME_FIELD = "relationName";

    private final LoadingCache<RelationName, Stats> cache = Caffeine.newBuilder()
            .expireAfterWrite(1, TimeUnit.MINUTES)
            .maximumSize(100)
            .build(this::loadFromDisk);

    private final Path dataPath;

    public PersistedStatsService(Path dataPath) {
        this.dataPath = dataPath;
    }

    @Override
    public @Nullable Stats get(RelationName relationName) {
        return cache.get(relationName);
    }

    @Override
    public Stats getOrDefault(RelationName relationName, Stats defaultValue) {
        Stats result = get(relationName);
        return result == null ? defaultValue : result;
    }

    @Override
    public void remove(RelationName relationName) {
        try {
            try (Writer writer = createWriter(dataPath)) {
                writer.delete(relationName);
                writer.flush();
                writer.commit();
            }
            cache.invalidate(relationName);
        } catch (IOException e) {
            throw new RuntimeException("Can't delete TableStats from disk", e);
        }
    }

    @Override
    public void clear() {
        try {
            try (Writer writer = createWriter(dataPath)) {
                writer.deleteAll();
                writer.flush();
                writer.commit();
            }
            cache.invalidateAll();
        } catch (IOException e) {
            throw new RuntimeException("Can't delete TableStats from disk", e);
        }
    }

    @VisibleForTesting
    @Nullable
    Stats loadFromDisk(RelationName relationName) {
        Path path = dataPath.resolve(STATS);
        if (Files.exists(path)) {
            try {
                try (Directory dir = new NIOFSDirectory(path)) {
                    DirectoryReader reader;
                    try {
                        reader = DirectoryReader.open(dir);
                    } catch (IndexNotFoundException e) {
                        LOGGER.warn("Try to load Stats from %s but no index exists %s", path.toString(), e.getMessage());
                        return null;
                    }
                    IndexSearcher indexSearcher = new IndexSearcher(reader);
                    indexSearcher.setQueryCache(null);
                    Query query = new TermQuery(new Term(RELATION_NAME_FIELD, relationName.fqn()));
                    Weight weight = indexSearcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 0.0f);
                    try (IndexReader indexReader = indexSearcher.getIndexReader()) {
                        for (LeafReaderContext leafReaderContext : indexReader.leaves()) {
                            Scorer scorer = weight.scorer(leafReaderContext);
                            if (scorer != null) {
                                DocIdSetIterator docIdSetIterator = scorer.iterator();
                                StoredFields storedFields = leafReaderContext.reader().storedFields();
                                if (docIdSetIterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                                    Document doc = storedFields.document(docIdSetIterator.docID());
                                    BytesRef binaryValue = doc.getBinaryValue(DATA_FIELD);
                                    ByteArrayInputStream bis = new ByteArrayInputStream(binaryValue.bytes);
                                    return Stats.readFrom(new InputStreamStreamInput(bis));
                                }
                            }
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Can't load TableStats from disk", e);
            }
        }
        return null;
    }


    private static Writer createWriter(Path path) throws IOException {
        Path dataPath = path.resolve(STATS);
        Directory directory = new NIOFSDirectory(dataPath);
        boolean openExisting = DirectoryReader.indexExists(directory);

        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
        indexWriterConfig.setOpenMode(openExisting ? IndexWriterConfig.OpenMode.APPEND : IndexWriterConfig.OpenMode.CREATE);
        indexWriterConfig.setMergeScheduler(new SerialMergeScheduler());
        IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
        return new Writer(directory, indexWriter);
    }

    public void add(Map<RelationName, Stats> stats) {
        try {
            try (Writer writer = createWriter(dataPath)) {
                for (Map.Entry<RelationName, Stats> entry : stats.entrySet()) {
                    RelationName relationName = entry.getKey();
                    Document doc = writer.makeDocument(relationName, entry.getValue());
                    writer.updateDoc(relationName, doc);
                }
                writer.prepareCommit();
                writer.flush();
                writer.commit();
            }
        } catch (IOException e) {
            throw new RuntimeException("Can't load TableStats from disk", e);
        }
    }

    public void add(RelationName relationName, Stats stats) {
        try {
            try (Writer writer = createWriter(dataPath)) {
                Document doc = writer.makeDocument(relationName, stats);
                writer.updateDoc(relationName, doc);
                writer.prepareCommit();
                writer.flush();
                writer.commit();
            }
        } catch (IOException e) {
            throw new RuntimeException("Can't write TableStats to disk ", e);
        }
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

        void delete(RelationName relationName) throws IOException {
            indexWriter.deleteDocuments(new Term(RELATION_NAME_FIELD, relationName.fqn()));
        }

        void deleteAll() throws IOException {
            indexWriter.deleteAll();
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
            document.add(new StoredField(DATA_FIELD, bytesStreamOutput.bytes().toBytesRef()));
            return document;
        }
    }
}

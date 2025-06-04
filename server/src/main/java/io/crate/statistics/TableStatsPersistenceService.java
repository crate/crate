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
import java.util.function.Function;

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
import org.elasticsearch.common.lucene.Lucene;
import org.jetbrains.annotations.Nullable;

import io.crate.common.io.IOUtils;
import io.crate.metadata.RelationName;

public class TableStatsPersistenceService implements Function<RelationName, Stats> {

    private static final String STATS = "_stats";
    private static final String DATA_FIELD = "data";
    private static final String RELATION_NAME_FIELD = "relationName";

    private final Path dataPath;

    public TableStatsPersistenceService(Path dataPath) {
        this.dataPath = dataPath;
    }

    @Nullable
    public Stats apply(RelationName relationName) {
        Path path = dataPath.resolve(STATS);
        if (Files.exists(path)) {
            try {
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
                            Scorer scorer = weight.scorer(leafReaderContext);
                            if (scorer != null) {
                                DocIdSetIterator docIdSetIterator = scorer.iterator();
                                StoredFields storedFields = leafReaderContext.reader().storedFields();
                                while (docIdSetIterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                                    Document doc = storedFields.document(docIdSetIterator.docID());
                                    BytesRef binaryValue = doc.getBinaryValue(DATA_FIELD);
                                    ByteArrayInputStream bis = new ByteArrayInputStream(binaryValue.bytes);
                                    return new Stats(new InputStreamStreamInput(bis));

                                }
                            }
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    private static Writer createWriter(Path path) throws IOException {
        Directory directory = new NIOFSDirectory(path.resolve(STATS));
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
        indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        indexWriterConfig.setMergeScheduler(new SerialMergeScheduler());
        IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
        return new Writer(directory, indexWriter);
    }

    public void deleteTableStats() {
        try {
            Lucene.cleanLuceneIndex(new NIOFSDirectory(dataPath.resolve(STATS)));
        } catch (IOException e) {
            throw new RuntimeException("Can not load TableStats from disk", e);
        }
    }

    public void write(Map<RelationName, Stats> stats) {
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

    public void write(RelationName relationName, Stats stats) {
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

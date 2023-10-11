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

package org.elasticsearch.index.engine;

import java.io.IOException;
import java.util.Collections;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.translog.Translog;

/**
 * Internal class that mocks a single doc read from the transaction log as a leaf reader.
 */
final class TranslogLeafReader extends LeafReader {

    private final Translog.Index operation;
    private static final FieldInfo FAKE_SOURCE_FIELD
        = new FieldInfo(SourceFieldMapper.NAME, 1, false, false, false, IndexOptions.NONE, DocValuesType.NONE, -1, Collections.emptyMap(),
        0, 0, 0, 0, VectorEncoding.BYTE, VectorSimilarityFunction.EUCLIDEAN, false);
    private static final FieldInfo FAKE_ID_FIELD
        = new FieldInfo(IdFieldMapper.NAME, 3, false, false, false, IndexOptions.NONE, DocValuesType.NONE, -1, Collections.emptyMap(),
        0, 0, 0, 0, VectorEncoding.BYTE, VectorSimilarityFunction.EUCLIDEAN, false);

    TranslogLeafReader(Translog.Index operation) {
        this.operation = operation;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Terms terms(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NumericDocValues getNormValues(String field) {
        throw new UnsupportedOperationException();
    }


    @Override
    public FieldInfos getFieldInfos() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Bits getLiveDocs() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PointValues getPointValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkIntegrity() {

    }

    @Override
    public LeafMetaData getMetaData() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Fields getTermVectors(int docID) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int numDocs() {
        return 1;
    }

    @Override
    public int maxDoc() {
        return 1;
    }

    @Override
    public void document(int docID, StoredFieldVisitor visitor) throws IOException {
        docFromOperation(docID, visitor, operation);
    }

    @Override
    protected void doClose() {

    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void searchNearestVectors(String field,
                                     float[] target,
                                     KnnCollector knnCollector,
                                     Bits acceptDocs) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void searchNearestVectors(String field,
                                     byte[] target,
                                     KnnCollector knnCollector,
                                     Bits acceptDocs) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public TermVectors termVectors() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StoredFields storedFields() throws IOException {
        return new StoredFields() {

            @Override
            public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                docFromOperation(docID, visitor, operation);
            }
        };
    }

    private static void docFromOperation(int docID, StoredFieldVisitor visitor, Translog.Index operation) throws IOException {
        if (docID != 0) {
            throw new IllegalArgumentException("no such doc ID " + docID);
        }
        if (visitor.needsField(FAKE_SOURCE_FIELD) == StoredFieldVisitor.Status.YES) {
            assert operation.getSource().toBytesRef().offset == 0;
            assert operation.getSource().toBytesRef().length == operation.getSource().toBytesRef().bytes.length;
            visitor.binaryField(FAKE_SOURCE_FIELD, operation.getSource().toBytesRef().bytes);
        }
        if (visitor.needsField(FAKE_ID_FIELD) == StoredFieldVisitor.Status.YES) {
            visitor.stringField(FAKE_ID_FIELD, operation.id());
        }
    }
}

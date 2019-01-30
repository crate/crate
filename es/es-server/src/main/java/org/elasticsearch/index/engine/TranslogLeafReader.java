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

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.index.fielddata.AbstractSortedDocValues;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

/**
 * Internal class that mocks a single doc read from the transaction log as a leaf reader.
 */
final class TranslogLeafReader extends LeafReader {

    private final Translog.Index operation;
    private static final FieldInfo FAKE_SOURCE_FIELD
        = new FieldInfo(SourceFieldMapper.NAME, 1, false, false, false, IndexOptions.NONE, DocValuesType.NONE, -1, Collections.emptyMap(),
        0, 0, false);
    private static final FieldInfo FAKE_ROUTING_FIELD
        = new FieldInfo(RoutingFieldMapper.NAME, 2, false, false, false, IndexOptions.NONE, DocValuesType.NONE, -1, Collections.emptyMap(),
        0, 0, false);
    private static final FieldInfo FAKE_ID_FIELD
        = new FieldInfo(IdFieldMapper.NAME, 3, false, false, false, IndexOptions.NONE, DocValuesType.NONE, -1, Collections.emptyMap(),
        0, 0, false);
    private static final FieldInfo FAKE_UID_FIELD
        = new FieldInfo(UidFieldMapper.NAME, 4, false, false, false, IndexOptions.NONE, DocValuesType.NONE, -1, Collections.emptyMap(),
        0, 0, false);
    private final Version indexVersionCreated;

    TranslogLeafReader(Translog.Index operation, Version indexVersionCreated) {
        this.operation = operation;
        this.indexVersionCreated = indexVersionCreated;
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
        // TODO this can be removed in 7.0 and upwards we don't support the parent field anymore
        if (field.startsWith(ParentFieldMapper.NAME + "#") && operation.parent() != null) {
            return new AbstractSortedDocValues() {
                @Override
                public int docID() {
                    return 0;
                }

                private final BytesRef term = new BytesRef(operation.parent());
                private int ord;
                @Override
                public boolean advanceExact(int docID) {
                    if (docID != 0) {
                        throw new IndexOutOfBoundsException("do such doc ID: " + docID);
                    }
                    ord = 0;
                    return true;
                }

                @Override
                public int ordValue() {
                    return ord;
                }

                @Override
                public BytesRef lookupOrd(int ord) {
                    if (ord == 0) {
                        return term;
                    }
                    return null;
                }

                @Override
                public int getValueCount() {
                    return 1;
                }
            };
        }
        if (operation.parent() == null) {
            return null;
        }
        assert false : "unexpected field: " + field;
        return null;
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
        if (docID != 0) {
            throw new IllegalArgumentException("no such doc ID " + docID);
        }
        if (visitor.needsField(FAKE_SOURCE_FIELD) == StoredFieldVisitor.Status.YES) {
            assert operation.source().toBytesRef().offset == 0;
            assert operation.source().toBytesRef().length == operation.source().toBytesRef().bytes.length;
            visitor.binaryField(FAKE_SOURCE_FIELD, operation.source().toBytesRef().bytes);
        }
        if (operation.routing() != null && visitor.needsField(FAKE_ROUTING_FIELD) == StoredFieldVisitor.Status.YES) {
            visitor.stringField(FAKE_ROUTING_FIELD, operation.routing().getBytes(StandardCharsets.UTF_8));
        }
        if (visitor.needsField(FAKE_ID_FIELD) == StoredFieldVisitor.Status.YES) {
            final byte[] id;
            if (indexVersionCreated.onOrAfter(Version.V_6_0_0)) {
                BytesRef bytesRef = Uid.encodeId(operation.id());
                id = new byte[bytesRef.length];
                System.arraycopy(bytesRef.bytes, bytesRef.offset, id, 0, bytesRef.length);
            } else { // TODO this can go away in 7.0 after backport
                id = operation.id().getBytes(StandardCharsets.UTF_8);
            }
            visitor.stringField(FAKE_ID_FIELD, id);
        }
        if (visitor.needsField(FAKE_UID_FIELD) == StoredFieldVisitor.Status.YES) {
            visitor.stringField(FAKE_UID_FIELD,  Uid.createUid(operation.type(), operation.id()).getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    protected void doClose() {

    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        throw new UnsupportedOperationException();
    }
}

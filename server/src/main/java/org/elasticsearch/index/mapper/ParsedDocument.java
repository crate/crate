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

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;

import io.crate.metadata.doc.SysColumns;

/**
 * The result of parsing a document.
 */
public class ParsedDocument {

    public static ParsedDocument createDeleteTombstoneDoc(String index, String id) throws MapperParsingException {
        Document doc = new Document();

        BytesRef idBytes = Uid.encodeId(id);
        doc.add(new Field(SysColumns.Names.ID, idBytes, SysColumns.ID.FIELD_TYPE));

        NumericDocValuesField version = new NumericDocValuesField(SysColumns.VERSION.name(), -1L);
        doc.add(version);

        SequenceIDFields seqID = SequenceIDFields.emptySeqID();
        doc.add(seqID.seqNo);
        doc.add(seqID.seqNoDocValue);
        doc.add(seqID.primaryTerm);
        return new ParsedDocument(
            version,
            seqID,
            id,
            doc,
            new BytesArray("{}")
        ).toTombstone();
    }

    public static ParsedDocument createNoopTombstoneDoc(String index, String reason) throws MapperParsingException {
        Document doc = new Document();

        final String id = ""; // _id won't be used.

        NumericDocValuesField version = new NumericDocValuesField(SysColumns.VERSION.name(), -1L);
        doc.add(version);

        SequenceIDFields seqID = SequenceIDFields.emptySeqID();
        doc.add(seqID.seqNo);
        doc.add(seqID.seqNoDocValue);
        doc.add(seqID.primaryTerm);
        ParsedDocument parsedDoc = new ParsedDocument(
            version,
            seqID,
            id,
            doc,
            new BytesArray("{}")
        ).toTombstone();
        // Store the reason of a noop as a raw string in the _source field
        final BytesRef byteRef = new BytesRef(reason);
        doc.add(new StoredField(SysColumns.Source.NAME, byteRef.bytes, byteRef.offset, byteRef.length));
        return parsedDoc;
    }

    private final Field version;

    private final String id;
    private final SequenceIDFields seqID;

    private final Document document;

    private final BytesReference source;

    public ParsedDocument(Field version,
                          SequenceIDFields seqID,
                          String id,
                          Document document,
                          BytesReference source) {
        this.version = version;
        this.seqID = seqID;
        this.id = id;
        this.document = document;
        this.source = source;
    }

    public String id() {
        return this.id;
    }

    public Field version() {
        return version;
    }

    public void updateSeqID(long sequenceNumber, long primaryTerm) {
        this.seqID.seqNo.setLongValue(sequenceNumber);
        this.seqID.seqNoDocValue.setLongValue(sequenceNumber);
        this.seqID.primaryTerm.setLongValue(primaryTerm);
    }

    /**
     * Makes the processing document as a tombstone document rather than a regular document.
     * Tombstone documents are stored in Lucene index to represent delete operations or Noops.
     */
    ParsedDocument toTombstone() {
        this.seqID.tombstoneField.setLongValue(1);
        doc().add(this.seqID.tombstoneField);
        return this;
    }

    public Document doc() {
        return document;
    }

    public BytesReference source() {
        return this.source;
    }

    @Override
    public String toString() {
        return "Document id[" + id + "] doc [" + document + ']';
    }
}

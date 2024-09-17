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

package io.crate.execution.dml;

import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SequenceIDFields;
import org.elasticsearch.index.mapper.Uid;

import io.crate.data.Input;
import io.crate.expression.reference.doc.lucene.StoredRowLookup;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.doc.DocSysColumns;

/**
 * Used by ValueIndexer implementations to construct a lucene document for indexing
 */
public class IndexDocumentBuilder {

    private final Document doc;
    private final TranslogWriter translogWriter;
    private final ValueIndexer.Synthetics synthetics;
    private final Map<ColumnIdent, Indexer.ColumnConstraint> constraints;
    private final Version tableVersionCreated;

    /**
     * Builds a new IndexDocumentBuilder
     */
    public IndexDocumentBuilder(
        TranslogWriter translogWriter,
        ValueIndexer.Synthetics synthetics,
        Map<ColumnIdent, Indexer.ColumnConstraint> constraints,
        Version tableVersionCreated
    ) {
        this(new Document(), translogWriter, synthetics, constraints, tableVersionCreated);
    }

    private IndexDocumentBuilder(
        Document doc,
        TranslogWriter translogWriter,
        ValueIndexer.Synthetics synthetics,
        Map<ColumnIdent, Indexer.ColumnConstraint> constraints,
        Version tableVersionCreated
    ) {
        this.doc = doc;
        this.translogWriter = translogWriter;
        this.synthetics = synthetics;
        this.constraints = constraints;
        this.tableVersionCreated = tableVersionCreated;
    }

    /**
     * Add a new lucene indexable field
     */
    public void addField(IndexableField field) {
        doc.add(field);
    }

    /**
     * @return the TranslogWriter
     */
    public TranslogWriter translogWriter() {
        return translogWriter;
    }

    /**
     * @return a generated value for the given column if one exists, otherwise null
     */
    public Object getSyntheticValue(ColumnIdent columnIdent) {
        Input<Object> input = synthetics.get(columnIdent);
        return input == null ? null : input.value();
    }

    /**
     * Checks column constraints are all met for a given column and value
     */
    public void checkColumnConstraint(ColumnIdent columnIdent, Object value) {
        Indexer.ColumnConstraint constraint = constraints.get(columnIdent);
        if (constraint != null) {
            constraint.verify(value);
        }
    }

    public boolean maybeAddStoredField() {
        return tableVersionCreated.onOrAfter(StoredRowLookup.PARTIAL_STORED_SOURCE_VERSION);
    }

    public IndexDocumentBuilder noStoredField() {
        return new IndexDocumentBuilder(doc, translogWriter, synthetics, constraints, tableVersionCreated) {
            @Override
            public boolean maybeAddStoredField() {
                return false;
            }
        };
    }

    /**
     * Constructs a new ParsedDocument with the given id from the indexed values
     */
    public ParsedDocument build(String id) {

        NumericDocValuesField version = new NumericDocValuesField(DocSysColumns.Names.VERSION, -1L);
        addField(version);

        BytesReference translog = translogWriter.bytes();
        BytesRef translogRef = translog.toBytesRef();
        if (tableVersionCreated.onOrAfter(StoredRowLookup.PARTIAL_STORED_SOURCE_VERSION)) {
            addField(new StoredField(DocSysColumns.Source.RECOVERY_NAME, translogRef.bytes, translogRef.offset, translogRef.length));
//            addField(new NumericDocValuesField(DocSysColumns.Source.RECOVERY_NAME, 1));
        } else {
            addField(new StoredField(DocSysColumns.Source.NAME, translogRef.bytes, translogRef.offset, translogRef.length));
        }

        BytesRef idBytes = Uid.encodeId(id);
        addField(new Field(DocSysColumns.Names.ID, idBytes, DocSysColumns.ID.FIELD_TYPE));

        SequenceIDFields seqID = SequenceIDFields.emptySeqID();
        // Actual values are set via ParsedDocument.updateSeqID
        addField(seqID.seqNo);
        addField(seqID.seqNoDocValue);
        addField(seqID.primaryTerm);

        return new ParsedDocument(version, seqID, id, doc, translog);
    }

    public Version getTableVersionCreated() {
        return tableVersionCreated;
    }
}

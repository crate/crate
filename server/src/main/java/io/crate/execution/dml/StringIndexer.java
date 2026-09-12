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

package io.crate.execution.dml;

import java.io.IOException;
import java.util.Locale;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.BytesRef;

import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.SysColumns;

public class StringIndexer implements ValueIndexer<String> {

    public static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.freeze();
    }

    private final Reference ref;

    public StringIndexer(Reference ref) {
        this.ref = ref;
    }

    @Override
    public void indexValue(String value, IndexDocumentBuilder docBuilder) throws IOException {
        String name = ref.storageIdent();
        BytesRef binaryValue = new BytesRef(value);
        if (binaryValue.length > IndexWriter.MAX_TERM_LENGTH
                && (ref.indexType() != IndexType.NONE || ref.hasDocValues())) {
            // Lucene enforces this limit on indexed terms and on sorted doc values, but it
            // only knows the storage identifier (oid) of the column. Raise the error upfront
            // to be able to include the column name instead of the oid.
            // See https://github.com/crate/crate/issues/17376
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Value for column \"%s\" is too large, must be <= %d bytes (got %d bytes). "
                    + "Use `INDEX OFF` and `STORAGE WITH (columnstore = false)` to store larger values",
                ref.column().sqlFqn(),
                IndexWriter.MAX_TERM_LENGTH,
                binaryValue.length));
        }
        if (ref.indexType() != IndexType.NONE) {
            Field field = new Field(name, binaryValue, FIELD_TYPE);
            docBuilder.addField(field);
            if (ref.hasDocValues() == false) {
                docBuilder.addField(new Field(
                    SysColumns.FieldNames.NAME,
                    name,
                    SysColumns.FieldNames.FIELD_TYPE));
            }
        }
        if (ref.hasDocValues()) {
            docBuilder.addField(new SortedSetDocValuesField(name, binaryValue));
        } else {
            if (docBuilder.maybeAddStoredField()) {
                docBuilder.addField(new StoredField(name, value));
            }
        }
        docBuilder.translogWriter().writeValue(value);
    }

    @Override
    public String storageIdentLeafName() {
        return ref.storageIdentLeafName();
    }
}

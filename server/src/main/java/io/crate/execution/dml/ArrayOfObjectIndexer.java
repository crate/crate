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

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StoredField;
import org.elasticsearch.Version;
import org.jetbrains.annotations.NotNull;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;

/**
 * Specialized indexer for an array of objects, which duplicates the translog entry
 * for this column and writes it into a stored field.  At read time, ColumnFieldVisitor
 * can parse this translog entry using a SourceParser that will handle any object fields
 * that have been dropped since the row was added. Dropped columns mean that using
 * a Streamer (like {@link ArrayIndexer} won't work, because there may be unrecognized
 * columns from previously dropped columns read back which would cause Exceptions.
 */
public class ArrayOfObjectIndexer<T> extends ArrayIndexer<T> {

    public ArrayOfObjectIndexer(ValueIndexer<T> innerIndexer, Function<ColumnIdent, Reference> getRef, Reference reference) {
        super(innerIndexer, getRef, reference);
    }

    @Override
    public void indexValue(@NotNull List<T> values, IndexDocumentBuilder docBuilder) throws IOException {
        docBuilder = docBuilder.wrapTranslog(w -> new SidecarTranslogWriter(w, reference.storageIdentLeafName()));
        docBuilder.translogWriter().startArray();
        var nestedDocBuilder = docBuilder.noStoredField();
        for (T value : values) {
            if (value == null) {
                docBuilder.translogWriter().writeNull();
            } else {
                innerIndexer.indexValue(value, nestedDocBuilder);
            }
        }
        if (docBuilder.getTableVersionCreated().onOrAfter(Version.V_5_9_0)) {
            // map '[]' to '_array_length_ = 0'
            // map '[null]' to '_array_length_ = 1'
            // 'null' is not mapped; can utilize 'FieldExistsQuery' for 'IS NULL' filtering
            docBuilder.addField(new IntField(arrayLengthFieldName, values.size(), Field.Store.NO));
        }
        docBuilder.translogWriter().endArray();
        if (docBuilder.maybeAddStoredField()) {
            // we use a prefix here so that there is no confusion between StoredField and IntField, as using
            // both can result in inconsistent docvalues types across documents.
            var storedField = ARRAY_VALUES_FIELD_PREFIX + reference.storageIdent();
            var arrayBytes = docBuilder.translogWriter().bytes().toBytesRef();
            docBuilder.addField(new StoredField(storedField, arrayBytes));
        }
    }

}

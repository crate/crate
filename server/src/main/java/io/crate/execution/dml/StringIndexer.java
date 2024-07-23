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
import java.util.Map;
import java.util.function.Consumer;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.NotNull;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;

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
    public String indexValue(@NotNull String value,
                             Consumer<? super IndexableField> addField,
                             Synthetics synthetics,
                             Map<ColumnIdent, Indexer.ColumnConstraint> toValidate) throws IOException {
        String name = ref.storageIdent();
        BytesRef binaryValue = new BytesRef(value);
        if (ref.indexType() != IndexType.NONE) {
            Field field = new Field(name, binaryValue, FIELD_TYPE);
            addField.accept(field);
            if (ref.hasDocValues() == false) {
                addField.accept(new Field(
                    DocSysColumns.FieldNames.NAME,
                    name,
                    DocSysColumns.FieldNames.FIELD_TYPE));
            }
        }
        if (ref.hasDocValues()) {
            addField.accept(new SortedSetDocValuesField(name, binaryValue));
        }
        return value;
    }
}

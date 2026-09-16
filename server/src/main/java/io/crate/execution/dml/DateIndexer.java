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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;

import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.SysColumns;
import io.crate.types.DateType;

public class DateIndexer implements ValueIndexer<Long> {

    private final Reference ref;
    private final String name;

    public DateIndexer(Reference ref) {
        this.ref = ref;
        this.name = ref.storageIdent();
    }

    @Override
    public void indexValue(Long value, IndexDocumentBuilder docBuilder) throws IOException {
        int epochDay = DateType.toEpochDay(value);
        if (ref.hasDocValues() && ref.indexType() != IndexType.NONE) {
            docBuilder.addField(new IntField(name, epochDay, Field.Store.NO));
        } else {
            if (ref.indexType() != IndexType.NONE) {
                docBuilder.addField(new IntPoint(name, epochDay));
            }
            if (ref.hasDocValues()) {
                docBuilder.addField(new SortedNumericDocValuesField(name, epochDay));
            } else {
                if (docBuilder.maybeAddStoredField()) {
                    docBuilder.addField(new StoredField(name, epochDay));
                }
                docBuilder.addField(new Field(
                    SysColumns.FieldNames.NAME,
                    name,
                    SysColumns.FieldNames.FIELD_TYPE));
            }
        }
        docBuilder.translogWriter().writeValue(epochDay);
    }

    @Override
    public String storageIdentLeafName() {
        return name;
    }
}

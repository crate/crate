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

import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.util.NumericUtils;
import org.jetbrains.annotations.NotNull;

import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.SysColumns;

public class DoubleIndexer implements ValueIndexer<Number> {

    private final Reference ref;
    private final String name;

    public DoubleIndexer(Reference ref) {
        this.ref = ref;
        this.name = ref.storageIdent();
    }

    @Override
    public void indexValue(@NotNull Number value, IndexDocumentBuilder docBuilder) throws IOException {
        double doubleValue = value.doubleValue();
        if (ref.hasDocValues() && ref.indexType() != IndexType.NONE) {
            docBuilder.addField(new DoubleField(name, doubleValue, Field.Store.NO));
        } else {
            if (ref.indexType() != IndexType.NONE) {
                docBuilder.addField(new DoublePoint(name, doubleValue));
            }
            if (ref.hasDocValues()) {
                docBuilder.addField(
                    new SortedNumericDocValuesField(name, NumericUtils.doubleToSortableLong(doubleValue))
                );
            } else {
                if (docBuilder.maybeAddStoredField()) {
                    docBuilder.addField(new StoredField(name, doubleValue));
                }
                docBuilder.addField(new Field(
                    SysColumns.FieldNames.NAME,
                    name,
                    SysColumns.FieldNames.FIELD_TYPE));
            }
        }
        docBuilder.translogWriter().writeValue(value);
    }

    @Override
    public String storageIdentLeafName() {
        return ref.storageIdentLeafName();
    }
}

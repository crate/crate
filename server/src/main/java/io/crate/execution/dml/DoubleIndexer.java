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

import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;

import io.crate.execution.dml.Indexer.ColumnConstraint;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;

public class DoubleIndexer implements ValueIndexer<Number> {

    private final Reference ref;
    private final String name;

    public DoubleIndexer(Reference ref) {
        this.ref = ref;
        this.name = ref.storageIdent();
    }

    @Override
    public void indexValue(Number value,
                           XContentBuilder xcontentBuilder,
                           Consumer<? super IndexableField> addField,
                           Synthetics synthetics,
                           Map<ColumnIdent, ColumnConstraint> toValidate) throws IOException {
        xcontentBuilder.value(value);
        double doubleValue = value.doubleValue();
        if (ref.hasDocValues() && ref.indexType() != IndexType.NONE) {
            addField.accept(new DoubleField(name, doubleValue, Field.Store.NO));
        } else {
            if (ref.indexType() != IndexType.NONE) {
                addField.accept(new DoublePoint(name, doubleValue));
            }
            if (ref.hasDocValues()) {
                addField.accept(
                        new SortedNumericDocValuesField(name, NumericUtils.doubleToSortableLong(doubleValue))
                );
            } else {
                addField.accept(new Field(
                        DocSysColumns.FieldNames.NAME,
                        name,
                        DocSysColumns.FieldNames.FIELD_TYPE));
            }
        }
    }
}

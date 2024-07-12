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
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.jetbrains.annotations.Nullable;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;

public class ArrayIndexer<T> implements ValueIndexer<List<T>> {

    private final ValueIndexer<T> innerIndexer;
    private final String storageIdent;

    public ArrayIndexer(ValueIndexer<T> innerIndexer, String storageIdent) {
        this.innerIndexer = innerIndexer;
        this.storageIdent = storageIdent;
    }

    @Override
    public void indexValue(List<T> values,
                           XContentBuilder xContentBuilder,
                           Consumer<? super IndexableField> addField,
                           Synthetics synthetics,
                           Map<ColumnIdent, Indexer.ColumnConstraint> toValidate) throws IOException {
        xContentBuilder.startArray();
        if (values != null) {
            for (T value : values) {
                if (value == null) {
                    xContentBuilder.nullValue();
                } else {
                    innerIndexer.indexValue(
                        value,
                        xContentBuilder,
                        addField,
                        synthetics,
                        toValidate
                    );
                }
            }
            // map '[]' to '_array_size_ = 0'
            // map '[null]' to '_array_size_ = 1'
            addField.accept(new IntField("_array_size_" + storageIdent, values.size(), Field.Store.NO));
        } else {
            // map 'null' to '_array_size_ = -1'
            addField.accept(new IntField("_array_size_" + storageIdent, -1, Field.Store.NO));
        }
        xContentBuilder.endArray();
    }

    @Override
    public void collectSchemaUpdates(@Nullable List<T> values,
                                     Consumer<? super Reference> onDynamicColumn,
                                     Synthetics synthetics) throws IOException {
        if (values != null) {
            for (T value : values) {
                if (value != null) {
                    innerIndexer.collectSchemaUpdates(value, onDynamicColumn, synthetics);
                }
            }
        }
    }

    @Override
    public void updateTargets(Function<ColumnIdent, Reference> getRef) {
        innerIndexer.updateTargets(getRef);
    }
}

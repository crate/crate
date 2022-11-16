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
import java.util.function.Consumer;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.xcontent.XContentBuilder;

import io.crate.metadata.Reference;

public class IntIndexer implements ValueIndexer<Integer> {

    private final String name;
    private Reference ref;

    public IntIndexer(Reference ref) {
        this.ref = ref;
        this.name = ref.column().fqn();
    }

    @Override
    public void indexValue(Integer value,
                           XContentBuilder xContentBuilder,
                           Consumer<? super IndexableField> addField,
                           Consumer<? super Reference> onDynamicColumn) throws IOException {
        xContentBuilder.value(value);
        if (value == null) {
            return;
        }
        int intValue = value.intValue();
        if (ref.hasDocValues()) {
            addField.accept(new SortedNumericDocValuesField(name, intValue));
        }
        // TODO: only if indexed
        addField.accept(new IntPoint(name, intValue));
        // TODO: StoredField
    }
}

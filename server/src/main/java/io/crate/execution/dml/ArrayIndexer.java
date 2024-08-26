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
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.table.TableInfo;
import io.crate.types.DataType;

public class ArrayIndexer<T> implements ValueIndexer<List<T>> {

    public static Query arrayLengthTermQuery(Reference arrayRef, int length, Function<ColumnIdent, Reference> getRef) {
        return IntPoint.newExactQuery(toArrayLengthFieldName(arrayRef, getRef), length);
    }

    public static Query arrayLengthRangeQuery(Reference arrayRef, int includeLower, int includeUpper, Function<ColumnIdent, Reference> getRef) {
        return IntPoint.newRangeQuery(toArrayLengthFieldName(arrayRef, getRef), includeLower, includeUpper);
    }

    static String toArrayLengthFieldName(Reference arrayRef, Function<ColumnIdent, Reference> getRef) {
        // If the arrayRef is a descendant of an object array its type can be a readType. i.e. the type of
        // obj_array['int_col'] is 'int' BUT its readType is 'array(int)'. If so, there is no '_array_length_' indexed
        // for obj_array['int_col']. Imagine indexing obj_array = [{int_col = 1}, {int_col = 2}], '_array_length_obj_array'
        // will be indexed but NOT '_array_length_int_col' since its type is an int.
        // However, queries like array_length(obj_array['int_col'], 1) are valid since the readType of obj_array['int_col']
        // is an array(int) - [1, 2]. In such cases we can simply use the fact that every element of 'obj_array' there
        // exists its sub-column 'int_col' such that array_length(obj_array, 1) = array_length(obj_array['int_col'], 1)
        DataType<?> valueType = Optional
            .ofNullable(getRef.apply(arrayRef.column())) // null if the arrayRef is dynamically added
            .orElse(arrayRef).valueType();

        // if the arrayRef is a ReadReference
        if (!arrayRef.valueType().equals(valueType)) {
            ColumnIdent topMostObjectArray = null;
            for (var columnIdent = arrayRef.column(); columnIdent != null; columnIdent = columnIdent.getParent()) {
                if (TableInfo.IS_OBJECT_ARRAY.test(getRef.apply(columnIdent).valueType())) {
                    topMostObjectArray = columnIdent;
                }
            }
            assert topMostObjectArray != null : "When the arrayRef is a ReadReference it must be a child of an object array type";
            return ARRAY_LENGTH_FIELD_PREFIX + getRef.apply(topMostObjectArray).storageIdentLeafName();
        }
        return ARRAY_LENGTH_FIELD_PREFIX + arrayRef.storageIdentLeafName();
    }

    @VisibleForTesting
    static final String ARRAY_LENGTH_FIELD_PREFIX = "_array_length_";

    private final ValueIndexer<T> innerIndexer;
    private final String arrayLengthFieldName;

    public ArrayIndexer(ValueIndexer<T> innerIndexer, Function<ColumnIdent, Reference> getRef, Reference reference) {
        this.innerIndexer = innerIndexer;
        this.arrayLengthFieldName = toArrayLengthFieldName(reference, getRef);
    }

    @Override
    public void indexValue(@NotNull List<T> values, IndexDocumentBuilder docBuilder) throws IOException {
        docBuilder.translogWriter().startArray();
        for (T value : values) {
            if (value == null) {
                docBuilder.translogWriter().writeNull();
            } else {
                innerIndexer.indexValue(value, docBuilder);
            }
        }
        if (docBuilder.getTableVersionCreated().onOrAfter(Version.V_5_9_0)) {
            // map '[]' to '_array_length_ = 0'
            // map '[null]' to '_array_length_ = 1'
            // 'null' is not mapped; can utilize 'FieldExistsQuery' for 'IS NULL' filtering
            docBuilder.addField(new IntField(arrayLengthFieldName, values.size(), Field.Store.NO));
        }
        docBuilder.translogWriter().endArray();
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

    @Override
    public String storageIdentLeafName() {
        return innerIndexer.storageIdentLeafName();
    }
}

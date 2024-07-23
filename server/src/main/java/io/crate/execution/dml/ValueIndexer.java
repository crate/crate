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
import java.util.function.Function;

import org.apache.lucene.index.IndexableField;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.execution.dml.Indexer.ColumnConstraint;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;

/**
 * Component used to index values. An implementation must:
 * <ul>
 * <li>Add the value to the source using the provided `xcontentBuilder`</li>
 * <li>Create index fields for the value and call `addField`</li>
 * <li>Call `onDynamicColumn` for dynamically created columns</li>
 * </ul>
 *
 * <p>
 * `synthetics` and `toValidate` is for object types, where the indexer
 * implementation must use them to generate inner columns (DEFAULT, GENERATED
 * AS) and validate constraints. Primitive value indexers can ignore those.
 * </p>
 **/
public interface ValueIndexer<T> {

    /**
     * Represents values for columns generated from other input columns
     */
    interface Synthetics {

        /**
         * @return an Input object that returns generated values for a given column
         */
        Input<Object> get(ColumnIdent columnIdent);
    }

    /**
     * Only {@link ObjectIndexer}, {@link ArrayIndexer} and {@link DynamicIndexer} can create new columns.
     */
    default void collectSchemaUpdates(@Nullable T value,
                                      Consumer<? super Reference> onDynamicColumn,
                                      Synthetics synthetics) throws IOException {}

    /**
     * Update value indexer of inner columns.
     * Should be only triggered when new columns were detected by {@link #collectSchemaUpdates(Object, Consumer, Synthetics)
     * and added to the cluster state
     *
     * @param getRef A function that returns a reference for a given column ident based on the current cluster state
     */
    default void updateTargets(Function<ColumnIdent, Reference> getRef) {}

    /**
     * Writes a value into an indexable document, returning the value that should be written to the translog
     */
    T indexValue(
        @NotNull T value,
        Consumer<? super IndexableField> addField,
        Synthetics synthetics,
        Map<ColumnIdent, ColumnConstraint> toValidate
    ) throws IOException;

}

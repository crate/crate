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

package io.crate.types;


import java.util.function.Function;

import javax.annotation.Nullable;

import org.apache.lucene.document.FieldType;

import io.crate.execution.dml.ValueIndexer;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

public abstract class StorageSupport<T> {

    private final boolean docValuesDefault;
    private final boolean hasFieldNamesIndex;

    @Nullable
    private final EqQuery<T> eqQuery;

    public StorageSupport(boolean docValuesDefault,
                          boolean hasFieldNamesIndex,
                          EqQuery<T> eqQuery) {
        this.docValuesDefault = docValuesDefault;
        this.hasFieldNamesIndex = hasFieldNamesIndex;
        this.eqQuery = eqQuery;
    }

    public boolean getComputedDocValuesDefault(@Nullable IndexType indexType) {
        return docValuesDefault && indexType != IndexType.FULLTEXT;
    }


    public abstract ValueIndexer<T> valueIndexer(
        RelationName table,
        Reference ref,
        Function<ColumnIdent, FieldType> getFieldType,
        Function<ColumnIdent, Reference> getRef);


    public boolean docValuesDefault() {
        return docValuesDefault;
    }

    public boolean hasFieldNamesIndex() {
        return hasFieldNamesIndex;
    }

    public EqQuery<T> eqQuery() {
        return eqQuery;
    }
}

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

import org.elasticsearch.common.bytes.BytesReference;
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

    ArrayOfObjectIndexer(ValueIndexer<T> innerIndexer, Function<ColumnIdent, Reference> getRef, Reference reference) {
        super(innerIndexer, getRef, reference);
    }

    @Override
    public void indexValue(@NotNull List<T> values, IndexDocumentBuilder docBuilder) throws IOException {
        docBuilder = docBuilder.wrapTranslog(w -> new SidecarTranslogWriter(w, reference.storageIdentLeafName()));
        super.indexValue(values, docBuilder);
    }

    @Override
    protected BytesReference arrayToBytes(List<T> values, IndexDocumentBuilder docBuilder) {
        return docBuilder.translogWriter().bytes();
    }
}

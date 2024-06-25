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

package io.crate.expression.symbol;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.expression.symbol.format.Style;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.types.DataType;

/**
 * A FetchMarker wraps a `_fetchid` and behaves mostly transparent (as if it were the _fetchId),
 * but it carries all the `fetchRefs` that can and will be fetched via the _fetchId.
 */
public final class FetchMarker implements Symbol {

    private final RelationName relationName;
    private final List<Reference> fetchRefs;
    private final Reference fetchId;

    public FetchMarker(RelationName relationName, List<Reference> fetchRefs) {
        this(relationName, fetchRefs, DocSysColumns.forTable(relationName, DocSysColumns.FETCHID));
    }

    public FetchMarker(RelationName relationName, List<Reference> fetchRefs, Reference fetchId) {
        this.relationName = relationName;
        this.fetchRefs = fetchRefs;
        this.fetchId = fetchId;
    }

    public List<Reference> fetchRefs() {
        return fetchRefs;
    }

    public RelationName relationName() {
        return relationName;
    }

    public Reference fetchId() {
        return fetchId;
    }

    @Override
    public boolean any(Predicate<? super Symbol> predicate) {
        return predicate.test(this) || fetchId.any(predicate);
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.REFERENCE;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitFetchMarker(this, context);
    }

    @Override
    public DataType<?> valueType() {
        return fetchId.valueType();
    }

    @Override
    public String toString() {
        return toString(Style.UNQUALIFIED);
    }

    @Override
    public String toString(Style style) {
        if (relationName.schema() == null) {
            return relationName.sqlFqn() + "." + fetchId.toString(style);
        }
        return fetchId.toString(style);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        fetchId.writeTo(out);
    }

    @Override
    public long ramBytesUsed() {
        return fetchId.ramBytesUsed()
            + RamUsageEstimator.sizeOf(relationName.schema())
            + RamUsageEstimator.sizeOf(relationName.name());
    }
}

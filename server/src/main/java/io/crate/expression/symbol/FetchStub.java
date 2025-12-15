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
import java.io.UnsupportedEncodingException;

import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.expression.symbol.format.Style;
import io.crate.metadata.ScopedRef;
import io.crate.types.DataType;

/**
 * A placeholder for a reference that the {@link io.crate.planner.operators.Fetch} operator can fetch.
 */
public final class FetchStub implements Symbol {

    private final FetchMarker fetchMarker;
    private final ScopedRef ref;

    /**
     * @param fetchMarker the _fetchId marker that must be used to fetch the value for the reference
     */
    public FetchStub(FetchMarker fetchMarker, ScopedRef ref) {
        this.fetchMarker = fetchMarker;
        this.ref = ref;
    }

    public FetchMarker fetchMarker() {
        return fetchMarker;
    }

    public ScopedRef ref() {
        return ref;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.FETCH_STUB;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitFetchStub(this, context);
    }

    @Override
    public DataType<?> valueType() {
        return ref.valueType();
    }

    @Override
    public String toString() {
        return toString(Style.UNQUALIFIED);
    }

    @Override
    public String toString(Style style) {
        return ref.toString(style);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedEncodingException(
            "Cannot stream FetchStub. This is a planning symbol and not suitable for the execution layer");
    }

    @Override
    public long ramBytesUsed() {
        return ref.ramBytesUsed() + fetchMarker.ramBytesUsed();
    }
}

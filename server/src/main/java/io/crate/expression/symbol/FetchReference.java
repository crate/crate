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
import java.util.function.Predicate;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.expression.symbol.format.Style;
import io.crate.metadata.Reference;
import io.crate.types.DataType;

public class FetchReference implements Symbol {

    private final InputColumn fetchId;
    private final Reference ref;

    public FetchReference(InputColumn fetchId, Reference ref) {
        this.fetchId = fetchId;
        this.ref = ref;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.FETCH_REFERENCE;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitFetchReference(this, context);
    }

    @Override
    public DataType<?> valueType() {
        return ref.valueType();
    }

    public InputColumn fetchId() {
        return fetchId;
    }

    public Reference ref() {
        return ref;
    }

    @Override
    public boolean isDeterministic() {
        return fetchId.isDeterministic() && ref.isDeterministic();
    }

    @Override
    public boolean any(Predicate<? super Symbol> predicate) {
        return predicate.test(this) || fetchId.any(predicate) || ref.any(predicate);
    }

    public FetchReference(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("FetchReference cannot be streamed");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("FetchReference cannot be streamed");
    }

    @Override
    public String toString() {
        return toString(Style.UNQUALIFIED);
    }

    @Override
    public String toString(Style style) {
        return "FETCH("
            + fetchId.toString(style)
            + ", "
            + ref.toString(style)
            + ')';
    }

    @Override
    public long ramBytesUsed() {
        return fetchId.ramBytesUsed() + ref.ramBytesUsed();
    }
}

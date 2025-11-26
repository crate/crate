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

package io.crate.metadata;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.format.Style;
import io.crate.types.DataType;

public class ScopedRef implements Symbol {

    private final RelationName relation;
    private final Reference ref;

    public ScopedRef(RelationName relation, Reference ref) {
        this.relation = relation;
        this.ref = ref;
    }

    public ScopedRef(StreamInput in) throws IOException {
        this.relation = new RelationName(in);
        this.ref = Reference.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        relation.writeTo(out);
        Reference.toStream(out, ref);
    }

    public Reference ref() {
        return ref;
    }

    public RelationName relation() {
        return relation;
    }

    @Override
    public long ramBytesUsed() {
        return ref.ramBytesUsed() + relation.ramBytesUsed();
    }

    @Override
    public SymbolType symbolType() {
        return ref.symbolType();
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitScopedRef(this, context);
    }

    @Override
    public DataType<?> valueType() {
        return ref.valueType();
    }

    @Override
    public String toString(Style style) {
        if (style == Style.QUALIFIED) {
            return relation.sqlFqn() + '.' + ref().column().quotedOutputName();
        }
        return ref.toString(style);
    }
}

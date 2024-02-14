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
import java.util.List;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;

import io.crate.metadata.GeneratedReference;
import io.crate.metadata.GeoReference;
import io.crate.metadata.IndexReference;
import io.crate.metadata.SimpleReference;

public enum SymbolType {

    AGGREGATION(Aggregation::new),
    REFERENCE(SimpleReference::new),
    RELATION_OUTPUT(in -> {
        throw new UnsupportedOperationException("Field is not streamable");
    }),
    FUNCTION(Function::new),
    WINDOW_FUNCTION(WindowFunction::new),
    LITERAL(Literal::new),
    INPUT_COLUMN(InputColumn::new),
    DYNAMIC_REFERENCE(DynamicReference::new),
    MATCH_PREDICATE(null),
    FETCH_REFERENCE(null),
    INDEX_REFERENCE(IndexReference::new),
    GEO_REFERENCE(GeoReference::new),
    GENERATED_REFERENCE(GeneratedReference::new),
    PARAMETER(ParameterSymbol::new),
    SELECT_SYMBOL(in -> {
        throw new UnsupportedOperationException("SelectSymbol is not streamable");
    }),
    // Added in 4.2
    ALIAS(AliasSymbol::new),
    FETCH_STUB(in -> {
        throw new UnsupportedEncodingException("FetchStub is not streamable");
    }),
    VOID_REFERENCE(VoidReference::new);

    public static final List<SymbolType> VALUES = List.of(values());

    private final Writeable.Reader<Symbol> reader;

    SymbolType(Writeable.Reader<Symbol> reader) {
        this.reader = reader;
    }

    public Symbol newInstance(StreamInput in) throws IOException {
        return reader.read(in);
    }

    public boolean isValueSymbol() {
        return ordinal() == LITERAL.ordinal();
    }

    public boolean isValueOrParameterSymbol() {
        return ordinal() == LITERAL.ordinal() || ordinal() == PARAMETER.ordinal();
    }
}

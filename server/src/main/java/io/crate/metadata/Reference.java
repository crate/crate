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

import javax.annotation.Nullable;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.sql.tree.ColumnPolicy;

public interface Reference extends Symbol {

    ReferenceIdent ident();

    ColumnIdent column();

    IndexType indexType();

    ColumnPolicy columnPolicy();

    boolean isNullable();

    RowGranularity granularity();

    int position();

    boolean hasDocValues();

    @Nullable
    Symbol defaultExpression();

    Reference getRelocated(ReferenceIdent referenceIdent);

    static void toStream(Reference ref, StreamOutput out) throws IOException {
        out.writeVInt(ref.symbolType().ordinal());
        ref.writeTo(out);
    }

    @SuppressWarnings("unchecked")
    static <T extends Reference> T fromStream(StreamInput in) throws IOException {
        return (T) SymbolType.VALUES.get(in.readVInt()).newInstance(in);
    }
}

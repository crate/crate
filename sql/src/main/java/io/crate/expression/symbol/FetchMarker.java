/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.symbol;

import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class FetchMarker extends Symbol {

    private final QualifiedName qName;
    private final Reference fetchId;
    private final ArrayList<Symbol> columnsToFetch;
    private final DocTableInfo table;

    public FetchMarker(DocTableInfo table, QualifiedName qName, Reference fetchId, ArrayList<Symbol> columnsToFetch) {
        this.table = table;
        this.qName = qName;
        this.fetchId = fetchId;
        this.columnsToFetch = columnsToFetch;
    }

    @Override
    public SymbolType symbolType() {
        return null;
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
    public String representation() {
        return fetchId.representation();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Symbols.toStream(fetchId, out);
    }

    public Reference fetchId() {
        return fetchId;
    }

    public List<Symbol> columnsToFetch() {
        return columnsToFetch;
    }

    public QualifiedName qualifiedName() {
        return qName;
    }

    public DocTableInfo table() {
        return table;
    }
}

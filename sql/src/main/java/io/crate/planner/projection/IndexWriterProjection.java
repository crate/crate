/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner.projection;

import com.google.common.collect.ImmutableList;
import io.crate.DataType;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.Value;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IndexWriterProjection extends Projection {

    private final static List<Symbol> OUTPUTS = ImmutableList.<Symbol>of(
            new Value(DataType.LONG)  // number of rows imported
    );

    private String tableName;
    private List<Symbol> idSymbols;
    private List<String> primaryKeys;
    private Symbol rawSourceSymbol;
    private Symbol clusteredBySymbol;

    public static final ProjectionFactory<IndexWriterProjection> FACTORY =
            new ProjectionFactory<IndexWriterProjection>() {
        @Override
        public IndexWriterProjection newInstance() {
            return new IndexWriterProjection();
        }
    };

    public IndexWriterProjection() {}
    public IndexWriterProjection(String tableName, List<String> primaryKeys) {
        this.tableName = tableName;
        this.idSymbols = new ArrayList<>(primaryKeys.size());
        for (int i = 0; i < primaryKeys.size(); i++) {
            idSymbols.add(new InputColumn(i));
        }
        clusteredBySymbol = new InputColumn(primaryKeys.size());
        rawSourceSymbol = new InputColumn(primaryKeys.size() + 1);
        this.primaryKeys = primaryKeys;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.INDEX_WRITER;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitIndexWriterProjection(this, context);
    }

    @Override
    public List<? extends Symbol> outputs() {
        return OUTPUTS;
    }

    // TODO: make defaults settable
    //  - currently defaults that are also used by the inout importers are in use::
    public Integer bulkActions() {
        return 10000;
    }
    public Integer concurrency() {
        return 4;
    }

    public List<String> primaryKeys() {
        return primaryKeys;
    }

    public List<Symbol> ids() {
        return idSymbols;
    }

    public Symbol clusteredBy() {
        return clusteredBySymbol;
    }

    public Symbol rawSource() {
        return rawSourceSymbol;
    }

    public String tableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        tableName = in.readString();
        int numIdSymbols = in.readVInt();
        idSymbols = new ArrayList<>(numIdSymbols);
        for (int i = 0; i < numIdSymbols; i++) {
            idSymbols.add(Symbol.fromStream(in));
        }

        int numPks = in.readVInt();
        primaryKeys = new ArrayList<>(numPks);
        for (int i = 0; i < numPks; i++) {
            primaryKeys.add(in.readString());
        }

        clusteredBySymbol = Symbol.fromStream(in);
        rawSourceSymbol = Symbol.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(tableName);

        out.writeVInt(idSymbols.size());
        for (Symbol idSymbol : idSymbols) {
            Symbol.toStream(idSymbol, out);
        }
        out.writeVInt(primaryKeys.size());
        for (String primaryKey : primaryKeys) {
            out.writeString(primaryKey);
        }
        Symbol.toStream(clusteredBySymbol, out);
        Symbol.toStream(rawSourceSymbol, out);
    }
}

/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner.node;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ESIndexNode extends PlanNode {

    private String index;

    private List<Reference> columns;
    private List<List<Symbol>> valuesLists;
    private int[] primaryKeyIndices;

    public ESIndexNode(String index,
                       List<Reference> columns,
                       List<List<Symbol>> valuesLists,
                       @Nullable int[] primaryKeyIndices) {
        Preconditions.checkNotNull(index, "index is null");
        this.index = index;
        this.columns = Objects.firstNonNull(columns, ImmutableList.<Reference>of());
        this.valuesLists = Objects.firstNonNull(valuesLists, ImmutableList.<List<Symbol>>of());
        this.primaryKeyIndices = Objects.firstNonNull(primaryKeyIndices, new int[0]);
    }

    public String index() {
        return index;
    }

    public List<Reference> columns() {
        return columns;
    }

    public List<List<Symbol>> valuesLists() {
        return valuesLists;
    }

    @Override
    public Set<String> executionNodes() {
        return null;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitESIndexNode(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readString();

        int numColumns = in.readVInt();
        columns = new ArrayList<>(numColumns);
        for (int i=0; i<numColumns;i++) {
            Reference reference = new Reference();
            reference.readFrom(in);
            columns.add(reference);
        }

        int numValuesLists = in.readVInt();
        valuesLists = new ArrayList<>(numValuesLists);
        for (int i=0; i<numValuesLists;i++) {
            int numValues = in.readVInt();
            List<Symbol> values = new ArrayList<>(numValues);
            for (int j=0; j<numValues;j++) {
                Symbol symbol = Symbol.fromStream(in);
                values.add(symbol);
            }
            valuesLists.add(values);
        }
        int numIndices = in.readVInt();

        primaryKeyIndices = new int[numIndices];
        for (int i = 0; i< numIndices; i++) {
            primaryKeyIndices[i] = in.readVInt();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);

        out.writeVInt(columns.size());
        for (Reference reference : columns) {
            reference.writeTo(out);
        }

        out.writeVInt(valuesLists.size());
        for (List<Symbol> values : valuesLists) {
            out.writeVInt(values.size());
            for (Symbol symbol : values) {
                Symbol.toStream(symbol, out);
            }
        }
        out.writeVInt(primaryKeyIndices.length);
        if (primaryKeyIndices.length > 0) {
            for (int i : primaryKeyIndices) {
                out.writeVInt(i);
            }
        }
    }

    public int[] primaryKeyIndices() {
        return primaryKeyIndices;
    }

    public boolean hasPrimaryKey() {
        return primaryKeyIndices.length > 0;
    }
}

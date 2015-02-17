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

import com.carrotsearch.hppc.IntSet;
import com.google.common.collect.Lists;
import io.crate.metadata.ColumnIdent;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnIndexWriterProjection extends AbstractIndexWriterProjection {

    private List<Symbol> columnSymbols;
    private List<Reference> columnReferences;
    @Nullable
    private Map<Reference, Symbol> onDuplicateKeyAssignments;

    public static final ProjectionFactory<ColumnIndexWriterProjection> FACTORY =
            new ProjectionFactory<ColumnIndexWriterProjection>() {
                @Override
                public ColumnIndexWriterProjection newInstance() {
                    return new ColumnIndexWriterProjection();
                }
            };

    protected ColumnIndexWriterProjection() {}

    /**
     *
     * @param tableName
     * @param primaryKeys
     * @param columns the columnReferences of all the columns to be written in order of appearance
     * @param onDuplicateKeyAssignments reference to symbol map used for update on duplicate key
     * @param primaryKeyIndices
     * @param partitionedByIndices
     * @param clusteredByIndex
     * @param settings
     */
    public ColumnIndexWriterProjection(String tableName,
                                       List<ColumnIdent> primaryKeys,
                                       List<Reference>  columns,
                                       @Nullable
                                       Map<Reference, Symbol> onDuplicateKeyAssignments,
                                       IntSet primaryKeyIndices,
                                       IntSet partitionedByIndices,
                                       @Nullable ColumnIdent clusteredByColumn,
                                       int clusteredByIndex,
                                       Settings settings,
                                       boolean autoCreateIndices) {
        super(tableName, primaryKeys, clusteredByColumn, settings, autoCreateIndices);
        generateSymbols(primaryKeyIndices.toArray(), partitionedByIndices.toArray(), clusteredByIndex);

        this.onDuplicateKeyAssignments = onDuplicateKeyAssignments;
        this.columnReferences = Lists.newArrayList(columns);
        this.columnSymbols = new ArrayList<>(columns.size()-partitionedByIndices.size());

        for (int i = 0; i < columns.size(); i++) {
            if (!partitionedByIndices.contains(i)) {
                this.columnSymbols.add(new InputColumn(i, columns.get(i).valueType()));
            } else {
                columnReferences.remove(i);
            }
        }

    }

    public List<Symbol> columnSymbols() {
        return columnSymbols;
    }

    public List<Reference> columnReferences() {
        return columnReferences;
    }

    public Map<Reference, Symbol> onDuplicateKeyAssignments() {
        return onDuplicateKeyAssignments;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitColumnIndexWriterProjection(this, context);
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.COLUMN_INDEX_WRITER;
    }

    @SuppressWarnings("SimplifiableIfStatement")
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ColumnIndexWriterProjection that = (ColumnIndexWriterProjection) o;

        if (!columnReferences.equals(that.columnReferences)) return false;
        if (!columnSymbols.equals(that.columnSymbols)) return false;
        return !(onDuplicateKeyAssignments != null ?
                !onDuplicateKeyAssignments.equals(that.onDuplicateKeyAssignments)
                : that.onDuplicateKeyAssignments != null);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + columnSymbols.hashCode();
        result = 31 * result + columnReferences.hashCode();
        result = 31 * result + (onDuplicateKeyAssignments != null ? onDuplicateKeyAssignments.hashCode() : 0);
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        if (in.readBoolean()) {
            int length = in.readVInt();
            columnSymbols = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                columnSymbols.add(Symbol.fromStream(in));
            }
        }
        if (in.readBoolean()) {
            int length = in.readVInt();
            columnReferences = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                columnReferences.add(Reference.fromStream(in));
            }
        }

        if (in.readBoolean()) {
            int mapSize = in.readVInt();
            onDuplicateKeyAssignments = new HashMap<>(mapSize);
            for (int i = 0; i < mapSize; i++) {
                onDuplicateKeyAssignments.put(Reference.fromStream(in), Symbol.fromStream(in));
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        if (columnSymbols == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(columnSymbols.size());
            for (Symbol columnSymbol : columnSymbols) {
                Symbol.toStream(columnSymbol, out);
            }
        }
        if (columnReferences == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(columnReferences.size());
            for (Reference columnIdent : columnReferences) {
                columnIdent.writeTo(out);
            }
        }

        if (onDuplicateKeyAssignments == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(onDuplicateKeyAssignments.size());
            for (Map.Entry<Reference, Symbol> entry : onDuplicateKeyAssignments.entrySet()) {
                Reference.toStream(entry.getKey(), out);
                Symbol.toStream(entry.getValue(), out);
            }
        }

    }

}

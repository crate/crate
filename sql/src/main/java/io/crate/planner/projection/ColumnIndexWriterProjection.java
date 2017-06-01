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

import com.google.common.base.Function;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.collections.Lists2;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.TableIdent;
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

    /**
     * @param tableIdent                identifying the table to write to
     * @param columns                   the columnReferences of all the columns to be written in order of appearance
     * @param onDuplicateKeyAssignments reference to symbol map used for update on duplicate key
     */
    public ColumnIndexWriterProjection(TableIdent tableIdent,
                                       @Nullable String partitionIdent,
                                       List<ColumnIdent> primaryKeys,
                                       List<Reference> columns,
                                       @Nullable Map<Reference, Symbol> onDuplicateKeyAssignments,
                                       List<Symbol> primaryKeySymbols,
                                       List<ColumnIdent> partitionedByColumns,
                                       List<Symbol> partitionedBySymbols,
                                       @Nullable ColumnIdent clusteredByColumn,
                                       @Nullable Symbol clusteredBySymbol,
                                       Settings settings,
                                       boolean autoCreateIndices) {
        super(tableIdent, partitionIdent, primaryKeys, clusteredByColumn, settings, primaryKeySymbols, autoCreateIndices);
        this.partitionedBySymbols = partitionedBySymbols;
        this.onDuplicateKeyAssignments = onDuplicateKeyAssignments;
        this.columnReferences = new ArrayList<>(columns);
        this.columnSymbols = new ArrayList<>(columns.size() - partitionedBySymbols.size());
        this.clusteredBySymbol = clusteredBySymbol;

        for (int i = 0; i < columns.size(); i++) {
            Reference ref = columns.get(i);
            if (partitionedByColumns.contains(ref.ident().columnIdent())) {
                columnReferences.remove(i);
            } else {
                this.columnSymbols.add(new InputColumn(i, ref.valueType()));
            }
        }
    }

    ColumnIndexWriterProjection(StreamInput in) throws IOException {
        super(in);

        if (in.readBoolean()) {
            columnSymbols = Symbols.listFromStream(in);
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
                onDuplicateKeyAssignments.put(Reference.fromStream(in), Symbols.fromStream(in));
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
    public void replaceSymbols(Function<Symbol, Symbol> replaceFunction) {
        Lists2.replaceItems(columnSymbols, replaceFunction);
        if (onDuplicateKeyAssignments != null && !onDuplicateKeyAssignments.isEmpty()) {
            for (Map.Entry<Reference, Symbol> entry : onDuplicateKeyAssignments.entrySet()) {
                entry.setValue(replaceFunction.apply(entry.getValue()));
            }
        }
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
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        if (columnSymbols == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Symbols.toStream(columnSymbols, out);
        }
        if (columnReferences == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(columnReferences.size());
            for (Reference columnIdent : columnReferences) {
                Reference.toStream(columnIdent, out);
            }
        }

        if (onDuplicateKeyAssignments == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(onDuplicateKeyAssignments.size());
            for (Map.Entry<Reference, Symbol> entry : onDuplicateKeyAssignments.entrySet()) {
                Reference.toStream(entry.getKey(), out);
                Symbols.toStream(entry.getValue(), out);
            }
        }

    }
}

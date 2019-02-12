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

package io.crate.execution.dsl.projection;

import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnIndexWriterProjection extends AbstractIndexWriterProjection {

    private final List<Symbol> columnSymbols;
    private final List<Reference> targetColsExclPartitionCols;
    private final boolean ignoreDuplicateKeys;
    @Nullable
    private final Map<Reference, Symbol> onDuplicateKeyAssignments;

    /**
     * @param relationName                identifying the table to write to
     * @param columns                   the columnReferences of all the columns to be written in order of appearance
     * @param onDuplicateKeyAssignments reference to symbol map used for update on duplicate key
     */
    public ColumnIndexWriterProjection(RelationName relationName,
                                       @Nullable String partitionIdent,
                                       List<ColumnIdent> primaryKeys,
                                       List<Reference> columns,
                                       boolean ignoreDuplicateKeys,
                                       @Nullable Map<Reference, Symbol> onDuplicateKeyAssignments,
                                       List<Symbol> primaryKeySymbols,
                                       List<ColumnIdent> partitionedByColumns,
                                       List<Symbol> partitionedBySymbols,
                                       @Nullable ColumnIdent clusteredByColumn,
                                       @Nullable Symbol clusteredBySymbol,
                                       Settings settings,
                                       boolean autoCreateIndices) {
        super(relationName, partitionIdent, primaryKeys, clusteredByColumn, settings, primaryKeySymbols, autoCreateIndices);
        assert partitionedBySymbols.stream().noneMatch(s -> SymbolVisitors.any(Symbols.IS_COLUMN, s))
            : "All references and fields in partitionedBySymbols must be resolved to inputColumns, got: " + partitionedBySymbols;
        this.partitionedBySymbols = partitionedBySymbols;
        this.ignoreDuplicateKeys = ignoreDuplicateKeys;
        this.onDuplicateKeyAssignments = onDuplicateKeyAssignments;
        this.targetColsExclPartitionCols = new ArrayList<>(columns.size() - partitionedByColumns.size());
        for (Reference column : columns) {
            if (partitionedByColumns.contains(column.column())) {
                continue;
            }
            targetColsExclPartitionCols.add(column);
        }
        this.clusteredBySymbol = clusteredBySymbol;
        columnSymbols = InputColumns.create(targetColsExclPartitionCols, new InputColumns.SourceSymbols(columns));
    }

    ColumnIndexWriterProjection(StreamInput in) throws IOException {
        super(in);

        if (in.readBoolean()) {
            columnSymbols = Symbols.listFromStream(in);
        } else {
            columnSymbols = Collections.emptyList();
        }
        if (in.readBoolean()) {
            int length = in.readVInt();
            targetColsExclPartitionCols = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                targetColsExclPartitionCols.add(Reference.fromStream(in));
            }
        } else {
            targetColsExclPartitionCols = Collections.emptyList();
        }

        ignoreDuplicateKeys = in.readBoolean();
        if (in.readBoolean()) {
            int mapSize = in.readVInt();
            onDuplicateKeyAssignments = new HashMap<>(mapSize);
            for (int i = 0; i < mapSize; i++) {
                onDuplicateKeyAssignments.put(Reference.fromStream(in), Symbols.fromStream(in));
            }
        } else {
            onDuplicateKeyAssignments = Collections.emptyMap();
        }
    }

    public List<Symbol> columnSymbols() {
        return columnSymbols;
    }

    public List<Reference> columnReferences() {
        return targetColsExclPartitionCols;
    }

    public boolean isIgnoreDuplicateKeys() {
        return ignoreDuplicateKeys;
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

        if (!targetColsExclPartitionCols.equals(that.targetColsExclPartitionCols)) return false;
        if (!columnSymbols.equals(that.columnSymbols)) return false;
        return !(onDuplicateKeyAssignments != null ?
                     !onDuplicateKeyAssignments.equals(that.onDuplicateKeyAssignments)
                     : that.onDuplicateKeyAssignments != null);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + columnSymbols.hashCode();
        result = 31 * result + targetColsExclPartitionCols.hashCode();
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
        if (targetColsExclPartitionCols == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(targetColsExclPartitionCols.size());
            for (Reference columnIdent : targetColsExclPartitionCols) {
                Reference.toStream(columnIdent, out);
            }
        }

        out.writeBoolean(ignoreDuplicateKeys);
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

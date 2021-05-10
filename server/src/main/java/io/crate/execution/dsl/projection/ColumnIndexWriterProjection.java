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

package io.crate.execution.dsl.projection;

import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import org.elasticsearch.Version;
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
import java.util.Objects;
import java.util.function.Function;

public class ColumnIndexWriterProjection extends AbstractIndexWriterProjection {

    private final List<Symbol> targetColsSymbolsExclPartition;
    private final List<Reference> targetColsExclPartitionCols;
    private final boolean ignoreDuplicateKeys;
    private final Map<Reference, Symbol> onDuplicateKeyAssignments;
    private final List<Reference> allTargetColumns;
    /**
     * List of columns used for the result set
     */
    private final List<? extends Symbol> outputs;

    /**
     * List of values or expressions used to be retrieved from the inserted/updated rows,
     * empty if no values should be returned. The types of the returnValues need
     * to match with outputs.
     */
    private List<Symbol> returnValues;

    /**
     * @param relationName              identifying the table to write to
     * @param allTargetColumns          all the columns to be written in order of appearance
     * @param onDuplicateKeyAssignments reference to symbol map used for update on duplicate key
     */
    public ColumnIndexWriterProjection(RelationName relationName,
                                       @Nullable String partitionIdent,
                                       List<ColumnIdent> primaryKeys,
                                       List<Reference> allTargetColumns,
                                       List<Reference> targetColsExclPartitionCols,
                                       List<Symbol> targetColsSymbolsExclPartition,
                                       boolean ignoreDuplicateKeys,
                                       Map<Reference, Symbol> onDuplicateKeyAssignments,
                                       List<Symbol> primaryKeySymbols,
                                       List<Symbol> partitionedBySymbols,
                                       @Nullable ColumnIdent clusteredByColumn,
                                       @Nullable Symbol clusteredBySymbol,
                                       Settings settings,
                                       boolean autoCreateIndices,
                                       List<? extends Symbol> outputs,
                                       List<Symbol> returnValues
                                       ) {

        super(relationName, partitionIdent, primaryKeys, clusteredByColumn, settings, primaryKeySymbols, autoCreateIndices);
        assert partitionedBySymbols.stream().noneMatch(s -> SymbolVisitors.any(Symbols.IS_COLUMN, s))
            : "All references and fields in partitionedBySymbols must be resolved to inputColumns, got: " + partitionedBySymbols;
        this.allTargetColumns = allTargetColumns;
        this.partitionedBySymbols = partitionedBySymbols;
        this.ignoreDuplicateKeys = ignoreDuplicateKeys;
        this.onDuplicateKeyAssignments = onDuplicateKeyAssignments;
        this.targetColsExclPartitionCols = targetColsExclPartitionCols;
        this.clusteredBySymbol = clusteredBySymbol;
        this.targetColsSymbolsExclPartition = targetColsSymbolsExclPartition;
        this.outputs = outputs;
        this.returnValues = returnValues;
    }

    ColumnIndexWriterProjection(StreamInput in) throws IOException {
        super(in);

        if (in.readBoolean()) {
            targetColsSymbolsExclPartition = Symbols.listFromStream(in);
        } else {
            targetColsSymbolsExclPartition = Collections.emptyList();
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

        if (in.getVersion().onOrAfter(Version.V_4_2_0)) {
            int mapSize = in.readVInt();
            allTargetColumns = new ArrayList<>();
            for (int i = 0; i < mapSize; i++) {
                allTargetColumns.add(Reference.fromStream(in));
            }

            int outputSize = in.readVInt();
            if (outputSize > 0) {
                var result = new ArrayList<Symbol>(outputSize);
                for (int i = 0; i < outputSize; i++) {
                    result.add(Symbols.fromStream(in));
                }
                outputs = result;
            } else {
                outputs = List.of();
            }

            int returnValueSize = in.readVInt();
            if (returnValueSize > 0) {
                returnValues = new ArrayList<>(returnValueSize);
                for (int i = 0; i < returnValueSize; i++) {
                    returnValues.add(Symbols.fromStream(in));
                }
            } else {
                returnValues = List.of();
            }
        } else {
            returnValues = List.of();
            outputs = List.of();
            allTargetColumns = List.of();
        }

    }

    public List<? extends Symbol> outputs() {
        return outputs;
    }

    public List<Symbol> returnValues() {
        return returnValues;
    }

    public List<Reference> allTargetColumns() {
        return allTargetColumns;
    }

    public List<Symbol> columnSymbolsExclPartition() {
        return targetColsSymbolsExclPartition;
    }

    public List<Reference> columnReferencesExclPartition() {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ColumnIndexWriterProjection that = (ColumnIndexWriterProjection) o;
        return targetColsSymbolsExclPartition.equals(that.targetColsSymbolsExclPartition) &&
               targetColsExclPartitionCols.equals(that.targetColsExclPartitionCols) &&
               onDuplicateKeyAssignments.equals(that.onDuplicateKeyAssignments) &&
               allTargetColumns.equals(that.allTargetColumns) &&
               Objects.equals(outputs, that.outputs) &&
               Objects.equals(returnValues, that.returnValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            targetColsSymbolsExclPartition,
                            targetColsExclPartitionCols,
                            onDuplicateKeyAssignments,
                            allTargetColumns,
                            outputs,
                            returnValues);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        if (targetColsSymbolsExclPartition == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Symbols.toStream(targetColsSymbolsExclPartition, out);
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

        if (out.getVersion().onOrAfter(Version.V_4_2_0)) {
            out.writeVInt(allTargetColumns.size());
            for (var ref : allTargetColumns) {
                Symbols.toStream(ref, out);
            }
            if (outputs != null) {
                out.writeVInt(outputs.size());
                for (var output : outputs) {
                    Symbols.toStream(output, out);
                }
            } else {
                out.writeVInt(0);
            }
            out.writeVInt(returnValues.size());
            for (var returnValue : returnValues) {
                Symbols.toStream(returnValue, out);
            }
        }
    }

    public ColumnIndexWriterProjection bind(Function<? super Symbol, Symbol> binder) {
        HashMap<Reference, Symbol> boundOnDuplicateKeyAssignments =
            new HashMap<>(onDuplicateKeyAssignments.size());
        for (var assignment : onDuplicateKeyAssignments.entrySet()) {
            boundOnDuplicateKeyAssignments.put(
                assignment.getKey(),
                binder.apply(assignment.getValue()));
        }

        return new ColumnIndexWriterProjection(
            tableIdent(),
            null,
            primaryKeys,
            allTargetColumns,
            targetColsExclPartitionCols,
            targetColsSymbolsExclPartition,
            ignoreDuplicateKeys,
            boundOnDuplicateKeyAssignments,
            (List<Symbol>) ids(),
            partitionedBySymbols,
            clusteredByIdent(),
            clusteredBySymbol,
            Settings.EMPTY,
            autoCreateIndices(),
            outputs,
            returnValues
            );
    }
}

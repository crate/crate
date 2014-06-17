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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.crate.metadata.ColumnIdent;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.Value;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractIndexWriterProjection extends Projection {

    protected final static List<Symbol> OUTPUTS = ImmutableList.<Symbol>of(
            new Value(DataTypes.LONG)  // number of rows imported
    );

    protected final static String CONCURRENCY = "concurrency";
    protected final static int CONCURRENCY_DEFAULT = 4;

    protected final static String BULK_SIZE = "bulk_size";
    protected final static int BULK_SIZE_DEFAULT = 10000;

    protected Integer concurrency;
    protected Integer bulkActions;
    protected String tableName;
    protected List<ColumnIdent> primaryKeys;
    protected Optional<ColumnIdent> clusteredByColumn;

    protected List<Symbol> idSymbols;
    protected List<Symbol> partitionedBySymbols;
    protected @Nullable Symbol clusteredBySymbol;

    protected AbstractIndexWriterProjection() {}

    protected AbstractIndexWriterProjection(String tableName,
                                            List<ColumnIdent> primaryKeys,
                                            @Nullable ColumnIdent clusteredByColumn,
                                            Settings settings) {
        this.tableName = tableName;
        this.primaryKeys = primaryKeys;
        this.clusteredByColumn = Optional.fromNullable(clusteredByColumn);

        this.bulkActions = settings.getAsInt(BULK_SIZE, BULK_SIZE_DEFAULT);
        this.concurrency = settings.getAsInt(CONCURRENCY, CONCURRENCY_DEFAULT);
        Preconditions.checkArgument(concurrency > 0, "\"concurrency\" must be greater than 0.");
        Preconditions.checkArgument(bulkActions > 0, "\"bulk_size\" must be greater than 0.");
    }

    public List<Symbol> ids() {
        return idSymbols;
    }

    public Optional<ColumnIdent> clusteredByIdent() {
        return clusteredByColumn;
    }

    public Symbol clusteredBy() {
        return clusteredBySymbol;
    }

    public List<Symbol> partitionedBySymbols() {
        return partitionedBySymbols;
    }

    /**
     * generate Symbols needed for projection

     */
    protected void generateSymbols(int[] primaryKeyIndices,
                                   int[] partitionedByIndices,
                                   int clusteredByIdx) {
        this.idSymbols = new ArrayList<>(primaryKeys.size());
        for (int primaryKeyIndex : primaryKeyIndices) {
            idSymbols.add(new InputColumn(primaryKeyIndex));
        }

        this.partitionedBySymbols = new ArrayList<>(partitionedByIndices.length);
        for (int partitionByIndex : partitionedByIndices) {
            partitionedBySymbols.add(new InputColumn(partitionByIndex));
        }
        if (clusteredByIdx >= 0) {
            clusteredBySymbol = new InputColumn(clusteredByIdx);
        }
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.INDEX_WRITER;
    }

    public List<ColumnIdent> primaryKeys() {
        return primaryKeys;
    }

    @Override
    public List<? extends Symbol> outputs() {
        return OUTPUTS;
    }

    public Integer bulkActions() {
        return bulkActions;
    }
    public Integer concurrency() {
        return concurrency;
    }
    public String tableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AbstractIndexWriterProjection)) return false;

        AbstractIndexWriterProjection that = (AbstractIndexWriterProjection) o;

        if (!bulkActions.equals(that.bulkActions)) return false;
        if (clusteredBySymbol != null ? !clusteredBySymbol.equals(that.clusteredBySymbol) : that.clusteredBySymbol != null)
            return false;
        if (!concurrency.equals(that.concurrency)) return false;
        if (!idSymbols.equals(that.idSymbols)) return false;
        if (!partitionedBySymbols.equals(that.partitionedBySymbols))
            return false;
        if (!primaryKeys.equals(that.primaryKeys)) return false;
        if (!tableName.equals(that.tableName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + concurrency.hashCode();
        result = 31 * result + bulkActions.hashCode();
        result = 31 * result + tableName.hashCode();
        result = 31 * result + primaryKeys.hashCode();
        result = 31 * result + idSymbols.hashCode();
        result = 31 * result + partitionedBySymbols.hashCode();
        result = 31 * result + (clusteredBySymbol != null ? clusteredBySymbol.hashCode() : 0);
        return result;
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
            ColumnIdent ident = new ColumnIdent();
            ident.readFrom(in);
            primaryKeys.add(ident);
        }

        int numPartitionedSymbols = in.readVInt();
        partitionedBySymbols = new ArrayList<>(numPartitionedSymbols);
        for (int i = 0; i < numPartitionedSymbols; i++) {
            partitionedBySymbols.add(Symbol.fromStream(in));
        }
        if (in.readBoolean()) {
            clusteredBySymbol = Symbol.fromStream(in);
        } else {
            clusteredBySymbol = null;
        }
        if (in.readBoolean()) {
            ColumnIdent ident = new ColumnIdent();
            ident.readFrom(in);
            clusteredByColumn = Optional.of(ident);
        } else {
            clusteredByColumn = Optional.absent();
        }
        concurrency = in.readVInt();
        bulkActions = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(tableName);

        out.writeVInt(idSymbols.size());
        for (Symbol idSymbol : idSymbols) {
            Symbol.toStream(idSymbol, out);
        }
        out.writeVInt(primaryKeys.size());
        for (ColumnIdent primaryKey : primaryKeys) {
            primaryKey.writeTo(out);
        }
        out.writeVInt(partitionedBySymbols.size());
        for (Symbol partitionedSymbol : partitionedBySymbols) {
            Symbol.toStream(partitionedSymbol, out);
        }
        if (clusteredBySymbol == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Symbol.toStream(clusteredBySymbol, out);
        }

        if (!clusteredByColumn.isPresent()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            clusteredByColumn.get().writeTo(out);
        }
        out.writeVInt(concurrency);
        out.writeVInt(bulkActions);
    }
}

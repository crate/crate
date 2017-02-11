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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.analyze.symbol.Value;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.TableIdent;
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

    protected final static String BULK_SIZE = "bulk_size";
    protected final static int BULK_SIZE_DEFAULT = 10000;

    protected Integer bulkActions;
    protected TableIdent tableIdent;
    protected String partitionIdent;
    protected List<ColumnIdent> primaryKeys;
    protected
    @Nullable
    ColumnIdent clusteredByColumn;

    protected List<Symbol> idSymbols;
    protected List<Symbol> partitionedBySymbols;
    protected
    @Nullable
    Symbol clusteredBySymbol;

    protected boolean autoCreateIndices;


    protected AbstractIndexWriterProjection(TableIdent tableIdent,
                                            @Nullable String partitionIdent,
                                            List<ColumnIdent> primaryKeys,
                                            @Nullable ColumnIdent clusteredByColumn,
                                            Settings settings,
                                            boolean autoCreateIndices) {
        this.tableIdent = tableIdent;
        this.partitionIdent = partitionIdent;
        this.primaryKeys = primaryKeys;
        this.clusteredByColumn = clusteredByColumn;
        this.autoCreateIndices = autoCreateIndices;

        this.bulkActions = settings.getAsInt(BULK_SIZE, BULK_SIZE_DEFAULT);
        Preconditions.checkArgument(bulkActions > 0, "\"bulk_size\" must be greater than 0.");
    }

    protected AbstractIndexWriterProjection(StreamInput in) throws IOException {
        tableIdent = new TableIdent(in);

        partitionIdent = in.readOptionalString();
        idSymbols = Symbols.listFromStream(in);

        int numPks = in.readVInt();
        primaryKeys = new ArrayList<>(numPks);
        for (int i = 0; i < numPks; i++) {
            primaryKeys.add(new ColumnIdent(in));
        }

        partitionedBySymbols = Symbols.listFromStream(in);
        if (in.readBoolean()) {
            clusteredBySymbol = Symbols.fromStream(in);
        } else {
            clusteredBySymbol = null;
        }
        if (in.readBoolean()) {
            clusteredByColumn = new ColumnIdent(in);
        }
        bulkActions = in.readVInt();
        autoCreateIndices = in.readBoolean();
    }


    public List<? extends Symbol> ids() {
        return idSymbols;
    }

    @Nullable
    public ColumnIdent clusteredByIdent() {
        return clusteredByColumn;
    }

    @Nullable
    public Symbol clusteredBy() {
        return clusteredBySymbol;
    }

    public List<Symbol> partitionedBySymbols() {
        return partitionedBySymbols;
    }

    public boolean autoCreateIndices() {
        return autoCreateIndices;
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

    public TableIdent tableIdent() {
        return tableIdent;
    }

    @Nullable
    public String partitionIdent() {
        return partitionIdent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AbstractIndexWriterProjection)) return false;

        AbstractIndexWriterProjection that = (AbstractIndexWriterProjection) o;

        if (autoCreateIndices != that.autoCreateIndices) return false;
        if (!bulkActions.equals(that.bulkActions)) return false;
        if (clusteredByColumn != null ? !clusteredByColumn.equals(that.clusteredByColumn) :
            that.clusteredByColumn != null)
            return false;
        if (clusteredBySymbol != null ? !clusteredBySymbol.equals(that.clusteredBySymbol) :
            that.clusteredBySymbol != null)
            return false;
        if (!idSymbols.equals(that.idSymbols)) return false;
        if (!partitionedBySymbols.equals(that.partitionedBySymbols))
            return false;
        if (!primaryKeys.equals(that.primaryKeys)) return false;
        if (!tableIdent.equals(that.tableIdent)) return false;
        if (partitionIdent != null ? !partitionIdent.equals(that.partitionIdent) : that.partitionIdent != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + bulkActions.hashCode();
        result = 31 * result + tableIdent.hashCode();
        result = 31 * result + (partitionIdent != null ? partitionIdent.hashCode() : 0);
        result = 31 * result + primaryKeys.hashCode();
        result = 31 * result + (clusteredByColumn != null ? clusteredByColumn.hashCode() : 0);
        result = 31 * result + idSymbols.hashCode();
        result = 31 * result + partitionedBySymbols.hashCode();
        result = 31 * result + (clusteredBySymbol != null ? clusteredBySymbol.hashCode() : 0);
        result = 31 * result + (autoCreateIndices ? 1 : 0);
        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        tableIdent.writeTo(out);
        out.writeOptionalString(partitionIdent);

        Symbols.toStream(idSymbols, out);
        out.writeVInt(primaryKeys.size());
        for (ColumnIdent primaryKey : primaryKeys) {
            primaryKey.writeTo(out);
        }
        Symbols.toStream(partitionedBySymbols, out);
        if (clusteredBySymbol == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Symbols.toStream(clusteredBySymbol, out);
        }

        if (clusteredByColumn == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            clusteredByColumn.writeTo(out);
        }
        out.writeVInt(bulkActions);
        out.writeBoolean(autoCreateIndices);
    }
}

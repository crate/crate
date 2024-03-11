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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.Nullable;

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.types.DataTypes;

public abstract class AbstractIndexWriterProjection extends Projection {

    public static final List<? extends Symbol> OUTPUTS = List.of(new InputColumn(0, DataTypes.LONG));  // number of rows imported

    private static final String BULK_SIZE = "bulk_size";

    @VisibleForTesting
    public static final int BULK_SIZE_DEFAULT = 10000;

    private final int bulkActions;
    protected RelationName relationName;
    protected String partitionIdent;
    protected List<ColumnIdent> primaryKeys;

    @Nullable
    private ColumnIdent clusteredByColumn;

    private List<Symbol> idSymbols;
    List<Symbol> partitionedBySymbols;

    @Nullable
    Symbol clusteredBySymbol;

    private boolean autoCreateIndices;


    protected AbstractIndexWriterProjection(RelationName relationName,
                                            @Nullable String partitionIdent,
                                            List<ColumnIdent> primaryKeys,
                                            @Nullable ColumnIdent clusteredByColumn,
                                            Settings settings,
                                            List<Symbol> idSymbols,
                                            boolean autoCreateIndices) {
        this.relationName = relationName;
        this.partitionIdent = partitionIdent;
        this.primaryKeys = primaryKeys;
        this.clusteredByColumn = clusteredByColumn;
        this.autoCreateIndices = autoCreateIndices;
        this.idSymbols = idSymbols;

        this.bulkActions = settings.getAsInt(BULK_SIZE, BULK_SIZE_DEFAULT);
        if (bulkActions <= 0) {
            throw new IllegalArgumentException("\"bulk_size\" must be greater than 0.");
        }
    }

    protected AbstractIndexWriterProjection(StreamInput in) throws IOException {
        relationName = new RelationName(in);

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


    public List<Symbol> ids() {
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

    public int bulkActions() {
        return bulkActions;
    }

    public RelationName tableIdent() {
        return relationName;
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
        if (bulkActions != that.bulkActions) return false;
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
        if (!relationName.equals(that.relationName)) return false;
        if (partitionIdent != null ? !partitionIdent.equals(that.partitionIdent) : that.partitionIdent != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Integer.hashCode(bulkActions);
        result = 31 * result + relationName.hashCode();
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
        relationName.writeTo(out);
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

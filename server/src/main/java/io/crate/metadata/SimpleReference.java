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

package io.crate.metadata;

import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.TypeParsers;
import org.jetbrains.annotations.Nullable;

import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.format.Style;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.StorageSupport;

public class SimpleReference implements Reference {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(SimpleReference.class);

    protected DataType<?> type;

    protected final int position;
    protected final long oid;
    protected boolean isDropped;

    protected final ReferenceIdent ident;
    protected final ColumnPolicy columnPolicy;
    protected final RowGranularity granularity;
    protected final IndexType indexType;
    protected final boolean nullable;
    protected final boolean hasDocValues;

    @Nullable
    protected final Symbol defaultExpression;

    public SimpleReference(StreamInput in) throws IOException {
        ident = new ReferenceIdent(in);
        if (in.getVersion().before(Version.V_4_6_0)) {
            Integer pos = in.readOptionalVInt();
            position = pos == null ? 0 : pos;
        } else {
            position = in.readVInt();
        }
        if (in.getVersion().onOrAfter(Version.V_5_5_0)) {
            oid = in.readLong();
            isDropped = in.readBoolean();
        } else {
            oid = COLUMN_OID_UNASSIGNED;
            isDropped = false;
        }
        type = DataTypes.fromStream(in);
        granularity = RowGranularity.fromStream(in);

        columnPolicy = ColumnPolicy.VALUES.get(in.readVInt());
        indexType = IndexType.fromStream(in);
        nullable = in.readBoolean();

        // property was "columnStoreDisabled" so need to reverse the value.
        hasDocValues = !in.readBoolean();
        final boolean hasDefaultExpression = in.readBoolean();
        defaultExpression = hasDefaultExpression
            ? Symbols.fromStream(in)
            : null;
    }

    public SimpleReference(ReferenceIdent ident,
                           RowGranularity granularity,
                           DataType<?> type,
                           int position,
                           @Nullable Symbol defaultExpression) {
        this(ident,
             granularity,
             type,
             ColumnPolicy.DYNAMIC,
             IndexType.PLAIN,
             true,
             false,
             position,
             COLUMN_OID_UNASSIGNED,
             false,
             defaultExpression);
    }

    public SimpleReference(ReferenceIdent ident,
                           RowGranularity granularity,
                           DataType<?> type,
                           ColumnPolicy columnPolicy,
                           IndexType indexType,
                           boolean nullable,
                           boolean hasDocValues,
                           int position,
                           long oid,
                           boolean isDropped,
                           @Nullable Symbol defaultExpression) {
        this.position = position;
        this.ident = ident;
        this.type = type;
        this.granularity = granularity;
        this.columnPolicy = columnPolicy;
        this.indexType = indexType;
        this.nullable = nullable;
        this.hasDocValues = hasDocValues;
        this.defaultExpression = defaultExpression;
        this.oid = oid;
        this.isDropped = isDropped;
    }

    /**
     * Returns a cloned Reference with the given ident
     */
    @Override
    public Reference getRelocated(ReferenceIdent newIdent) {
        return new SimpleReference(
            newIdent,
            granularity,
            type,
            columnPolicy,
            indexType,
            nullable,
            hasDocValues,
            position,
            oid,
            isDropped,
            defaultExpression
        );
    }

    @Override
    public Reference applyColumnOid(LongSupplier oidSupplier) {
        if (oid != COLUMN_OID_UNASSIGNED) {
            return this;
        }
        return new SimpleReference(
                ident,
                granularity,
                type,
                columnPolicy,
                indexType,
                nullable,
                hasDocValues,
                position,
                oidSupplier.getAsLong(),
                isDropped,
                defaultExpression
        );
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.REFERENCE;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitReference(this, context);
    }

    @Override
    public DataType<?> valueType() {
        return type;
    }

    @Override
    public String toString() {
        return toString(Style.UNQUALIFIED);
    }

    @Override
    public String toString(Style style) {
        if (style == Style.QUALIFIED) {
            return ident.tableIdent().sqlFqn() + '.' + column().quotedOutputName();
        }
        return column().quotedOutputName();
    }

    @Override
    public ReferenceIdent ident() {
        return ident;
    }

    @Override
    public ColumnIdent column() {
        return ident.columnIdent();
    }

    @Override
    public RowGranularity granularity() {
        return granularity;
    }

    @Override
    public ColumnPolicy columnPolicy() {
        return columnPolicy;
    }

    @Override
    public IndexType indexType() {
        return indexType;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public boolean hasDocValues() {
        return hasDocValues;
    }

    @Override
    public int position() {
        return position;
    }

    @Override
    public long oid() {
        return oid;
    }

    @Override
    public boolean isDropped() {
        return isDropped;
    }

    @Override
    public void setDropped() {
        this.isDropped = true;
    }

    @Nullable
    @Override
    public Symbol defaultExpression() {
        return defaultExpression;
    }

    @Override
    public boolean isGenerated() {
        return false;
    }

    @Override
    public Map<String, Object> toMapping(int position) {
        DataType<?> innerType = ArrayType.unnest(type);
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("type", DataTypes.esMappingNameFrom(innerType.id()));
        mapping.put("position", position);
        if (oid != COLUMN_OID_UNASSIGNED) {
            mapping.put("oid", oid);
        }
        if (isDropped) {
            mapping.put("dropped", true);
        }
        if (indexType == IndexType.NONE && type.id() != ObjectType.ID) {
            mapping.put("index", false);
        }
        StorageSupport<?> storageSupport = innerType.storageSupport();
        if (storageSupport != null) {
            boolean docValuesDefault = storageSupport.getComputedDocValuesDefault(indexType);
            if (docValuesDefault != hasDocValues) {
                mapping.put(TypeParsers.DOC_VALUES, Boolean.toString(hasDocValues));
            }
        }
        if (defaultExpression != null) {
            mapping.put("default_expr", defaultExpression.toString(Style.UNQUALIFIED));
        }
        innerType.addMappingOptions(mapping);
        return mapping;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleReference reference = (SimpleReference) o;
        if (nullable != reference.nullable) {
            return false;
        }
        if (hasDocValues != reference.hasDocValues) {
            return false;
        }
        if (!type.equals(reference.type)) {
            return false;
        }
        if (!Objects.equals(position, reference.position)) {
            return false;
        }
        if (!ident.equals(reference.ident)) {
            return false;
        }
        if (columnPolicy != reference.columnPolicy) {
            return false;
        }
        if (granularity != reference.granularity) {
            return false;
        }
        if (indexType != reference.indexType) {
            return false;
        }
        if (oid != reference.oid) {
            return false;
        }
        return Objects.equals(defaultExpression, reference.defaultExpression);
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + Integer.hashCode(position);
        result = 31 * result + ident.hashCode();
        result = 31 * result + columnPolicy.hashCode();
        result = 31 * result + granularity.hashCode();
        result = 31 * result + indexType.hashCode();
        result = 31 * result + (nullable ? 1 : 0);
        result = 31 * result + (hasDocValues ? 1 : 0);
        result = 31 * result + Long.hashCode(oid);
        result = 31 * result + (defaultExpression != null ? defaultExpression.hashCode() : 0);
        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ident.writeTo(out);
        if (out.getVersion().before(Version.V_4_6_0)) {
            out.writeOptionalVInt(position);
        } else {
            out.writeVInt(position);
        }
        if (out.getVersion().onOrAfter(Version.V_5_5_0)) {
            out.writeLong(oid);
            out.writeBoolean(isDropped);
        }
        DataTypes.toStream(type, out);
        RowGranularity.toStream(granularity, out);

        out.writeVInt(columnPolicy.ordinal());
        out.writeVInt(indexType.ordinal());
        out.writeBoolean(nullable);
        // property was "columnStoreDisabled" so need to reverse the value.
        out.writeBoolean(!hasDocValues);
        final boolean hasDefaultExpression = defaultExpression != null;
        out.writeBoolean(hasDefaultExpression);
        if (hasDefaultExpression) {
            Symbols.toStream(defaultExpression, out);
        }
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE
            + type.ramBytesUsed()
            + ident.ramBytesUsed()
            + (defaultExpression == null ? 0 : defaultExpression.ramBytesUsed());
    }
}

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

import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.format.Style;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;

public class Reference extends Symbol {

    public enum IndexType {
        ANALYZED,
        NOT_ANALYZED,
        NO;
    }

    protected DataType<?> type;

    private final int position;
    private final ReferenceIdent ident;
    private final ColumnPolicy columnPolicy;
    private final RowGranularity granularity;
    private final IndexType indexType;
    private final boolean nullable;
    private final boolean columnStoreDisabled;

    @Nullable
    private final Symbol defaultExpression;

    public Reference(StreamInput in) throws IOException {
        ident = new ReferenceIdent(in);
        if (in.getVersion().before(Version.V_4_6_0)) {
            Integer pos = in.readOptionalVInt();
            position = pos == null ? 0 : pos;
        } else {
            position = in.readVInt();
        }
        type = DataTypes.fromStream(in);
        granularity = RowGranularity.fromStream(in);

        columnPolicy = ColumnPolicy.values()[in.readVInt()];
        indexType = IndexType.values()[in.readVInt()];
        nullable = in.readBoolean();
        columnStoreDisabled = in.readBoolean();
        final boolean hasDefaultExpression = in.readBoolean();
        defaultExpression = hasDefaultExpression
            ? Symbols.fromStream(in)
            : null;
    }

    public Reference(ReferenceIdent ident,
                     RowGranularity granularity,
                     DataType<?> type,
                     int position,
                     @Nullable Symbol defaultExpression) {
        this(ident,
             granularity,
             type,
             ColumnPolicy.DYNAMIC,
             IndexType.NOT_ANALYZED,
             true,
             position,
             defaultExpression);
    }

    public Reference(ReferenceIdent ident,
                     RowGranularity granularity,
                     DataType<?> type,
                     ColumnPolicy columnPolicy,
                     IndexType indexType,
                     boolean nullable,
                     int position,
                     @Nullable Symbol defaultExpression) {
        this(ident,
             granularity,
             type,
             columnPolicy,
             indexType,
             nullable,
             false,
             position,
             defaultExpression);
    }

    public Reference(ReferenceIdent ident,
                     RowGranularity granularity,
                     DataType<?> type,
                     ColumnPolicy columnPolicy,
                     IndexType indexType,
                     boolean nullable,
                     boolean columnStoreDisabled,
                     int position,
                     @Nullable Symbol defaultExpression) {
        this.position = position;
        this.ident = ident;
        this.type = type;
        this.granularity = granularity;
        this.columnPolicy = columnPolicy;
        this.indexType = indexType;
        this.nullable = nullable;
        this.columnStoreDisabled = columnStoreDisabled;
        this.defaultExpression = defaultExpression;
    }

    /**
     * Returns a cloned Reference with the given ident
     */
    public Reference getRelocated(ReferenceIdent newIdent) {
        return new Reference(newIdent,
                             granularity,
                             type,
                             columnPolicy,
                             indexType,
                             nullable,
                             columnStoreDisabled,
                             position,
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
    public String toString(Style style) {
        if (style == Style.QUALIFIED) {
            return ident.tableIdent().sqlFqn() + '.' + column().quotedOutputName();
        }
        return column().quotedOutputName();
    }

    public ReferenceIdent ident() {
        return ident;
    }

    public ColumnIdent column() {
        return ident.columnIdent();
    }

    public RowGranularity granularity() {
        return granularity;
    }

    public ColumnPolicy columnPolicy() {
        return columnPolicy;
    }

    public IndexType indexType() {
        return indexType;
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean isColumnStoreDisabled() {
        return columnStoreDisabled;
    }

    public int position() {
        return position;
    }

    @Nullable
    public Symbol defaultExpression() {
        return defaultExpression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Reference reference = (Reference) o;
        if (nullable != reference.nullable) {
            return false;
        }
        if (columnStoreDisabled != reference.columnStoreDisabled) {
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
        result = 31 * result + (columnStoreDisabled ? 1 : 0);
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
        DataTypes.toStream(type, out);
        RowGranularity.toStream(granularity, out);

        out.writeVInt(columnPolicy.ordinal());
        out.writeVInt(indexType.ordinal());
        out.writeBoolean(nullable);
        out.writeBoolean(columnStoreDisabled);
        final boolean hasDefaultExpression = defaultExpression != null;
        out.writeBoolean(hasDefaultExpression);
        if (hasDefaultExpression) {
            Symbols.toStream(defaultExpression, out);
        }
    }

    public static void toStream(Reference reference, StreamOutput out) throws IOException {
        out.writeVInt(reference.symbolType().ordinal());
        reference.writeTo(out);
    }

    public static <R extends Reference> R fromStream(StreamInput in) throws IOException {
        //noinspection unchecked
        return (R) SymbolType.VALUES.get(in.readVInt()).newInstance(in);
    }
}

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

package io.crate.metadata;

import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.Symbols;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Comparator;
import java.util.Objects;

import static io.crate.metadata.RowGranularity.DOC;

public class Reference extends Symbol {

    public static final Comparator<Reference> COMPARE_BY_COLUMN_IDENT = Comparator.comparing(Reference::column);

    public enum IndexType {
        ANALYZED,
        NOT_ANALYZED,
        NO;
    }

    protected DataType type;
    private final Integer position;
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
        position = in.readOptionalVInt();
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
                     DataType type,
                     @Nullable Integer position,
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
                     DataType type,
                     ColumnPolicy columnPolicy,
                     IndexType indexType,
                     boolean nullable,
                     @Nullable Integer position,
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
                     DataType type,
                     ColumnPolicy columnPolicy,
                     IndexType indexType,
                     boolean nullable,
                     boolean columnStoreDisabled,
                     @Nullable Integer position,
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
    public DataType valueType() {
        return type;
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

    @Nullable
    public Integer position() {
        return position;
    }

    @Nullable
    public Symbol defaultExpression() {
        return defaultExpression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Reference reference = (Reference) o;
        return Objects.equals(position, reference.position) &&
               nullable == reference.nullable &&
               columnStoreDisabled == reference.columnStoreDisabled &&
               Objects.equals(type, reference.type) &&
               Objects.equals(ident, reference.ident) &&
               Objects.equals(defaultExpression, reference.defaultExpression) &&
               columnPolicy == reference.columnPolicy &&
               granularity == reference.granularity &&
               indexType == reference.indexType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(position,
                            type,
                            ident,
                            columnPolicy,
                            granularity,
                            indexType,
                            nullable,
                            columnStoreDisabled,
                            defaultExpression);
    }

    @Override
    public String representation() {
        return "Ref{" + ident.tableIdent() + '.' + ident.columnIdent() + ", " + type + '}';
    }

    @Override
    public String toString() {
        return "Ref{"
               + ident.tableIdent().fqn() + '.' + ident.columnIdent().sqlFqn() + "::" + type +
               ", pos=" + position +
               (columnPolicy != ColumnPolicy.DYNAMIC ? ", columnPolicy=" + columnPolicy : "") +
               (granularity != DOC ? ", granularity=" + granularity : "") +
               (indexType != IndexType.NOT_ANALYZED ? ", index=" + indexType : "") +
               ", nullable=" + nullable +
               (columnStoreDisabled ? ", columnStoreOff=" + columnStoreDisabled : "") +
               (defaultExpression != null ? ", defaultExpression=" + defaultExpression : "") +
               '}';
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ident.writeTo(out);
        out.writeOptionalVInt(position);
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
        return (R) SymbolType.VALUES.get(in.readVInt()).newInstance(in);
    }
}

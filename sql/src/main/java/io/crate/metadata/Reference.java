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

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolType;
import io.crate.analyze.symbol.SymbolVisitor;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Comparator;
import java.util.Objects;

public class Reference extends Symbol {

    public static final Comparator<Reference> COMPARE_BY_COLUMN_IDENT = new Comparator<Reference>() {
        @Override
        public int compare(Reference o1, Reference o2) {
            return o1.ident().columnIdent().compareTo(o2.ident().columnIdent());
        }
    };

    public static final Function<? super Reference, ColumnIdent> TO_COLUMN_IDENT = new Function<Reference, ColumnIdent>() {
        @Nullable
        @Override
        public ColumnIdent apply(@Nullable Reference input) {
            return input == null ? null : input.ident.columnIdent();
        }
    };

    public static final Function<? super Reference, String> TO_COLUMN_NAME = new Function<Reference, String>() {
        @Nullable
        @Override
        public String apply(@Nullable Reference input) {
            return input == null ? null : input.ident.columnIdent().sqlFqn();
        }
    };

    public enum IndexType {
        ANALYZED,
        NOT_ANALYZED,
        NO;
    }

    protected DataType type;
    private ReferenceIdent ident;
    private ColumnPolicy columnPolicy = ColumnPolicy.DYNAMIC;
    private RowGranularity granularity;
    private IndexType indexType = IndexType.NOT_ANALYZED;
    private boolean nullable = true;
    private boolean columnStoreDisabled = false;  // are DOC_VALUES disabled?

    public Reference(StreamInput in) throws IOException {
        ident = new ReferenceIdent(in);
        type = DataTypes.fromStream(in);
        granularity = RowGranularity.fromStream(in);

        columnPolicy = ColumnPolicy.values()[in.readVInt()];
        indexType = IndexType.values()[in.readVInt()];
        nullable = in.readBoolean();
        columnStoreDisabled = in.readBoolean();
    }

    public Reference() {

    }

    public Reference(ReferenceIdent ident,
                     RowGranularity granularity,
                     DataType type) {
        this(ident, granularity, type, ColumnPolicy.DYNAMIC, IndexType.NOT_ANALYZED, true);
    }

    public Reference(ReferenceIdent ident,
                     RowGranularity granularity,
                     DataType type,
                     ColumnPolicy columnPolicy,
                     IndexType indexType,
                     boolean nullable) {
        this(ident, granularity, type, columnPolicy, indexType, nullable, false);
    }

    public Reference(ReferenceIdent ident,
                     RowGranularity granularity,
                     DataType type,
                     ColumnPolicy columnPolicy,
                     IndexType indexType,
                     boolean nullable,
                     boolean columnStoreDisabled) {
        this.ident = ident;
        this.type = type;
        this.granularity = granularity;
        this.columnPolicy = columnPolicy;
        this.indexType = indexType;
        this.nullable = nullable;
        this.columnStoreDisabled = columnStoreDisabled;
    }

    /**
     * Returns a cloned Reference with the given ident
     */
    public Reference getRelocated(ReferenceIdent newIdent) {
        return new Reference(newIdent, granularity, type, columnPolicy, indexType, nullable, columnStoreDisabled);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Reference reference = (Reference) o;
        return nullable == reference.nullable &&
               columnStoreDisabled == reference.columnStoreDisabled &&
               Objects.equals(type, reference.type) &&
               Objects.equals(ident, reference.ident) &&
               columnPolicy == reference.columnPolicy &&
               granularity == reference.granularity &&
               indexType == reference.indexType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, ident, columnPolicy, granularity, indexType, nullable, columnStoreDisabled);
    }

    @Override
    public String representation() {
        return "Ref{" + ident.tableIdent() + '.' + ident.columnIdent() + ", " + type + '}';
    }

    @Override
    public String toString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this)
            .add("ident", ident)
            .add("granularity", granularity)
            .add("type", type);
        if (type.equals(DataTypes.OBJECT)) {
            helper.add("column policy", columnPolicy.name());
        }
        helper.add("index type", indexType.name());
        helper.add("nullable", nullable);
        helper.add("columnstore enabled", columnStoreDisabled);
        return helper.toString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ident.writeTo(out);
        DataTypes.toStream(type, out);
        RowGranularity.toStream(granularity, out);

        out.writeVInt(columnPolicy.ordinal());
        out.writeVInt(indexType.ordinal());
        out.writeBoolean(nullable);
        out.writeBoolean(columnStoreDisabled);
    }


    public static void toStream(Reference reference, StreamOutput out) throws IOException {
        out.writeVInt(reference.symbolType().ordinal());
        reference.writeTo(out);
    }

    public static <R extends Reference> R fromStream(StreamInput in) throws IOException {
        return (R) SymbolType.VALUES.get(in.readVInt()).newInstance(in);
    }
}

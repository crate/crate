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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.planner.RowGranularity;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class ReferenceInfo implements Streamable {

    public static class Builder {
        private ReferenceIdent ident;
        private DataType type;
        private ColumnPolicy columnPolicy = ColumnPolicy.DYNAMIC; // reflects default value of objects
        private RowGranularity granularity;
        private IndexType indexType = IndexType.NOT_ANALYZED; // reflects default behaviour

        public Builder type(DataType type) {
            this.type = type;
            return this;
        }

        public Builder granularity(RowGranularity rowGranularity) {
            this.granularity = rowGranularity;
            return this;
        }

        public Builder ident(ReferenceIdent ident) {
            this.ident = ident;
            return this;
        }

        public Builder ident(TableIdent table, ColumnIdent column) {
            this.ident = new ReferenceIdent(table, column);
            return this;
        }

        public Builder columnPolicy(ColumnPolicy columnPolicy) {
            this.columnPolicy = columnPolicy;
            return this;
        }

        public ReferenceInfo build() {
            Preconditions.checkNotNull(ident, "ident is null");
            Preconditions.checkNotNull(granularity, "granularity is null");
            Preconditions.checkNotNull(type, "type is null");
            return new ReferenceInfo(ident, granularity, type, columnPolicy, indexType);
        }
    }

    public static enum IndexType {
        ANALYZED,
        NOT_ANALYZED,
        NO;

        public String toString() {
            return name().toLowerCase();
        }
    }


    private ReferenceIdent ident;
    private DataType type;
    private ColumnPolicy columnPolicy = ColumnPolicy.DYNAMIC;
    private RowGranularity granularity;
    private IndexType indexType = IndexType.NOT_ANALYZED;

    public ReferenceInfo() {

    }

    public ReferenceInfo(ReferenceIdent ident,
                         RowGranularity granularity,
                         DataType type) {
        this(ident, granularity, type, ColumnPolicy.DYNAMIC, IndexType.NOT_ANALYZED);
    }

    public ReferenceInfo(ReferenceIdent ident,
                         RowGranularity granularity,
                         DataType type,
                         ColumnPolicy columnPolicy,
                         IndexType indexType) {
        this.ident = ident;
        this.type = type;
        this.granularity = granularity;
        this.columnPolicy = columnPolicy;
        this.indexType = indexType;
    }

    /**
     * Returns a cloned ReferenceInfo with the given ident
     */
    public ReferenceInfo getRelocated(ReferenceIdent newIdent){
        return new ReferenceInfo(newIdent, granularity, type, columnPolicy, indexType);
    }

    public ReferenceIdent ident() {
        return ident;
    }

    public DataType type() {
        return type;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReferenceInfo that = (ReferenceInfo) o;

        if (granularity != that.granularity) return false;
        if (ident != null ? !ident.equals(that.ident) : that.ident != null) return false;
        if (columnPolicy.ordinal() != that.columnPolicy.ordinal()) { return false; }
        if (indexType.ordinal() != that.indexType.ordinal()) { return false; }
        if (type != null ? !type.equals(that.type) : that.type != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(granularity, ident, type, columnPolicy, indexType);
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
        helper.add("index type", indexType.toString());
        return helper.toString();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        ident = new ReferenceIdent();
        ident.readFrom(in);
        type = DataTypes.fromStream(in);
        granularity = RowGranularity.fromStream(in);

        columnPolicy = ColumnPolicy.values()[in.readVInt()];
        indexType = IndexType.values()[in.readVInt()];
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ident.writeTo(out);
        DataTypes.toStream(type, out);
        RowGranularity.toStream(granularity, out);

        out.writeVInt(columnPolicy.ordinal());
        out.writeVInt(indexType.ordinal());
    }
}

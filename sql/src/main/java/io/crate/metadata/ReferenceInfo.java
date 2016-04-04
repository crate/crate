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
import com.google.common.base.Objects;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Comparator;
import java.util.Locale;

public class ReferenceInfo implements Streamable {

    public static final Comparator<ReferenceInfo> COMPARE_BY_COLUMN_IDENT = new Comparator<ReferenceInfo>() {
        @Override
        public int compare(ReferenceInfo o1, ReferenceInfo o2) {
            return o1.ident().columnIdent().compareTo(o2.ident().columnIdent());
        }
    };

    public static final Function<? super ReferenceInfo, ColumnIdent> TO_COLUMN_IDENT = new Function<ReferenceInfo, ColumnIdent>() {
        @Nullable
        @Override
        public ColumnIdent apply(@Nullable ReferenceInfo input) {
            return input == null ? null : input.ident.columnIdent();
        }
    };

    public enum IndexType {
        ANALYZED,
        NOT_ANALYZED,
        NO;

        public String toString() {
            return name().toLowerCase(Locale.ENGLISH);
        }
    }

    public interface ReferenceInfoFactory<T extends ReferenceInfo> {
        T newInstance();
    }

    public static final ReferenceInfoFactory<ReferenceInfo> FACTORY = new ReferenceInfoFactory<ReferenceInfo>() {
        @Override
        public ReferenceInfo newInstance() {
            return new ReferenceInfo();
        }
    };


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


    public ReferenceInfoType referenceInfoType() {
        return ReferenceInfoType.DEFAULT;
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

    public static void toStream(ReferenceInfo referenceInfo, StreamOutput out) throws IOException {
        out.writeVInt(referenceInfo.referenceInfoType().ordinal());
        referenceInfo.writeTo(out);
    }

    public static <R extends ReferenceInfo> R fromStream(StreamInput in) throws IOException {
        ReferenceInfo referenceInfo = ReferenceInfoType.values()[in.readVInt()].newInstance();
        referenceInfo.readFrom(in);
        return (R) referenceInfo;
    }

}

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

package io.crate.metadata;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.planner.RowGranularity;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class IndexReferenceInfo extends ReferenceInfo {

    public static class Builder {
        private ReferenceIdent ident;
        private IndexType indexType = IndexType.ANALYZED;
        private List<ReferenceInfo> columns = new ArrayList<>();
        private String analyzer = null;

        public Builder ident(ReferenceIdent ident) {
            this.ident = ident;
            return this;
        }

        public Builder indexType(IndexType indexType) {
            this.indexType = indexType;
            return this;
        }

        public Builder addColumn(ReferenceInfo info) {
            this.columns.add(info);
            return this;
        }

        public Builder analyzer(String name) {
            this.analyzer = name;
            return this;
        }

        public IndexReferenceInfo build() {
            Preconditions.checkNotNull(ident, "ident is null");
            return new IndexReferenceInfo(ident, indexType, columns, analyzer);
        }
    }

    private String analyzer;
    private List<ReferenceInfo> columns;

    public IndexReferenceInfo(ReferenceIdent ident,
                         IndexType indexType,
                         List<ReferenceInfo> columns,
                         @Nullable String analyzer) {
        super(ident, RowGranularity.DOC, DataTypes.STRING, ColumnPolicy.DYNAMIC, indexType);
        this.columns = MoreObjects.firstNonNull(columns, Collections.<ReferenceInfo>emptyList());
        this.analyzer = analyzer;
    }

    public List<ReferenceInfo> columns() {
        return columns;
    }

    public String analyzer() {
        return analyzer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        IndexReferenceInfo that = (IndexReferenceInfo) o;

        if (analyzer != null ? !analyzer.equals(that.analyzer) : that.analyzer != null)
            return false;
        if (!columns.equals(that.columns)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (analyzer != null ? analyzer.hashCode() : 0);
        result = 31 * result + columns.hashCode();
        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        /**
         * if this is extended make sure to adjust all other places where ReferenceInfo/IndexReferenceInfo
         * is streamed for example in
         * {@link io.crate.planner.symbol.Reference#readFrom(org.elasticsearch.common.io.stream.StreamInput)}
         */
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("ident", ident()).toString();
    }
}

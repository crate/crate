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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import io.crate.expression.symbol.Symbol;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.expression.symbol.SymbolType;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataTypes;


public class IndexReference extends SimpleReference {



    public static class Builder {
        private final ReferenceIdent ident;
        private IndexType indexType = IndexType.FULLTEXT;
        private List<Reference> columns = new ArrayList<>();
        private String analyzer = null;
        private int position = 0;

        public Builder(ReferenceIdent ident) {
            requireNonNull(ident, "ident is null");
            this.ident = ident;
        }

        public Builder indexType(IndexType indexType) {
            this.indexType = indexType;
            return this;
        }

        public Builder addColumn(Reference info) {
            this.columns.add(info);
            return this;
        }

        public Builder analyzer(String name) {
            this.analyzer = name;
            return this;
        }

        public Builder position(int position) {
            this.position = position;
            return this;
        }

        public IndexReference build() {
            return new IndexReference(position, ident, indexType, columns, analyzer);
        }
    }

    @Nullable
    private final String analyzer;
    private final List<Reference> columns;

    public IndexReference(StreamInput in) throws IOException {
        super(in);
        analyzer = in.readOptionalString();
        int size = in.readVInt();
        columns = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            columns.add(Reference.fromStream(in));
        }
    }

    public IndexReference(int position,
                          ReferenceIdent ident,
                          IndexType indexType,
                          List<Reference> columns,
                          @Nullable String analyzer) {
        super(ident, RowGranularity.DOC, DataTypes.STRING, ColumnPolicy.DYNAMIC, indexType, false, true, position, null);
        this.columns = columns;
        this.analyzer = analyzer;
    }

    public IndexReference(int position,
                          boolean nullable,
                          boolean hasDocValues,
                          ReferenceIdent ident,
                          IndexType indexType,
                          List<Reference> columns,
                          @Nullable String analyzer) {
        super(ident, RowGranularity.DOC, DataTypes.STRING, ColumnPolicy.DYNAMIC, indexType, nullable, hasDocValues, position, null);
        this.columns = columns;
        this.analyzer = analyzer;
    }

    public IndexReference(ReferenceIdent ident,
                          RowGranularity granularity,
                          DataType<?> type,
                          ColumnPolicy columnPolicy,
                          IndexType indexType,
                          boolean nullable,
                          boolean hasDocValues,
                          int position,
                          long newOid,
                          Symbol defaultExpression,
                          List<Reference> columns,
                          String analyzer) {
        super(ident,
              granularity,
              type,
              columnPolicy,
              indexType,
              nullable,
              hasDocValues,
              position,
              newOid,
              defaultExpression
        );
        this.columns = columns;
        this.analyzer = analyzer;
    }

    public List<Reference> columns() {
        return columns;
    }

    @Nullable
    public String analyzer() {
        return analyzer;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.INDEX_REFERENCE;
    }

    @Override
    public Reference assignOid(long newOid) {
        return new IndexReference(
            ident,
            granularity,
            type,
            columnPolicy,
            indexType,
            nullable,
            hasDocValues,
            position,
            newOid,
            defaultExpression,
            columns,
            analyzer
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        IndexReference that = (IndexReference) o;
        return Objects.equals(analyzer, that.analyzer) &&
               Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), analyzer, columns);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(analyzer);
        out.writeVInt(columns.size());
        for (Reference reference : columns) {
            Reference.toStream(reference, out);
        }
    }

    @Override
    public Map<String, Object> toMapping() {
        Map<String, Object> mapping = super.toMapping();
        if (analyzer != null) {
            mapping.put("analyzer", analyzer);
            mapping.put("type", "text");
        }
        return mapping;
    }
}

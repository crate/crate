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
import java.util.stream.Collectors;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataType;
import io.crate.types.DataTypes;


public class IndexReference extends SimpleReference {

    public static class Builder {
        private final ReferenceIdent ident;
        private IndexType indexType = IndexType.FULLTEXT;

        @Deprecated
        private List<Reference> columns = new ArrayList<>();

        private String analyzer = null;
        private int position = 0;
        private long oid;

        // Temporal source names holder, real references resolved by names on build();
        private List<String> sourceNames = new ArrayList<>();

        public Builder(ReferenceIdent ident) {
            requireNonNull(ident, "ident is null");
            this.ident = ident;
        }

        public Builder indexType(IndexType indexType) {
            this.indexType = indexType;
            return this;
        }

        @Deprecated
        public Builder addColumn(Reference info) {
            this.columns.add(info);
            return this;
        }

        public Builder sources(List<String> sourceNames) {
            this.sourceNames = sourceNames;
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

        public Builder oid(long oid) {
            this.oid = oid;
            return this;
        }

        public IndexReference build(Map<ColumnIdent, Reference> references) {
            assert (columns.isEmpty() ^ sourceNames.isEmpty()) : "Only one of columns/sourceNames can be set.";
            if (columns.isEmpty() == false) {
                // columns is derived from copy_to which has been deprecated in 5.4.
                // When a node is upgraded it can have shards on older nodes with outdated mapping (still having copy_to and no sources).
                // This code handles outdated shards case.
                return new IndexReference(position, oid, ident, indexType, columns, analyzer);
            }
            List<Reference> sources = references.values().stream().filter(ref -> sourceNames.contains(ref.column().fqn())).collect(Collectors.toList());
            return new IndexReference(position, oid, ident, indexType, sources, analyzer);
        }
    }

    @Nullable
    private final String analyzer;
    @NotNull
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
                          long oid,
                          ReferenceIdent ident,
                          IndexType indexType,
                          List<Reference> columns,
                          @Nullable String analyzer) {
        super(ident, RowGranularity.DOC, DataTypes.STRING, ColumnPolicy.DYNAMIC, indexType, false, false, position, oid, null);
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
                          long oid,
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
              oid,
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
            Reference.toStream(out, reference);
        }
    }

    @Override
    public Reference getRelocated(ReferenceIdent newIdent) {
        return new IndexReference(
            newIdent,
            granularity,
            type,
            columnPolicy,
            indexType,
            nullable,
            hasDocValues,
            position,
            oid,
            defaultExpression,
            columns,
            analyzer
        );
    }

    @Override
    public Map<String, Object> toMapping(int position, @Nullable Metadata.ColumnOidSupplier columnOidSupplier) {
        Map<String, Object> mapping = super.toMapping(position, columnOidSupplier);
        if (analyzer != null) {
            mapping.put("analyzer", analyzer);
        }
        mapping.put("type", "text");

        if (columns.isEmpty() == false) {
            mapping.put("sources", columns.stream().map(ref -> ref.column().fqn()).toList());
        }

        return mapping;
    }
}

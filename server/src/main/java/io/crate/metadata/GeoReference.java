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
import static org.elasticsearch.index.mapper.GeoShapeFieldMapper.Names.TREE_BKD;
import static org.elasticsearch.index.mapper.GeoShapeFieldMapper.Names.TREE_GEOHASH;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Maps;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataType;

public class GeoReference extends SimpleReference {

    private final String geoTree;

    @Nullable
    private final String precision;

    @Nullable
    private final Integer treeLevels;

    @Nullable
    private final Double distanceErrorPct;

    public GeoReference(ReferenceIdent ident,
                        DataType<?> type,
                        ColumnPolicy columnPolicy,
                        IndexType indexType,
                        boolean nullable,
                        int position,
                        long oid,
                        boolean isDropped,
                        Symbol defaultExpression,
                        String geoTree,
                        String precision,
                        Integer treeLevels,
                        Double distanceErrorPct) {
        super(ident,
            RowGranularity.DOC, // Only primitive types columns can be used in PARTITIONED BY clause
            type,
            columnPolicy,
            indexType,
            nullable,
            false, //Geo shapes don't have doc values
            position,
            oid,
            isDropped,
            defaultExpression
        );
        this.geoTree = Objects.requireNonNullElse(geoTree, TREE_GEOHASH);
        if (TREE_BKD.equals(this.geoTree) && (precision != null || treeLevels != null || distanceErrorPct != null)) {
            throw new IllegalArgumentException(
                "The parameters precision, tree_levels, and distance_error_pct are not applicable to BKD tree indexes."
            );
        }
        this.precision = precision;
        this.treeLevels = treeLevels;
        this.distanceErrorPct = distanceErrorPct;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.GEO_REFERENCE;
    }

    public String geoTree() {
        return geoTree;
    }

    @Nullable
    public String precision() {
        return precision;
    }

    @Nullable
    public Integer treeLevels() {
        return treeLevels;
    }

    @Nullable
    public Double distanceErrorPct() {
        return distanceErrorPct;
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
        GeoReference that = (GeoReference) o;
        return java.util.Objects.equals(geoTree, that.geoTree) &&
               java.util.Objects.equals(precision, that.precision) &&
               java.util.Objects.equals(treeLevels, that.treeLevels) &&
               java.util.Objects.equals(distanceErrorPct, that.distanceErrorPct);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(super.hashCode(), geoTree, precision, treeLevels, distanceErrorPct);
    }

    public GeoReference(StreamInput in) throws IOException {
        super(in);
        geoTree = in.readString();
        precision = in.readOptionalString();
        treeLevels = in.readBoolean() ? null : in.readVInt();
        distanceErrorPct = in.readBoolean() ? null : in.readDouble();
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(geoTree);
        out.writeOptionalString(precision);
        out.writeBoolean(treeLevels == null);
        if (treeLevels != null) {
            out.writeVInt(treeLevels);
        }
        out.writeBoolean(distanceErrorPct == null);
        if (distanceErrorPct != null) {
            out.writeDouble(distanceErrorPct);
        }
    }

    @Override
    public Reference withReferenceIdent(ReferenceIdent newIdent) {
        return new GeoReference(
            newIdent,
            type,
            columnPolicy,
            indexType,
            nullable,
            position,
            oid,
            isDropped,
            defaultExpression,
            geoTree,
            precision,
            treeLevels,
            distanceErrorPct
        );
    }

    @Override
    public Reference withColumnOid(LongSupplier oidSupplier) {
        if (oid != COLUMN_OID_UNASSIGNED) {
            return this;
        }
        return new GeoReference(
                ident,
                type,
                columnPolicy,
                indexType,
                nullable,
                position,
                oidSupplier.getAsLong(),
                isDropped,
                defaultExpression,
                geoTree,
                precision,
                treeLevels,
                distanceErrorPct
        );
    }

    @Override
    public Map<String, Object> toMapping(int position) {
        Map<String, Object> mapping = super.toMapping(position);
        Maps.putNonNull(mapping, "tree", geoTree);
        Maps.putNonNull(mapping, "precision", precision);
        Maps.putNonNull(mapping, "tree_levels", treeLevels);
        if (distanceErrorPct != null) {
            mapping.put("distance_error_pct", distanceErrorPct.floatValue());
        }
        return mapping;
    }
}

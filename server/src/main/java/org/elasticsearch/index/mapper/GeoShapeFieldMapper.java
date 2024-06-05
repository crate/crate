/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.builders.ShapeBuilder.Orientation;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.jetbrains.annotations.Nullable;

/**
 * FieldMapper for indexing {@link org.locationtech.spatial4j.shape.Shape}s.
 * <p>
 * Currently Shapes can only be indexed and can only be queried using
 * {@link org.elasticsearch.index.query.GeoShapeQueryBuilder}, consequently
 * a lot of behavior in this Mapper is disabled.
 * <p>
 * Format supported:
 * <p>
 * "field" : {
 * "type" : "polygon",
 * "coordinates" : [
 * [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]
 * ]
 * }
 */
public class GeoShapeFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "geo_shape";
    public static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setStored(false);
        FIELD_TYPE.setStoreTermVectors(false);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.freeze();
    }

    public static class Names {
        public static final String TREE = "tree";
        public static final String TREE_GEOHASH = "geohash";
        public static final String TREE_QUADTREE = "quadtree";
        public static final String TREE_LEGACY_QUADTREE = "legacyquadtree";
        public static final String TREE_BKD = "bkdtree";
        public static final String TREE_LEVELS = "tree_levels";
        public static final String TREE_PRESISION = "precision";
        public static final String DISTANCE_ERROR_PCT = "distance_error_pct";
        public static final String ORIENTATION = "orientation";
    }

    public static final class Defaults {

        private Defaults() {}

        public static final String TREE = Names.TREE_GEOHASH;
        public static final int GEOHASH_LEVELS = GeoUtils.geoHashLevelsForPrecision("50m");
        public static final int QUADTREE_LEVELS = GeoUtils.quadTreeLevelsForPrecision("50m");
        public static final Orientation ORIENTATION = Orientation.RIGHT;
        public static final double LEGACY_DISTANCE_ERROR_PCT = 0.025d;
        public static final double DISTANCE_ERROR_PCT = 0.0;
    }

    public static class Builder extends FieldMapper.Builder {

        private String tree = Names.TREE_GEOHASH;
        private int treeLevels;
        private double precisionInMeters = -1;
        private Orientation orientation = Orientation.RIGHT;
        private Double distanceErrorPct = null;


        public Builder(String name) {
            super(name, FIELD_TYPE);
            this.hasDocValues = false;
        }


        @Override
        public GeoShapeFieldMapper build(BuilderContext context) {
            GeoShapeFieldType ft = new GeoShapeFieldType(
                buildFullName(context),
                indexed,
                hasDocValues,
                tree,
                treeLevels,
                precisionInMeters,
                distanceErrorPct,
                orientation
            );
            var mapper = new GeoShapeFieldMapper(
                name,
                position,
                columnOID,
                isDropped,
                defaultExpression,
                fieldType,
                ft,
                copyTo
            );
            context.putPositionInfo(mapper, position);
            return mapper;
        }


        public void setTree(String tree) {
            this.tree = tree;
        }

        public void setTreeLevels(int treeLevels) {
            this.treeLevels = treeLevels;
        }

        public void setPrecisionInMeters(double precisionInMeters) {
            this.precisionInMeters = precisionInMeters;
        }


        public void setDistanceErrorPct(double distanceErrorPct) {
            this.distanceErrorPct = distanceErrorPct;
        }


        public void setOrientation(Orientation orientation) {
            this.orientation = orientation;
        }
    }

    public static final class GeoShapeFieldType extends MappedFieldType {

        private String tree = Defaults.TREE;
        private int treeLevels = 0;
        private double precisionInMeters = -1;
        private Double distanceErrorPct;
        private Orientation orientation = Defaults.ORIENTATION;

        public GeoShapeFieldType(String name, boolean indexed, boolean hasDocValues) {
            super(name, indexed, hasDocValues);
        }

        public GeoShapeFieldType(String name,
                                 boolean indexed,
                                 boolean hasDocValues,
                                 String tree,
                                 int treeLevels,
                                 double precisionInMeters,
                                 Double distanceErrorPct,
                                 Orientation orientation) {
            this(name, indexed, hasDocValues);
            this.tree = tree;
            this.treeLevels = treeLevels;
            this.precisionInMeters = precisionInMeters;
            this.distanceErrorPct = distanceErrorPct;
            this.orientation = orientation;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }


        public String tree() {
            return tree;
        }

        public void setTree(String tree) {
            this.tree = tree;
        }

        public int treeLevels() {
            return treeLevels;
        }

        public double precisionInMeters() {
            return precisionInMeters;
        }

        public double defaultDistanceErrorPct() {
            return treeLevels == 0 && precisionInMeters < 0
                ? Defaults.LEGACY_DISTANCE_ERROR_PCT
                : Defaults.DISTANCE_ERROR_PCT;
        }

        public double distanceErrorPct() {
            return distanceErrorPct == null ? defaultDistanceErrorPct() : distanceErrorPct;
        }

        public Orientation orientation() {
            return this.orientation;
        }


    }

    public GeoShapeFieldMapper(String simpleName,
                               int position,
                               long columnOID,
                               boolean isDropped,
                               @Nullable String defaultExpression,
                               FieldType fieldType,
                               MappedFieldType mappedFieldType,
                               CopyTo copyTo) {
        super(simpleName, position, columnOID, isDropped, defaultExpression, fieldType, mappedFieldType, copyTo);
    }

    @Override
    public GeoShapeFieldType fieldType() {
        return (GeoShapeFieldType) super.fieldType();
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        GeoShapeFieldMapper gsfm = (GeoShapeFieldMapper) other;
        // prevent user from changing trees (changes encoding)
        if (fieldType().tree().equals(gsfm.fieldType().tree()) == false) {
            conflicts.add("mapper [" + name() + "] has different [tree]");
        }

        // TODO we should allow this, but at the moment levels is used to build bookkeeping variables
        // in lucene's SpatialPrefixTree implementations, need a patch to correct that first
        if (fieldType().treeLevels() != gsfm.fieldType().treeLevels()) {
            conflicts.add("mapper [" + name() + "] has different [tree_levels]");
        }
        if (fieldType().precisionInMeters() != gsfm.fieldType().precisionInMeters()) {
            conflicts.add("mapper [" + name() + "] has different [precision]");
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults) throws IOException {
        builder.field("type", contentType());

        if (includeDefaults || fieldType().tree().equals(Defaults.TREE) == false) {
            builder.field(Names.TREE, fieldType().tree());
        }
        if (position != NOT_TO_BE_POSITIONED) {
            builder.field("position", position);
        }
        if (columnOID != COLUMN_OID_UNASSIGNED) {
            builder.field("oid", columnOID);
        }
        if (isDropped) {
            builder.field("dropped", true);
        }
        if (fieldType().treeLevels() != 0) {
            builder.field(Names.TREE_LEVELS, fieldType().treeLevels());
        } else if (includeDefaults && fieldType().precisionInMeters() == -1) { // defaults only make sense if precision is not specified
            if ("geohash".equals(fieldType().tree())) {
                builder.field(Names.TREE_LEVELS, Defaults.GEOHASH_LEVELS);
            } else if ("legacyquadtree".equals(fieldType().tree())) {
                builder.field(Names.TREE_LEVELS, Defaults.QUADTREE_LEVELS);
            } else if ("quadtree".equals(fieldType().tree())) {
                builder.field(Names.TREE_LEVELS, Defaults.QUADTREE_LEVELS);
            } else {
                throw new IllegalArgumentException("Unknown prefix tree type [" + fieldType().tree() + "]");
            }
        }
        if (fieldType().precisionInMeters() != -1) {
            builder.field(Names.TREE_PRESISION, DistanceUnit.METERS.toString(fieldType().precisionInMeters()));
        } else if (includeDefaults && fieldType().treeLevels() == 0) { // defaults only make sense if tree levels are not specified
            builder.field(Names.TREE_PRESISION, DistanceUnit.METERS.toString(50));
        }
        if (includeDefaults || fieldType().distanceErrorPct() != fieldType().defaultDistanceErrorPct()) {
            builder.field(Names.DISTANCE_ERROR_PCT, fieldType().distanceErrorPct());
        }
        if (includeDefaults || fieldType().orientation() != Defaults.ORIENTATION) {
            builder.field(Names.ORIENTATION, fieldType().orientation());
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}

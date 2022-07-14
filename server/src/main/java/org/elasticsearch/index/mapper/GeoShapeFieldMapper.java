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

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeIntegerValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.TermQueryPrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.PackedQuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.XShapeCollection;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder.Orientation;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

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
        public static final String TREE_LEVELS = "tree_levels";
        public static final String TREE_PRESISION = "precision";
        public static final String DISTANCE_ERROR_PCT = "distance_error_pct";
        public static final String ORIENTATION = "orientation";
        public static final String STRATEGY = "strategy";
        public static final String STRATEGY_POINTS_ONLY = "points_only";
    }

    public static class Defaults {
        public static final String TREE = Names.TREE_GEOHASH;
        public static final String STRATEGY = SpatialStrategy.RECURSIVE.getStrategyName();
        public static final boolean POINTS_ONLY = false;
        public static final int GEOHASH_LEVELS = GeoUtils.geoHashLevelsForPrecision("50m");
        public static final int QUADTREE_LEVELS = GeoUtils.quadTreeLevelsForPrecision("50m");
        public static final Orientation ORIENTATION = Orientation.RIGHT;
        public static final double LEGACY_DISTANCE_ERROR_PCT = 0.025d;
        public static final double DISTANCE_ERROR_PCT = 0.0;
    }

    public static class Builder extends FieldMapper.Builder<Builder> {

        private String tree = Names.TREE_GEOHASH;
        private String strategyName = SpatialStrategy.RECURSIVE.getStrategyName();
        private boolean pointsOnly = false;
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
                strategyName,
                pointsOnly,
                treeLevels,
                precisionInMeters,
                distanceErrorPct,
                orientation
            );
            var mapper = new GeoShapeFieldMapper(
                name,
                position,
                defaultExpression,
                fieldType,
                ft,
                context.indexSettings(),
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


        public void setStrategyName(String strategyName) {
            this.strategyName = strategyName;
            if (this.strategyName.equals(SpatialStrategy.TERM.getStrategyName())) {
                this.pointsOnly = true;
            }
        }


        public void setPointsOnly(Boolean pointsOnly) {
            this.pointsOnly = pointsOnly;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);
            Boolean pointsOnly = null;
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (Names.TREE.equals(fieldName)) {
                    builder.setTree(fieldNode.toString());
                    iterator.remove();
                } else if (Names.TREE_LEVELS.equals(fieldName)) {
                    builder.setTreeLevels(Integer.parseInt(fieldNode.toString()));
                    iterator.remove();
                } else if (Names.TREE_PRESISION.equals(fieldName)) {
                    builder.setPrecisionInMeters(DistanceUnit.parse(fieldNode.toString(), DistanceUnit.DEFAULT, DistanceUnit.DEFAULT));
                    iterator.remove();
                } else if (Names.DISTANCE_ERROR_PCT.equals(fieldName)) {
                    builder.setDistanceErrorPct(Double.parseDouble(fieldNode.toString()));
                    iterator.remove();
                } else if (Names.ORIENTATION.equals(fieldName)) {
                    builder.setOrientation(ShapeBuilder.Orientation.fromString(fieldNode.toString()));
                    iterator.remove();
                } else if (Names.STRATEGY.equals(fieldName)) {
                    builder.setStrategyName(fieldNode.toString());
                    iterator.remove();
                } else if (Names.STRATEGY_POINTS_ONLY.equals(fieldName)) {
                    pointsOnly = nodeBooleanValue(fieldNode, fieldName + '.' + Names.STRATEGY_POINTS_ONLY);
                    iterator.remove();
                } else if ("position".equals(fieldName)) {
                    builder.position(nodeIntegerValue(fieldNode));
                    iterator.remove();
                }
            }
            if (pointsOnly != null) {
                if (builder.strategyName.equals(SpatialStrategy.TERM.getStrategyName()) && pointsOnly == false) {
                    throw new IllegalArgumentException("points_only cannot be set to false for term strategy");
                } else {
                    builder.setPointsOnly(pointsOnly);
                }
            }
            return builder;
        }
    }

    public static final class GeoShapeFieldType extends MappedFieldType {

        private String tree = Defaults.TREE;
        private String strategyName = Defaults.STRATEGY;
        private boolean pointsOnly = Defaults.POINTS_ONLY;
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
                                 String strategyName,
                                 boolean pointsOnly,
                                 int treeLevels,
                                 double precisionInMeters,
                                 Double distanceErrorPct,
                                 Orientation orientation) {
            this(name, indexed, hasDocValues);
            this.tree = tree;
            this.strategyName = strategyName;
            this.pointsOnly = pointsOnly;
            this.treeLevels = treeLevels;
            this.precisionInMeters = precisionInMeters;
            this.distanceErrorPct = distanceErrorPct;
            this.orientation = orientation;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        private static int getLevels(int treeLevels, double precisionInMeters, int defaultLevels, boolean geoHash) {
            if (treeLevels > 0 || precisionInMeters >= 0) {
                return Math.max(treeLevels, precisionInMeters >= 0 ? (geoHash ? GeoUtils.geoHashLevelsForPrecision(precisionInMeters)
                    : GeoUtils.quadTreeLevelsForPrecision(precisionInMeters)) : 0);
            }
            return defaultLevels;
        }

        public String tree() {
            return tree;
        }

        public void setTree(String tree) {
            this.tree = tree;
        }

        public String strategyName() {
            return strategyName;
        }

        public boolean pointsOnly() {
            return pointsOnly;
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

        public PrefixTreeStrategy defaultStrategy() {
            if (SpatialStrategy.RECURSIVE.getStrategyName().equals(strategyName)) {
                var recursiveStrategy = new RecursivePrefixTreeStrategy(prefixTree(), name());
                recursiveStrategy.setDistErrPct(distanceErrorPct());
                recursiveStrategy.setPruneLeafyBranches(false);
                recursiveStrategy.setPointsOnly(pointsOnly);
                return recursiveStrategy;
            }
            if (SpatialStrategy.TERM.getStrategyName().equals(strategyName)) {
                var termStrategy = new TermQueryPrefixTreeStrategy(prefixTree(), name());
                termStrategy.setDistErrPct(distanceErrorPct());
                termStrategy.setPointsOnly(pointsOnly);
                return termStrategy;
            }
            throw new IllegalArgumentException("Unknown prefix tree strategy [" + strategyName + "]");
        }

        private SpatialPrefixTree prefixTree() {
            return switch (tree) {
                case "geohash" -> new GeohashPrefixTree(
                    ShapeBuilder.SPATIAL_CONTEXT,
                    getLevels(treeLevels, precisionInMeters, Defaults.GEOHASH_LEVELS, true)
                );

                case "legacyquadtree" -> new QuadPrefixTree(
                    ShapeBuilder.SPATIAL_CONTEXT,
                    getLevels(treeLevels, precisionInMeters, Defaults.QUADTREE_LEVELS, false)
                );

                case "quadtree" -> new PackedQuadPrefixTree(
                    ShapeBuilder.SPATIAL_CONTEXT,
                    getLevels(treeLevels, precisionInMeters, Defaults.QUADTREE_LEVELS, false)
                );

                default -> throw new IllegalArgumentException("Unknown prefix tree type: " + tree);
            };
        }

    }

    public GeoShapeFieldMapper(String simpleName,
                               int position,
                               @Nullable String defaultExpression,
                               FieldType fieldType,
                               MappedFieldType mappedFieldType,
                               Settings indexSettings,
                               CopyTo copyTo) {
        super(simpleName, position, defaultExpression, fieldType, mappedFieldType, indexSettings, copyTo);
    }

    @Override
    public GeoShapeFieldType fieldType() {
        return (GeoShapeFieldType) super.fieldType();
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        try {
            Shape shape;
            ShapeBuilder shapeBuilder = ShapeParser.parse(context.parser(), this);
            if (shapeBuilder == null) {
                return;
            }
            shape = shapeBuilder.build();
            if (fieldType().pointsOnly() == true) {
                // index configured for pointsOnly
                if (shape instanceof XShapeCollection && XShapeCollection.class.cast(shape).pointsOnly()) {
                    // MULTIPOINT data: index each point separately
                    List<Shape> shapes = ((XShapeCollection) shape).getShapes();
                    for (Shape s : shapes) {
                        indexShape(context, s);
                    }
                    return;
                } else if (shape instanceof Point == false) {
                    throw new MapperParsingException("[{" + fieldType().name() + "}] is configured for points only but a " +
                        ((shape instanceof JtsGeometry) ? ((JtsGeometry)shape).getGeom().getGeometryType() : shape.getClass()) + " was found");
                }
            }
            indexShape(context, shape);
        } catch (Exception e) {
            throw new MapperParsingException("failed to parse field [{}] of type [{}]", e, fieldType().name(),
                    fieldType().typeName());
        }
    }

    private void indexShape(ParseContext context, Shape shape) {
        Field[] indexableFields = fieldType().defaultStrategy().createIndexableFields(shape);
        List<IndexableField> fields = new ArrayList<>(Arrays.asList(indexableFields));
        createFieldNamesField(context, fields);
        for (IndexableField field : fields) {
            context.doc().add(field);
        }
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        GeoShapeFieldMapper gsfm = (GeoShapeFieldMapper) other;
        // prevent user from changing strategies
        if (fieldType().strategyName().equals(gsfm.fieldType().strategyName()) == false) {
            conflicts.add("mapper [" + name() + "] has different [strategy]");
        }
        // prevent user from changing trees (changes encoding)
        if (fieldType().tree().equals(gsfm.fieldType().tree()) == false) {
            conflicts.add("mapper [" + name() + "] has different [tree]");
        }

        if ((fieldType().pointsOnly() != gsfm.fieldType().pointsOnly())) {
            conflicts.add("mapper [" + name() + "] has different points_only");
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
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        builder.field("type", contentType());

        if (includeDefaults || fieldType().tree().equals(Defaults.TREE) == false) {
            builder.field(Names.TREE, fieldType().tree());
        }
        if (position != NOT_TO_BE_POSITIONED) {
            builder.field("position", position);
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
        if (includeDefaults || fieldType().strategyName().equals(Defaults.STRATEGY) == false) {
            builder.field(Names.STRATEGY, fieldType().strategyName());
        }
        if (includeDefaults || fieldType().distanceErrorPct() != fieldType().defaultDistanceErrorPct()) {
            builder.field(Names.DISTANCE_ERROR_PCT, fieldType().distanceErrorPct());
        }
        if (includeDefaults || fieldType().orientation() != Defaults.ORIENTATION) {
            builder.field(Names.ORIENTATION, fieldType().orientation());
        }
        if (fieldType().strategyName().equals(SpatialStrategy.TERM.getStrategyName())) {
            // For TERMs strategy the defaults for points only change to true
            if (includeDefaults || fieldType().pointsOnly() != true) {
                builder.field(Names.STRATEGY_POINTS_ONLY, fieldType().pointsOnly());
            }
        } else {
            if (includeDefaults || fieldType().pointsOnly() != GeoShapeFieldMapper.Defaults.POINTS_ONLY) {
                builder.field(Names.STRATEGY_POINTS_ONLY, fieldType().pointsOnly());
            }
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}

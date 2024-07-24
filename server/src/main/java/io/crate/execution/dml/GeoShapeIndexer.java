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

package io.crate.execution.dml;

import static io.crate.types.GeoShapeType.Names.TREE_BKD;
import static io.crate.types.GeoShapeType.Names.TREE_GEOHASH;
import static io.crate.types.GeoShapeType.Names.TREE_LEGACY_QUADTREE;
import static io.crate.types.GeoShapeType.Names.TREE_QUADTREE;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.PackedQuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder.Orientation;
import org.elasticsearch.common.unit.DistanceUnit;
import org.jetbrains.annotations.NotNull;
import org.locationtech.spatial4j.shape.Shape;

import io.crate.execution.dml.Indexer.ColumnConstraint;
import io.crate.geo.GeoJSONUtils;
import io.crate.geo.LatLonShapeUtils;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.GeoReference;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.types.GeoShapeType.Names;

public class GeoShapeIndexer implements ValueIndexer<Map<String, Object>> {

    private final IndexableFieldsFactory indexableFieldsFactory;
    private final Reference ref;
    private final String name;

    public static final class Defaults {

        private Defaults() {
        }

        public static final String TREE = Names.TREE_GEOHASH;
        public static final int GEOHASH_LEVELS = GeoUtils.geoHashLevelsForPrecision("50m");
        public static final int QUADTREE_LEVELS = GeoUtils.quadTreeLevelsForPrecision("50m");
        public static final Orientation ORIENTATION = Orientation.RIGHT;
        public static final double LEGACY_DISTANCE_ERROR_PCT = 0.025d;
        public static final double DISTANCE_ERROR_PCT = 0.0;
    }

    public GeoShapeIndexer(Reference ref) {
        if (ref instanceof GeneratedReference generatedRef) {
            ref = generatedRef.reference();
        }
        assert ref instanceof GeoReference : "GeoShapeIndexer requires GeoReference";
        GeoReference geoReference = (GeoReference) ref;
        this.name = ref.storageIdent();
        if (TREE_BKD.equals(geoReference.geoTree())) {
            this.indexableFieldsFactory = new BkdTreeIndexableFieldsFactory(name);
        } else {
            this.indexableFieldsFactory = new PrefixTreeIndexableFieldsFactory(geoReference);
        }
        this.ref = ref;
    }

    @Override
    public void indexValue(@NotNull Map<String, Object> value,
                           Consumer<? super IndexableField> addField,
                           TranslogWriter translogWriter,
                           Synthetics synthetics,
                           Map<ColumnIdent, ColumnConstraint> toValidate) throws IOException {
        indexableFieldsFactory.create(value, addField);
        addField.accept(new Field(
            DocSysColumns.FieldNames.NAME,
            name,
            DocSysColumns.FieldNames.FIELD_TYPE));
        translogWriter.writeValue(value);
    }

    @Override
    public String storageIdentLeafName() {
        return ref.storageIdentLeafName();
    }

    private interface IndexableFieldsFactory {

        void create(Map<String, Object> value, Consumer<? super IndexableField> addField);
    }

    private static class PrefixTreeIndexableFieldsFactory implements IndexableFieldsFactory {

        private final RecursivePrefixTreeStrategy strategy;

        PrefixTreeIndexableFieldsFactory(GeoReference ref) {
            this.strategy = new RecursivePrefixTreeStrategy(prefixTree(ref), ref.storageIdent());
            Double distanceErrorPct = ref.distanceErrorPct();
            if (distanceErrorPct != null) {
                this.strategy.setDistErrPct(distanceErrorPct);
            }
            this.strategy.setPruneLeafyBranches(false);
        }

        @Override
        public void create(Map<String, Object> value, Consumer<? super IndexableField> addField) {
            Shape shape = GeoJSONUtils.map2Shape(value);
            Field[] fields = strategy.createIndexableFields(shape);
            for (var field : fields) {
                addField.accept(field);
            }
        }

        private SpatialPrefixTree prefixTree(GeoReference ref) {
            double precisionInMeters = ref.precision() == null ? -1 : DistanceUnit.parse(
                ref.precision(),
                DistanceUnit.DEFAULT,
                DistanceUnit.METERS
            );
            int treeLevels = ref.treeLevels() == null ? 0 : ref.treeLevels();
            return switch (ref.geoTree()) {
                case TREE_GEOHASH -> new GeohashPrefixTree(
                    ShapeBuilder.SPATIAL_CONTEXT,
                    levels(treeLevels, precisionInMeters, Defaults.GEOHASH_LEVELS, true)
                );

                case TREE_LEGACY_QUADTREE -> new QuadPrefixTree(
                    ShapeBuilder.SPATIAL_CONTEXT,
                    levels(treeLevels, precisionInMeters, Defaults.QUADTREE_LEVELS, false)
                );

                case TREE_QUADTREE -> new PackedQuadPrefixTree(
                    ShapeBuilder.SPATIAL_CONTEXT,
                    levels(treeLevels, precisionInMeters, Defaults.QUADTREE_LEVELS, false)
                );

                default -> throw new IllegalArgumentException("Unknown prefix tree type: " + ref.geoTree());
            };
        }

        private int levels(int treeLevels, double precisionInMeters, int defaultLevels, boolean geoHash) {
            if (treeLevels > 0 || precisionInMeters >= 0) {
                int levels = geoHash
                    ? GeoUtils.geoHashLevelsForPrecision(precisionInMeters)
                    : GeoUtils.quadTreeLevelsForPrecision(precisionInMeters);
                return Math.max(treeLevels, precisionInMeters >= 0 ? levels : 0);
            }
            return defaultLevels;
        }
    }

    private static class BkdTreeIndexableFieldsFactory implements IndexableFieldsFactory {

        private final String name;

        BkdTreeIndexableFieldsFactory(String name) {
            this.name = name;
        }

        @Override
        public void create(Map<String, Object> value, Consumer<? super IndexableField> addField) {
            Object shape = GeoJSONUtils.map2LuceneShape(value);
            LatLonShapeUtils.createIndexableFields(name, shape, addField);
        }
    }
}

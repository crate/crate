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

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.locationtech.spatial4j.shape.Shape;

import io.crate.execution.dml.Indexer.ColumnConstraint;
import io.crate.execution.dml.Indexer.Synthetic;
import io.crate.geo.GeoJSONUtils;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeoReference;
import io.crate.metadata.Reference;

public class GeoShapeIndexer implements ValueIndexer<Map<String, Object>> {

    private final GeoReference ref;
    private final String name;
    private final RecursivePrefixTreeStrategy strategy;

    public GeoShapeIndexer(Reference ref, FieldType fieldType) {
        assert ref instanceof GeoReference : "GeoShapeIndexer requires GeoReference";
        this.ref = (GeoReference) ref;
        this.name = ref.column().fqn();
        this.strategy = new RecursivePrefixTreeStrategy(this.ref.prefixTree(), name);
        Double distanceErrorPct = this.ref.distanceErrorPct();
        if (distanceErrorPct != null) {
            this.strategy.setDistErrPct(distanceErrorPct);
        }
        this.strategy.setPruneLeafyBranches(false);
    }

    @Override
    public void indexValue(Map<String, Object> value,
                           XContentBuilder xcontentBuilder,
                           Consumer<? super IndexableField> addField,
                           Map<ColumnIdent, Synthetic> synthetics,
                           Map<ColumnIdent, ColumnConstraint> toValidate,
                           Function<Reference, String> columnKeyProvider) throws IOException {
        xcontentBuilder.map(value);
        Shape shape = GeoJSONUtils.map2Shape(value);
        Field[] fields = strategy.createIndexableFields(shape);
        for (var field : fields) {
            addField.accept(field);
        }
        addField.accept(new Field(
            FieldNamesFieldMapper.NAME,
            name,
            FieldNamesFieldMapper.Defaults.FIELD_TYPE));
    }
}

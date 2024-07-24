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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.jetbrains.annotations.NotNull;
import org.locationtech.spatial4j.shape.Point;

import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;

public class GeoPointIndexer implements ValueIndexer<Point> {

    private final Reference ref;
    private final String name;

    public GeoPointIndexer(Reference ref) {
        this.ref = ref;
        this.name = ref.storageIdent();
    }

    @Override
    public void indexValue(@NotNull Point point, IndexDocumentBuilder docBuilder) throws IOException {

        if (ref.indexType() != IndexType.NONE) {
            docBuilder.addField(new LatLonPoint(name, point.getLat(), point.getLon()));
        }
        if (ref.hasDocValues()) {
            docBuilder.addField(new LatLonDocValuesField(name, point.getLat(), point.getLon()));
        } else {
            docBuilder.addField(new Field(
                DocSysColumns.FieldNames.NAME,
                name,
                DocSysColumns.FieldNames.FIELD_TYPE));
        }
        docBuilder.translogWriter().startArray();
        docBuilder.translogWriter().writeValue(point.getX());
        docBuilder.translogWriter().writeValue(point.getY());
        docBuilder.translogWriter().endArray();
    }

    @Override
    public String storageIdentLeafName() {
        return ref.storageIdentLeafName();
    }
}

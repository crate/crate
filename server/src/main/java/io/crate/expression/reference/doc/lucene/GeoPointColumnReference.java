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

package io.crate.expression.reference.doc.lucene;

import java.io.IOException;
import java.io.UncheckedIOException;

import io.crate.execution.engine.fetch.ReaderContext;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.impl.PointImpl;

import io.crate.exceptions.unscoped.ArrayViaDocValuesUnsupportedException;

public class GeoPointColumnReference extends LuceneCollectorExpression<Point> {

    private final String columnName;
    private SortedNumericDocValues values;
    private int docId;

    public GeoPointColumnReference(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public Point value() {
        try {
            if (values.advanceExact(docId)) {
                switch (values.docValueCount()) {
                    case 1:
                        long encoded = values.nextValue();
                        return new PointImpl(
                            GeoEncodingUtils.decodeLongitude((int) encoded),
                            GeoEncodingUtils.decodeLatitude((int) (encoded >>> 32)),
                            JtsSpatialContext.GEO
                        );

                    default:
                        throw new ArrayViaDocValuesUnsupportedException(columnName);
                }
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setNextDocId(int docId) {
        this.docId = docId;
    }

    @Override
    public void setNextReader(ReaderContext context) throws IOException {
        super.setNextReader(context);
        values = DocValues.getSortedNumeric(context.reader(), columnName);
    }
}

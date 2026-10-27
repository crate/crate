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

package io.crate.server.xcontent;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderExtension;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.impl.PointImpl;
import org.locationtech.spatial4j.shape.jts.JtsPoint;

import io.crate.common.unit.TimeValue;
import io.crate.data.RowN;
import io.crate.sql.tree.BitString;
import io.crate.types.IntervalType;
import io.crate.types.Regclass;
import io.crate.types.Regproc;
import io.crate.types.Regtype;
import io.crate.types.TimeTZ;

/**
 * SPI extensions for ES/CrateDB-specific classes (like the Lucene or Joda
 * dependency classes) that need to be encoded by {@link XContentBuilder} in a
 * specific way.
 */
public class ServerXContentExtension implements XContentBuilderExtension {

    @Override
    public Map<Class<?>, XContentBuilder.Writer> getXContentWriters() {
        Map<Class<?>, XContentBuilder.Writer> writers = new HashMap<>();

        // Fully-qualified here to reduce ambiguity around our (ES') Version class
        writers.put(org.apache.lucene.util.Version.class, (b, v) -> b.value(Objects.toString(v)));
        writers.put(TimeValue.class, (b, v) -> b.value(v.toString()));
        writers.put(org.joda.time.Period.class, (b, v) -> {
            org.joda.time.Period period = (org.joda.time.Period) v;
            b.value(IntervalType.PERIOD_FORMATTER.print(period));
        });
        writers.put(BytesReference.class, (b, v) -> {
            if (v == null) {
                b.nullValue();
            } else {
                BytesRef bytes = ((BytesReference) v).toBytesRef();
                b.value(bytes.bytes, bytes.offset, bytes.length);
            }
        });
        writers.put(BytesRef.class, (b, v) -> {
            if (v == null) {
                b.nullValue();
            } else {
                BytesRef bytes = (BytesRef) v;
                b.value(bytes.bytes, bytes.offset, bytes.length);
            }
        });
        writers.put(Regproc.class, (b, v) -> b.value(((Regproc) v).name()));
        writers.put(Regclass.class, (b, v) -> b.value(((Regclass) v).oid()));
        writers.put(Regtype.class, (b, v) -> b.value(((Regtype) v).name()));
        writers.put(PointImpl.class, (b, v) -> {
            Point point = (Point) v;
            b.startArray();
            b.value(point.getX());
            b.value(point.getY());
            b.endArray();
        });
        writers.put(JtsPoint.class, (b, v) -> {
            Point point = (Point) v;
            b.startArray();
            b.value(point.getX());
            b.value(point.getY());
            b.endArray();
        });
        writers.put(RowN.class, (b, v) -> {
            RowN row = (RowN) v;
            b.startArray();
            for (int i = 0; i < row.numColumns(); i++) {
                b.value(row.get(i));
            }
            b.endArray();
        });
        writers.put(TimeTZ.class, (b, v) -> {
            TimeTZ timetz = (TimeTZ) v;
            b.startArray();
            b.value(timetz.getMicrosFromMidnight());
            b.value(timetz.getSecondsFromUTC());
            b.endArray();
        });
        writers.put(BitString.class, (b, v) -> {
            BitString bitString = (BitString) v;
            b.value(bitString.asPrefixedBitString());
        });
        return writers;
    }
}

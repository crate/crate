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

package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParserHelper;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range.FROM_FIELD;
import static org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range.KEY_FIELD;
import static org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range.TO_FIELD;

public class GeoDistanceAggregationBuilder extends ValuesSourceAggregationBuilder<ValuesSource.GeoPoint, GeoDistanceAggregationBuilder> {
    public static final String NAME = "geo_distance";
    static final ParseField ORIGIN_FIELD = new ParseField("origin", "center", "point", "por");
    static final ParseField UNIT_FIELD = new ParseField("unit");
    static final ParseField DISTANCE_TYPE_FIELD = new ParseField("distance_type");

    private static final ObjectParser<GeoDistanceAggregationBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(GeoDistanceAggregationBuilder.NAME);
        ValuesSourceParserHelper.declareGeoFields(PARSER, true, false);

        PARSER.declareBoolean(GeoDistanceAggregationBuilder::keyed, RangeAggregator.KEYED_FIELD);

        PARSER.declareObjectArray((agg, ranges) -> {
            for (Range range : ranges) {
                agg.addRange(range);
            }
        }, (p, c) -> GeoDistanceAggregationBuilder.parseRange(p), RangeAggregator.RANGES_FIELD);

        PARSER.declareField(GeoDistanceAggregationBuilder::unit, p -> DistanceUnit.fromString(p.text()),
                UNIT_FIELD, ObjectParser.ValueType.STRING);

        PARSER.declareField(GeoDistanceAggregationBuilder::distanceType, p -> GeoDistance.fromString(p.text()),
                DISTANCE_TYPE_FIELD, ObjectParser.ValueType.STRING);

        PARSER.declareField(GeoDistanceAggregationBuilder::origin, GeoDistanceAggregationBuilder::parseGeoPoint,
                ORIGIN_FIELD, ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING);
    }

    public static AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        GeoDistanceAggregationBuilder builder = PARSER.parse(parser, new GeoDistanceAggregationBuilder(aggregationName), null);
        if (builder.origin() == null) {
            throw new IllegalArgumentException("Aggregation [" + aggregationName + "] must define an [origin].");
        }
        return builder;
    }

    public static class Range extends RangeAggregator.Range {
        public Range(String key, Double from, Double to) {
            super(key(key, from, to), from == null ? 0 : from, to);
        }

        /**
         * Read from a stream.
         */
        public Range(StreamInput in) throws IOException {
            super(in.readOptionalString(), in.readDouble(), in.readDouble());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(key);
            out.writeDouble(from);
            out.writeDouble(to);
        }

        private static String key(String key, Double from, Double to) {
            if (key != null) {
                return key;
            }
            StringBuilder sb = new StringBuilder();
            sb.append((from == null || from == 0) ? "*" : from);
            sb.append("-");
            sb.append((to == null || Double.isInfinite(to)) ? "*" : to);
            return sb.toString();
        }
    }

    private static GeoPoint parseGeoPoint(XContentParser parser) throws IOException {
        Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            GeoPoint point = new GeoPoint();
            point.resetFromString(parser.text());
            return point;
        }
        if (token == XContentParser.Token.START_ARRAY) {
            double lat = Double.NaN;
            double lon = Double.NaN;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (Double.isNaN(lon)) {
                    lon = parser.doubleValue();
                } else if (Double.isNaN(lat)) {
                    lat = parser.doubleValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "malformed [" + ORIGIN_FIELD.getPreferredName()
                        + "]: a geo point array must be of the form [lon, lat]");
                }
            }
            return new GeoPoint(lat, lon);
        }
        if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            double lat = Double.NaN;
            double lon = Double.NaN;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    if ("lat".equals(currentFieldName)) {
                        lat = parser.doubleValue();
                    } else if ("lon".equals(currentFieldName)) {
                        lon = parser.doubleValue();
                    }
                }
            }
            if (Double.isNaN(lat) || Double.isNaN(lon)) {
                throw new ParsingException(parser.getTokenLocation(),
                        "malformed [" + currentFieldName + "] geo point object. either [lat] or [lon] (or both) are " + "missing");
            }
            return new GeoPoint(lat, lon);
        }

        // should not happen since we only parse geo points when we encounter a string, an object or an array
        throw new IllegalArgumentException("Unexpected token [" + token + "] while parsing geo point");
    }

    private static Range parseRange(XContentParser parser) throws IOException {
        String fromAsStr = null;
        String toAsStr = null;
        double from = 0.0;
        double to = Double.POSITIVE_INFINITY;
        String key = null;
        String currentFieldName = null;
        Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (FROM_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    from = parser.doubleValue();
                } else if (TO_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    to = parser.doubleValue();
                } else {
                    XContentParserUtils.throwUnknownField(currentFieldName, parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (KEY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    key = parser.text();
                } else if (FROM_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fromAsStr = parser.text();
                } else if (TO_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    toAsStr = parser.text();
                } else {
                    XContentParserUtils.throwUnknownField(currentFieldName, parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.VALUE_NULL) {
                if (FROM_FIELD.match(currentFieldName, parser.getDeprecationHandler())
                    || TO_FIELD.match(currentFieldName, parser.getDeprecationHandler())
                    || KEY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    // ignore null value
                } else {
                    XContentParserUtils.throwUnknownField(currentFieldName, parser.getTokenLocation());
                }
            } else {
                XContentParserUtils.throwUnknownToken(token, parser.getTokenLocation());
            }
        }
        if (fromAsStr != null || toAsStr != null) {
            return new Range(key, Double.parseDouble(fromAsStr), Double.parseDouble(toAsStr));
        } else {
            return new Range(key, from, to);
        }
    }

    private GeoPoint origin;
    private List<Range> ranges = new ArrayList<>();
    private DistanceUnit unit = DistanceUnit.DEFAULT;
    private GeoDistance distanceType = GeoDistance.ARC;
    private boolean keyed = false;

    public GeoDistanceAggregationBuilder(String name, GeoPoint origin) {
        this(name, origin, InternalGeoDistance.FACTORY);
        if (origin == null) {
            throw new IllegalArgumentException("[origin] must not be null: [" + name + "]");
        }
    }

    private GeoDistanceAggregationBuilder(String name, GeoPoint origin,
                                          InternalRange.Factory<InternalGeoDistance.Bucket, InternalGeoDistance> rangeFactory) {
        super(name, rangeFactory.getValueSourceType(), rangeFactory.getValueType());
        this.origin = origin;
    }

    /**
     * Read from a stream.
     */
    public GeoDistanceAggregationBuilder(StreamInput in) throws IOException {
        super(in, InternalGeoDistance.FACTORY.getValueSourceType(), InternalGeoDistance.FACTORY.getValueType());
        origin = new GeoPoint(in.readDouble(), in.readDouble());
        int size = in.readVInt();
        ranges = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            ranges.add(new Range(in));
        }
        keyed = in.readBoolean();
        distanceType = GeoDistance.readFromStream(in);
        unit = DistanceUnit.readFromStream(in);
    }

    // for parsing
    GeoDistanceAggregationBuilder(String name) {
        this(name, null, InternalGeoDistance.FACTORY);
    }

    protected GeoDistanceAggregationBuilder(GeoDistanceAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
        this.origin = clone.origin;
        this.distanceType = clone.distanceType;
        this.unit = clone.unit;
        this.keyed = clone.keyed;
        this.ranges = new ArrayList<>(clone.ranges);
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metaData) {
        return new GeoDistanceAggregationBuilder(this, factoriesBuilder, metaData);
    }

    GeoDistanceAggregationBuilder origin(GeoPoint origin) {
        this.origin = origin;
        return this;
    }

    /**
     * Return the {@link GeoPoint} that is used for distance computations.
     */
    public GeoPoint origin() {
        return origin;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(origin.lat());
        out.writeDouble(origin.lon());
        out.writeVInt(ranges.size());
        for (Range range : ranges) {
            range.writeTo(out);
        }
        out.writeBoolean(keyed);
        distanceType.writeTo(out);
        unit.writeTo(out);
    }

    public GeoDistanceAggregationBuilder addRange(Range range) {
        if (range == null) {
            throw new IllegalArgumentException("[range] must not be null: [" + name + "]");
        }
        ranges.add(range);
        return this;
    }

    /**
     * Add a new range to this aggregation.
     *
     * @param key
     *            the key to use for this range in the response
     * @param from
     *            the lower bound on the distances, inclusive
     * @param to
     *            the upper bound on the distances, exclusive
     */
    public GeoDistanceAggregationBuilder addRange(String key, double from, double to) {
        ranges.add(new Range(key, from, to));
        return this;
    }

    /**
     * Same as {@link #addRange(String, double, double)} but the key will be
     * automatically generated based on <code>from</code> and
     * <code>to</code>.
     */
    public GeoDistanceAggregationBuilder addRange(double from, double to) {
        return addRange(null, from, to);
    }

    /**
     * Add a new range with no lower bound.
     *
     * @param key
     *            the key to use for this range in the response
     * @param to
     *            the upper bound on the distances, exclusive
     */
    public GeoDistanceAggregationBuilder addUnboundedTo(String key, double to) {
        ranges.add(new Range(key, null, to));
        return this;
    }

    /**
     * Same as {@link #addUnboundedTo(String, double)} but the key will be
     * computed automatically.
     */
    public GeoDistanceAggregationBuilder addUnboundedTo(double to) {
        return addUnboundedTo(null, to);
    }

    /**
     * Add a new range with no upper bound.
     *
     * @param key
     *            the key to use for this range in the response
     * @param from
     *            the lower bound on the distances, inclusive
     */
    public GeoDistanceAggregationBuilder addUnboundedFrom(String key, double from) {
        addRange(new Range(key, from, null));
        return this;
    }

    /**
     * Same as {@link #addUnboundedFrom(String, double)} but the key will be
     * computed automatically.
     */
    public GeoDistanceAggregationBuilder addUnboundedFrom(double from) {
        return addUnboundedFrom(null, from);
    }

    public List<Range> range() {
        return ranges;
    }

    @Override
    public String getType() {
        return NAME;
    }

    public GeoDistanceAggregationBuilder unit(DistanceUnit unit) {
        if (unit == null) {
            throw new IllegalArgumentException("[unit] must not be null: [" + name + "]");
        }
        this.unit = unit;
        return this;
    }

    public DistanceUnit unit() {
        return unit;
    }

    public GeoDistanceAggregationBuilder distanceType(GeoDistance distanceType) {
        if (distanceType == null) {
            throw new IllegalArgumentException("[distanceType] must not be null: [" + name + "]");
        }
        this.distanceType = distanceType;
        return this;
    }

    public GeoDistance distanceType() {
        return distanceType;
    }

    public GeoDistanceAggregationBuilder keyed(boolean keyed) {
        this.keyed = keyed;
        return this;
    }

    public boolean keyed() {
        return keyed;
    }

    @Override
    protected ValuesSourceAggregatorFactory<ValuesSource.GeoPoint, ?> innerBuild(SearchContext context,
            ValuesSourceConfig<ValuesSource.GeoPoint> config, AggregatorFactory<?> parent, Builder subFactoriesBuilder)
                    throws IOException {
        Range[] ranges = this.ranges.toArray(new Range[this.range().size()]);
        if (ranges.length == 0) {
            throw new IllegalArgumentException("No [ranges] specified for the [" + this.getName() + "] aggregation");
        }
        return new GeoDistanceRangeAggregatorFactory(name, config, origin, ranges, unit, distanceType, keyed, context, parent,
                subFactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(ORIGIN_FIELD.getPreferredName(), origin);
        builder.field(RangeAggregator.RANGES_FIELD.getPreferredName(), ranges);
        builder.field(RangeAggregator.KEYED_FIELD.getPreferredName(), keyed);
        builder.field(UNIT_FIELD.getPreferredName(), unit);
        builder.field(DISTANCE_TYPE_FIELD.getPreferredName(), distanceType);
        return builder;
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(origin, ranges, keyed, distanceType, unit);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        GeoDistanceAggregationBuilder other = (GeoDistanceAggregationBuilder) obj;
        return Objects.equals(origin, other.origin)
                && Objects.equals(ranges, other.ranges)
                && Objects.equals(keyed, other.keyed)
                && Objects.equals(distanceType, other.distanceType)
                && Objects.equals(unit, other.unit);
    }

}

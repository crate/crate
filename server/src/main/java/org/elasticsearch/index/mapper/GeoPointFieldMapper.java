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

import static org.elasticsearch.index.mapper.TypeParsers.parseField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

/**
 * Field Mapper for geo_point types.
 *
 * Uses lucene 6 LatLonPoint encoding
 */
public class GeoPointFieldMapper extends FieldMapper implements ArrayValueMapperParser {
    public static final String CONTENT_TYPE = "geo_point";

    public static class Defaults {
        public static final GeoPointFieldType FIELD_TYPE = new GeoPointFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.setDimensions(2, Integer.BYTES);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, GeoPointFieldMapper> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public GeoPointFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new GeoPointFieldMapper(
                name,
                position,
                defaultExpression,
                fieldType,
                defaultFieldType,
                context.indexSettings(),
                multiFieldsBuilder.build(this, context),
                copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        @SuppressWarnings("rawtypes")
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext)
                throws MapperParsingException {
            Builder builder = new GeoPointFieldMapper.Builder(name);
            parseField(builder, name, node, parserContext);
            return builder;
        }
    }

    public GeoPointFieldMapper(String simpleName,
                               Integer position,
                               @Nullable String defaultExpression,
                               MappedFieldType fieldType,
                               MappedFieldType defaultFieldType,
                               Settings indexSettings,
                               MultiFields multiFields,
                               CopyTo copyTo) {
        super(simpleName, position, defaultExpression, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    public static class GeoPointFieldType extends MappedFieldType {
        public GeoPointFieldType() {
        }

        GeoPointFieldType(GeoPointFieldType ref) {
            super(ref);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public MappedFieldType clone() {
            return new GeoPointFieldType(this);
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            }
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "Geo fields do not support exact searching, use dedicated geo queries instead: ["
                                                   + name() + "]");
        }
    }

    protected void parse(ParseContext context, GeoPoint point) throws IOException {

        if (point.lat() > 90.0 || point.lat() < -90.0) {
            throw new IllegalArgumentException("illegal latitude value [" + point.lat() + "] for " + name());
        }
        if (point.lon() > 180.0 || point.lon() < -180) {
            throw new IllegalArgumentException("illegal longitude value [" + point.lon() + "] for " + name());
        }
        if (fieldType().indexOptions() != IndexOptions.NONE) {
            context.doc().add(new LatLonPoint(fieldType().name(), point.lat(), point.lon()));
        }
        if (fieldType().stored()) {
            context.doc().add(new StoredField(fieldType().name(), point.toString()));
        }
        if (fieldType.hasDocValues()) {
            context.doc().add(new LatLonDocValuesField(fieldType().name(), point.lat(), point.lon()));
        } else if (fieldType().stored() || fieldType().indexOptions() != IndexOptions.NONE) {
            List<IndexableField> fields = new ArrayList<>(1);
            createFieldNamesField(context, fields);
            for (IndexableField field : fields) {
                context.doc().add(field);
            }
        }
        // if the mapping contains multifields then use the geohash string
        if (multiFields.iterator().hasNext()) {
            multiFields.parse(this, context.createExternalValueContext(point.geohash()));
        }
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        context.path().add(simpleName());

        GeoPoint sparse = context.parseExternalValue(GeoPoint.class);

        if (sparse != null) {
            parse(context, sparse);
        } else {
            sparse = new GeoPoint();
            XContentParser.Token token = context.parser().currentToken();
            if (token == XContentParser.Token.START_ARRAY) {
                token = context.parser().nextToken();
                if (token == XContentParser.Token.START_ARRAY) {
                    // its an array of array of lon/lat [ [1.2, 1.3], [1.4, 1.5] ]
                    while (token != XContentParser.Token.END_ARRAY) {
                        parseGeoPointIgnoringMalformed(context, sparse);
                        token = context.parser().nextToken();
                    }
                } else {
                    // its an array of other possible values
                    if (token == XContentParser.Token.VALUE_NUMBER) {
                        final double lon = context.parser().doubleValue();
                        token = context.parser().nextToken();
                        final double lat = context.parser().doubleValue();
                        token = context.parser().nextToken();
                        Double alt = Double.NaN;
                        if (token == XContentParser.Token.VALUE_NUMBER) {
                            alt = GeoPoint.assertZValue(context.parser().doubleValue());
                        } else if (token != XContentParser.Token.END_ARRAY) {
                            throw new ElasticsearchParseException("[{}] field type does not accept > 3 dimensions", CONTENT_TYPE);
                        }
                        parse(context, sparse.reset(lat, lon));
                    } else {
                        while (token != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_STRING) {
                                parseGeoPointStringIgnoringMalformed(context, sparse);
                            } else {
                                parseGeoPointIgnoringMalformed(context, sparse);
                            }
                            token = context.parser().nextToken();
                        }
                    }
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                parseGeoPointStringIgnoringMalformed(context, sparse);
            } else if (token == XContentParser.Token.VALUE_NULL) {
                if (fieldType.nullValue() != null) {
                    parse(context, (GeoPoint) fieldType.nullValue());
                }
            } else {
                parseGeoPointIgnoringMalformed(context, sparse);
            }
        }

        context.path().remove();
    }

    /**
     * Parses geopoint represented as an object or an array, ignores malformed geopoints if needed
     */
    private void parseGeoPointIgnoringMalformed(ParseContext context, GeoPoint sparse) throws IOException {
        parse(context, GeoUtils.parseGeoPoint(context.parser(), sparse));
    }

    /**
     * Parses geopoint represented as a string and ignores malformed geopoints if needed
     */
    private void parseGeoPointStringIgnoringMalformed(ParseContext context, GeoPoint sparse) throws IOException {
        parse(context, sparse.resetFromString(context.parser().text()));
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
    }
}

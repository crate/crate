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
package org.elasticsearch.common.geo.parsers;

import org.locationtech.jts.geom.Coordinate;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoShapeType;
import org.elasticsearch.common.geo.builders.CircleBuilder;
import org.elasticsearch.common.geo.builders.GeometryCollectionBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses shape geometry represented in geojson
 *
 * complies with geojson specification: https://tools.ietf.org/html/rfc7946
 */
abstract class GeoJsonParser {
    protected static ShapeBuilder parse(XContentParser parser, GeoShapeFieldMapper shapeMapper)
        throws IOException {
        GeoShapeType shapeType = null;
        DistanceUnit.Distance radius = null;
        CoordinateNode coordinateNode = null;
        GeometryCollectionBuilder geometryCollections = null;

        ShapeBuilder.Orientation requestedOrientation =
            (shapeMapper == null) ? ShapeBuilder.Orientation.RIGHT : shapeMapper.fieldType().orientation();
        Explicit<Boolean> coerce = (shapeMapper == null) ? GeoShapeFieldMapper.Defaults.COERCE : shapeMapper.coerce();
        Explicit<Boolean> ignoreZValue = (shapeMapper == null) ? GeoShapeFieldMapper.Defaults.IGNORE_Z_VALUE : shapeMapper.ignoreZValue();

        String malformedException = null;

        XContentParser.Token token;
        try {
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();

                    if (ShapeParser.FIELD_TYPE.match(fieldName, parser.getDeprecationHandler())) {
                        parser.nextToken();
                        final GeoShapeType type = GeoShapeType.forName(parser.text());
                        if (shapeType != null && shapeType.equals(type) == false) {
                            malformedException = ShapeParser.FIELD_TYPE + " already parsed as ["
                                + shapeType + "] cannot redefine as [" + type + "]";
                        } else {
                            shapeType = type;
                        }
                    } else if (ShapeParser.FIELD_COORDINATES.match(fieldName, parser.getDeprecationHandler())) {
                        parser.nextToken();
                        CoordinateNode tempNode = parseCoordinates(parser, ignoreZValue.value());
                        if (coordinateNode != null && tempNode.numDimensions() != coordinateNode.numDimensions()) {
                            throw new ElasticsearchParseException("Exception parsing coordinates: " +
                                "number of dimensions do not match");
                        }
                        coordinateNode = tempNode;
                    } else if (ShapeParser.FIELD_GEOMETRIES.match(fieldName, parser.getDeprecationHandler())) {
                        if (shapeType == null) {
                            shapeType = GeoShapeType.GEOMETRYCOLLECTION;
                        } else if (shapeType.equals(GeoShapeType.GEOMETRYCOLLECTION) == false) {
                            malformedException = "cannot have [" + ShapeParser.FIELD_GEOMETRIES + "] with type set to ["
                                + shapeType + "]";
                        }
                        parser.nextToken();
                        geometryCollections = parseGeometries(parser, shapeMapper);
                    } else if (CircleBuilder.FIELD_RADIUS.match(fieldName, parser.getDeprecationHandler())) {
                        if (shapeType == null) {
                            shapeType = GeoShapeType.CIRCLE;
                        } else if (shapeType != null && shapeType.equals(GeoShapeType.CIRCLE) == false) {
                            malformedException = "cannot have [" + CircleBuilder.FIELD_RADIUS + "] with type set to ["
                                + shapeType + "]";
                        }
                        parser.nextToken();
                        radius = DistanceUnit.Distance.parseDistance(parser.text());
                    } else if (ShapeParser.FIELD_ORIENTATION.match(fieldName, parser.getDeprecationHandler())) {
                        if (shapeType != null
                            && (shapeType.equals(GeoShapeType.POLYGON) || shapeType.equals(GeoShapeType.MULTIPOLYGON)) == false) {
                            malformedException = "cannot have [" + ShapeParser.FIELD_ORIENTATION + "] with type set to [" + shapeType + "]";
                        }
                        parser.nextToken();
                        requestedOrientation = ShapeBuilder.Orientation.fromString(parser.text());
                    } else {
                        parser.nextToken();
                        parser.skipChildren();
                    }
                }
            }
        } catch (Exception ex) {
            // Skip all other fields until the end of the object
            while (parser.currentToken() != XContentParser.Token.END_OBJECT && parser.currentToken() != null) {
                parser.nextToken();
                parser.skipChildren();
            }
            throw ex;
        }

        if (malformedException != null) {
            throw new ElasticsearchParseException(malformedException);
        } else if (shapeType == null) {
            throw new ElasticsearchParseException("shape type not included");
        } else if (coordinateNode == null && GeoShapeType.GEOMETRYCOLLECTION != shapeType) {
            throw new ElasticsearchParseException("coordinates not included");
        } else if (geometryCollections == null && GeoShapeType.GEOMETRYCOLLECTION == shapeType) {
            throw new ElasticsearchParseException("geometries not included");
        } else if (radius != null && GeoShapeType.CIRCLE != shapeType) {
            throw new ElasticsearchParseException("field [{}] is supported for [{}] only", CircleBuilder.FIELD_RADIUS,
                CircleBuilder.TYPE);
        }

        if (shapeType == null) {
            throw new ElasticsearchParseException("shape type [{}] not included", shapeType);
        }

        if (shapeType.equals(GeoShapeType.GEOMETRYCOLLECTION)) {
            return geometryCollections;
        }

        return shapeType.getBuilder(coordinateNode, radius, requestedOrientation, coerce.value());
    }

    /**
     * Recursive method which parses the arrays of coordinates used to define
     * Shapes
     *
     * @param parser
     *            Parser that will be read from
     * @return CoordinateNode representing the start of the coordinate tree
     * @throws IOException
     *             Thrown if an error occurs while reading from the
     *             XContentParser
     */
    private static CoordinateNode parseCoordinates(XContentParser parser, boolean ignoreZValue) throws IOException {
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            parser.skipChildren();
            parser.nextToken();
            throw new ElasticsearchParseException("coordinates cannot be specified as objects");
        }

        XContentParser.Token token = parser.nextToken();
        // Base cases
        if (token != XContentParser.Token.START_ARRAY &&
            token != XContentParser.Token.END_ARRAY &&
            token != XContentParser.Token.VALUE_NULL) {
            return new CoordinateNode(parseCoordinate(parser, ignoreZValue));
        } else if (token == XContentParser.Token.VALUE_NULL) {
            throw new IllegalArgumentException("coordinates cannot contain NULL values)");
        }

        List<CoordinateNode> nodes = new ArrayList<>();
        while (token != XContentParser.Token.END_ARRAY) {
            CoordinateNode node = parseCoordinates(parser, ignoreZValue);
            if (nodes.isEmpty() == false && nodes.get(0).numDimensions() != node.numDimensions()) {
                throw new ElasticsearchParseException("Exception parsing coordinates: number of dimensions do not match");
            }
            nodes.add(node);
            token = parser.nextToken();
        }

        return new CoordinateNode(nodes);
    }

    private static Coordinate parseCoordinate(XContentParser parser, boolean ignoreZValue) throws IOException {
        if (parser.currentToken() != XContentParser.Token.VALUE_NUMBER) {
            throw new ElasticsearchParseException("geo coordinates must be numbers");
        }
        double lon = parser.doubleValue();
        if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
            throw new ElasticsearchParseException("geo coordinates must be numbers");
        }
        double lat = parser.doubleValue();
        XContentParser.Token token = parser.nextToken();
        // alt (for storing purposes only - future use includes 3d shapes)
        double alt = Double.NaN;
        if (token == XContentParser.Token.VALUE_NUMBER) {
            alt = GeoPoint.assertZValue(ignoreZValue, parser.doubleValue());
            parser.nextToken();
        }
        // do not support > 3 dimensions
        if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
            throw new ElasticsearchParseException("geo coordinates greater than 3 dimensions are not supported");
        }
        return new Coordinate(lon, lat, alt);
    }

    /**
     * Parse the geometries array of a GeometryCollection
     *
     * @param parser Parser that will be read from
     * @return Geometry[] geometries of the GeometryCollection
     * @throws IOException Thrown if an error occurs while reading from the XContentParser
     */
    static GeometryCollectionBuilder parseGeometries(XContentParser parser, GeoShapeFieldMapper mapper) throws
        IOException {
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new ElasticsearchParseException("geometries must be an array of geojson objects");
        }

        XContentParser.Token token = parser.nextToken();
        GeometryCollectionBuilder geometryCollection = new GeometryCollectionBuilder();
        while (token != XContentParser.Token.END_ARRAY) {
            ShapeBuilder shapeBuilder = ShapeParser.parse(parser);
            geometryCollection.shape(shapeBuilder);
            token = parser.nextToken();
        }

        return geometryCollection;
    }
}

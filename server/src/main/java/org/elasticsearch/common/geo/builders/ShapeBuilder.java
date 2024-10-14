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

package org.elasticsearch.common.geo.builders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Assertions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;

/**
 * Basic class for building GeoJSON shapes like Polygons, Linestrings, etc
 */
public abstract class ShapeBuilder<T extends Shape, E extends ShapeBuilder<T,E>> {

    protected static final Logger LOGGER = LogManager.getLogger(ShapeBuilder.class);

    private static final boolean DEBUG;

    static {
        // if asserts are enabled we run the debug statements even if they are not logged
        // to prevent exceptions only present if debug enabled
        DEBUG = Assertions.ENABLED;
    }

    protected final List<Coordinate> coordinates;

    public static final double DATELINE = 180;

    /**
     * coordinate at [0.0, 0.0]
     */
    public static final Coordinate ZERO_ZERO = new Coordinate(0.0, 0.0);
    public static final JtsSpatialContext SPATIAL_CONTEXT = JtsSpatialContext.GEO;
    public static final JtsShapeFactory SHAPE_FACTORY = SPATIAL_CONTEXT.getShapeFactory();
    public static final GeometryFactory GEO_FACTORY = SHAPE_FACTORY.getGeometryFactory();

    /** We're expecting some geometries might cross the dateline. */
    protected final boolean wrapdateline = SPATIAL_CONTEXT.isGeo();

    /** It's possible that some geometries in a MULTI* shape might overlap. With the possible exception of GeometryCollection,
     * this normally isn't allowed.
     */
    protected static final boolean MULTI_POLYGON_MAY_OVERLAP = false;
    /** @see org.locationtech.spatial4j.shape.jts.JtsGeometry#validate() */
    protected static final boolean AUTO_VALIDATE_JTS_GEOMETRY = true;
    /** @see org.locationtech.spatial4j.shape.jts.JtsGeometry#index() */
    protected static final boolean AUTO_INDEX_JTS_GEOMETRY = true;//may want to turn off once SpatialStrategy impls do it.

    /** default ctor */
    protected ShapeBuilder() {
        coordinates = new ArrayList<>();
    }

    /** ctor from list of coordinates */
    protected ShapeBuilder(List<Coordinate> coordinates) {
        if (coordinates == null || coordinates.size() == 0) {
            throw new IllegalArgumentException("cannot create point collection with empty set of points");
        }
        this.coordinates = coordinates;
    }

    /**
     * Copy all coordinate to a new Array
     *
     * @param closed if set to true the first point of the array is repeated as last element
     * @return Array of coordinates
     */
    protected Coordinate[] coordinates(boolean closed) {
        Coordinate[] result = coordinates.toArray(new Coordinate[coordinates.size() + (closed ? 1 : 0)]);
        if (closed) {
            result[result.length - 1] = result[0];
        }
        return result;
    }

    protected JtsGeometry jtsGeometry(Geometry geom) {
        //dateline180Check is false because ElasticSearch does it's own dateline wrapping
        JtsGeometry jtsGeometry = new JtsGeometry(geom, SPATIAL_CONTEXT, false, MULTI_POLYGON_MAY_OVERLAP);
        if (AUTO_VALIDATE_JTS_GEOMETRY) {
            jtsGeometry.validate();
        }
        if (AUTO_INDEX_JTS_GEOMETRY) {
            jtsGeometry.index();
        }
        return jtsGeometry;
    }

    /**
     * Create a new Shape from this builder. Since calling this method could change the
     * defined shape. (by inserting new coordinates or change the position of points)
     * the builder looses its validity. So this method should only be called once on a builder
     * @return new {@link Shape} defined by the builder
     */
    public abstract T buildS4J();

    /**
     * build lucene geometry.
     *
     * @return GeoPoint, double[][], Line, Line[], Polygon, Polygon[], Rectangle, Object[]
     */
    public abstract Object buildLucene();

    protected static Coordinate shift(Coordinate coordinate, double dateline) {
        if (dateline == 0) {
            return coordinate;
        } else {
            return new Coordinate(-2 * dateline + coordinate.x, coordinate.y);
        }
    }

    /**
     * Calculate the intersection of a line segment and a vertical dateline.
     *
     * @param p1
     *            start-point of the line segment
     * @param p2
     *            end-point of the line segment
     * @param dateline
     *            x-coordinate of the vertical dateline
     * @return position of the intersection in the open range (0..1] if the line
     *         segment intersects with the line segment. Otherwise this method
     *         returns {@link Double#NaN}
     */
    protected static final double intersection(Coordinate p1, Coordinate p2, double dateline) {
        if (p1.x == p2.x && p1.x != dateline) {
            return Double.NaN;
        } else if (p1.x == p2.x && p1.x == dateline) {
            return 1.0;
        } else {
            final double t = (dateline - p1.x) / (p2.x - p1.x);
            if (t > 1 || t <= 0) {
                return Double.NaN;
            } else {
                return t;
            }
        }
    }

    /**
     * Calculate all intersections of line segments and a vertical line. The
     * Array of edges will be ordered asc by the y-coordinate of the
     * intersections of edges.
     *
     * @param dateline
     *            x-coordinate of the dateline
     * @param edges
     *            set of edges that may intersect with the dateline
     * @return number of intersecting edges
     */
    protected static int intersections(double dateline, Edge[] edges) {
        int numIntersections = 0;
        assert !Double.isNaN(dateline);
        for (int i = 0; i < edges.length; i++) {
            Coordinate p1 = edges[i].coordinate;
            Coordinate p2 = edges[i].next.coordinate;
            assert !Double.isNaN(p2.x) && !Double.isNaN(p1.x);
            edges[i].intersect = Edge.MAX_COORDINATE;

            double position = intersection(p1, p2, dateline);
            if (!Double.isNaN(position)) {
                edges[i].intersection(position);
                numIntersections++;
            }
        }
        Arrays.sort(edges, INTERSECTION_ORDER);
        return numIntersections;
    }

    /**
     * This helper class implements a linked list for {@link Coordinate}. It contains
     * fields for a dateline intersection and component id
     */
    protected static final class Edge {
        Coordinate coordinate; // coordinate of the start point
        Edge next; // next segment
        Coordinate intersect; // potential intersection with dateline
        int component = -1; // id of the component this edge belongs to
        public static final Coordinate MAX_COORDINATE = new Coordinate(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);

        protected Edge(Coordinate coordinate, Edge next, Coordinate intersection) {
            this.coordinate = coordinate;
            // use setter to catch duplicate point cases
            this.setNext(next);
            this.intersect = intersection;
            if (next != null) {
                this.component = next.component;
            }
        }

        protected Edge(Coordinate coordinate, Edge next) {
            this(coordinate, next, Edge.MAX_COORDINATE);
        }

        protected void setNext(Edge next) {
            // don't bother setting next if its null
            if (next != null) {
                // self-loop throws an invalid shape
                if (this.coordinate.equals(next.coordinate)) {
                    throw new InvalidShapeException("Provided shape has duplicate consecutive coordinates at: " + this.coordinate);
                }
                this.next = next;
            }
        }

        /**
         * Set the intersection of this line segment to the given position
         *
         * @param position
         *            position of the intersection [0..1]
         * @return the {@link Coordinate} of the intersection
         */
        protected Coordinate intersection(double position) {
            return intersect = position(coordinate, next.coordinate, position);
        }

        protected static Coordinate position(Coordinate p1, Coordinate p2, double position) {
            if (position == 0) {
                return p1;
            } else if (position == 1) {
                return p2;
            } else {
                final double x = p1.x + position * (p2.x - p1.x);
                final double y = p1.y + position * (p2.y - p1.y);
                return new Coordinate(x, y);
            }
        }

        @Override
        public String toString() {
            return "Edge[Component=" + component + "; start=" + coordinate + " " + "; intersection=" + intersect + "]";
        }
    }

    protected static final IntersectionOrder INTERSECTION_ORDER = new IntersectionOrder();

    private static final class IntersectionOrder implements Comparator<Edge> {
        @Override
        public int compare(Edge o1, Edge o2) {
            return Double.compare(o1.intersect.y, o2.intersect.y);
        }
    }

    public enum Orientation {
        LEFT,
        RIGHT;

        public static final Orientation CLOCKWISE = Orientation.LEFT;
        public static final Orientation COUNTER_CLOCKWISE = Orientation.RIGHT;
        public static final Orientation CW = Orientation.LEFT;
        public static final Orientation CCW = Orientation.RIGHT;

        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(this == Orientation.RIGHT);
        }

        public static Orientation readFrom(StreamInput in) throws IOException {
            return in.readBoolean() ? Orientation.RIGHT : Orientation.LEFT;
        }

        public static Orientation fromString(String orientation) {
            orientation = orientation.toLowerCase(Locale.ROOT);
            switch (orientation) {
                case "right":
                case "counterclockwise":
                case "ccw":
                    return Orientation.RIGHT;
                case "left":
                case "clockwise":
                case "cw":
                    return Orientation.LEFT;
                default:
                    throw new IllegalArgumentException("Unknown orientation [" + orientation + "]");
            }
        }
    }

    protected static final boolean debugEnabled() {
        return LOGGER.isDebugEnabled() || DEBUG;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ShapeBuilder)) return false;

        ShapeBuilder<?,?> that = (ShapeBuilder<?,?>) o;

        return Objects.equals(coordinates, that.coordinates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(coordinates);
    }
}

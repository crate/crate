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

package io.crate.integrationtests;

import static io.crate.testing.Asserts.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

public abstract class BaseGeoMatchIntegrationTest extends IntegTestCase {

    abstract String indexType();

    @Test
    public void test_geo_match_intersects_for_for_point() {
        createTableWithOneShape("POINT(13.5 52.5)");

        assertIntersects("POLYGON((13 53, 13 52, 14 52, 14 53, 13 53))");
        assertIntersects("""
            MULTIPOLYGON(
                ((13 53, 13 52, 14 52, 14 53, 13 53)),
                ((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);
        assertIntersects("""
            GEOMETRYCOLLECTION(
                POINT(14 53),
                LINESTRING(14.10 52.75, 14.5 52.45),
                POLYGON((13 53, 13 52, 14 52, 14 53, 13 53))
            )
            """);
    }

    @Test
    public void test_geo_match_disjoint_for_for_point() {
        createTableWithOneShape("POINT(13.5 52.5)");

        assertDisjoint("POINT(14 53)");
        assertDisjoint("MULTIPOINT((14 53), (12 52))");
        assertDisjoint("LINESTRING(13.3 52.5, 13.45 52.75)");
        assertDisjoint("MULTILINESTRING((13.3 52.5, 13.45 52.75), (14.10 52.75, 14.5 52.45))");
        assertDisjoint("POLYGON((13.6 52.7, 13.6 52.5, 14 52.5, 14 52.7, 13.6 52.7))");
        assertDisjoint("""
            MULTIPOLYGON(
                ((13.6 52.7, 13.6 52.5, 14 52.5, 14 52.7, 13.6 52.7)),
                ((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);
        assertDisjoint("""
            GEOMETRYCOLLECTION(
                POINT(14 53),
                LINESTRING(13.3 52.5, 13.45 52.75),
                POLYGON((13.6 52.7, 13.6 52.5, 14 52.5, 14 52.7, 13.6 52.7))
            )
            """);
    }

    @Test
    public void test_geo_within_for_match_for_point() {
        createTableWithOneShape("POINT(13.5 52.5)");

        assertWithin("POLYGON((13 53, 13 52, 14 52, 14 53, 13 53))");
        assertWithin("""
            MULTIPOLYGON(
                ((13 53, 13 52, 14 52, 14 53, 13 53)),
                ((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);
        assertWithin("""
            GEOMETRYCOLLECTION(
                POINT(13 52),
                POLYGON((13 53, 13 52, 14 52, 14 53, 13 53))
            )
            """);
    }

    @Test
    public void test_geo_match_intersects_for_multipoint() {
        createTableWithOneShape("MULTIPOINT((13.5 52.5), (12 52))");

        assertIntersects("POLYGON((13 53, 13 52, 14 52, 14 53, 13 53))");
        assertIntersects("""
            MULTIPOLYGON(
                ((13 53, 13 52, 14 52, 14 53, 13 53)),
                ((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);
        assertIntersects("""
            GEOMETRYCOLLECTION(
                POINT(14 53),
                LINESTRING(14.10 52.75, 14.5 52.45),
                POLYGON((13 53, 13 52, 14 52, 14 53, 13 53))
            )
            """);
    }

    @Test
    public void test_geo_match_disjoint_for_multipoint() {
        createTableWithOneShape("MULTIPOINT((13.5 52.5), (12 52))");

        assertDisjoint("POINT(14 53)");
        assertDisjoint("MULTIPOINT((14 53), (12.5 52.5))");
        assertDisjoint("LINESTRING(13.3 52.5, 13.45 52.75)");
        assertDisjoint("MULTILINESTRING((13.3 52.5, 13.45 52.75), (14.10 52.75, 14.5 52.45))");
        assertDisjoint("POLYGON((13.6 52.7, 13.6 52.5, 14 52.5, 14 52.7, 13.6 52.7))");
        assertDisjoint("""
            MULTIPOLYGON(
                ((13.6 52.7, 13.6 52.5, 14 52.5, 14 52.7, 13.6 52.7)),
                ((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);
        assertDisjoint("""
            GEOMETRYCOLLECTION(
                POINT(14 53),
                LINESTRING(13.3 52.5, 13.45 52.75),
                POLYGON((13.6 52.7, 13.6 52.5, 14 52.5, 14 52.7, 13.6 52.7))
            )
            """);
    }

    @Test
    public void test_geo_match_within_for_multipoint() {
        createTableWithOneShape("MULTIPOINT((13.5 52.5), (12 52))");

        assertWithin("POLYGON((11 53, 11 51, 14 51, 14 53, 11 53))");
        assertWithin("""
            MULTIPOLYGON(
                ((11.50 52, 12.05 52.20, 12.30 51.85, 11.50 52)),
                ((13.25 52.65, 13.50 52.30, 13.85 52.6, 13.25 52.65))
            )
            """);
        assertWithin("""
            GEOMETRYCOLLECTION(
                POINT(13 52),
                POLYGON((11 53, 11 51, 14 51, 14 53, 11 53))
            )
            """);
    }

    @Test
    public void test_geo_match_intersects_for_linestring() {
        createTableWithOneShape("LINESTRING(12.75 52.5, 13.75 53.05)");

        assertIntersects("LINESTRING(12.80 52.95, 13.60 52.80)");
        assertIntersects("MULTILINESTRING((12.80 52.95, 13.60 52.80), (14.10 52.75, 14.5 52.45))");
        assertIntersects("POLYGON((13 53, 13 52, 13.5 52, 13.5 53, 13 53))");
        assertIntersects("""
            MULTIPOLYGON(
                ((13 53, 13 52, 13.5 52, 13.5 53, 13 53)),
                ((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);
        assertIntersects("""
            GEOMETRYCOLLECTION(
                POINT(13.5 52.5),
                LINESTRING(12.80 52.95, 13.60 52.80),
                POLYGON((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);
    }

    @Test
    public void test_geo_match_disjoint_for_linestring() {
        createTableWithOneShape("LINESTRING(12.75 52.5, 13.75 53.05)");

        assertDisjoint("POINT(14 53)");
        assertDisjoint("MULTIPOINT((14 53), (12.5 52.5))");
        assertDisjoint("LINESTRING(14.10 52.75, 14.5 52.45)");
        assertDisjoint("MULTILINESTRING((12.5 53, 13.5 53), (14.10 52.75, 14.5 52.45))");
        assertDisjoint("POLYGON((13 52.5, 13 52, 13.5 52, 13.5 52.5, 13 52.5))");
        assertDisjoint("""
            MULTIPOLYGON(
                ((13 52.5, 13 52, 13.5 52, 13.5 52.5, 13 52.5)),
                ((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);
        assertDisjoint("""
            GEOMETRYCOLLECTION(
                POINT(14 53),
                LINESTRING(14.10 52.75, 14.5 52.45),
                POLYGON((13 52.5, 13 52, 13.5 52, 13.5 52.5, 13 52.5))
            )
            """);
    }

    @Test
    public void test_geo_match_within_for_linestring() {
        createTableWithOneShape("LINESTRING(12.75 52.5, 13.75 53.05)");

        assertWithin("POLYGON((12 53.5, 12 52, 14 52, 14 53.5, 12 53.5))");
        assertWithin("""
            MULTIPOLYGON(
                ((12 53.5, 12 52, 14 52, 14 53.5, 12 53.5)),
                ((11.23 53.24, 11.21 53, 11.49 53, 11.23 53.24))
            )
            """);
        assertWithin("""
            GEOMETRYCOLLECTION(
                POINT(11.5 53),
                POLYGON((12 53.5, 12 52, 14 52, 14 53.5, 12 53.5))
            )
            """);
    }

    @Test
    public void test_geo_match_intersects_for_multilinestring() {
        createTableWithOneShape("MULTILINESTRING((12.75 52.5, 13.75 53.05), (14.10 52.75, 14.5 52.45))");

        assertIntersects("LINESTRING(12 53, 14 53)");
        assertIntersects("MULTILINESTRING((12 53, 14 53), (12 54, 14 54))");
        assertIntersects("POLYGON((12.80 52.75, 13.45 52.75, 13 52.45, 12.80 52.75))");
        assertIntersects("""
            MULTIPOLYGON(
                ((12.80 52.75, 13.45 52.75, 13 52.45, 12.80 52.75)),
                ((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);
        assertIntersects("""
            GEOMETRYCOLLECTION(
                POINT(13.5 52.5),
                LINESTRING(12 53, 14 53),
                POLYGON((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);
    }

    @Test
    public void test_geo_match_disjoint_for_multilinestring() {
        createTableWithOneShape("MULTILINESTRING((12.75 52.5, 13.75 53.05), (14.10 52.75, 14.5 52.45))");

        assertDisjoint("POINT(12 52)");
        assertDisjoint("MULTIPOINT((13.5 53.5), (12 52))");
        assertDisjoint("LINESTRING(13.75 52.75, 13.75 52.45)");
        assertDisjoint("MULTILINESTRING((12.10 52.45, 12.5 52.90), (13.75 52.75, 13.75 52.45))");
        assertDisjoint("POLYGON((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))");
        assertDisjoint("""
            MULTIPOLYGON(
                ((12.50 52.50, 12.60 52.60, 12.60 52.50, 12.50 52.50)),
                ((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);
        assertDisjoint("""
            GEOMETRYCOLLECTION(
                POINT(12 52),
                LINESTRING(13.75 52.75, 13.75 52.45),
                POLYGON((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);
    }

    @Test
    public void test_geo_match_within_for_multilinestring() {
        createTableWithOneShape("MULTILINESTRING((12.75 52.5, 13.75 53.05), (14.10 52.75, 14.5 52.45))");

        assertWithin("POLYGON((12.55 52.55, 13.75 53.15, 14.70 52.40, 12.60 52.50, 12.55 52.55))");
        assertWithin("""
            MULTIPOLYGON(
                ((12.60 52.55, 13.70 53.15, 13.95 53.05, 12.75 52.40, 12.60 52.55)),
                ((14.05 52.80, 13.95 52.70, 14.45 52.40, 14.60 52.50, 14.05 52.80))
            )
            """);
        assertWithin("""
            GEOMETRYCOLLECTION(
                POINT(11.5 53),
                POLYGON((12.55 52.55, 13.75 53.15, 14.70 52.40, 12.60 52.50, 12.55 52.55))
            )
            """);
    }

    @Test
    public void test_geo_match_intersects_for_polygon() {
        createTableWithOneShape("POLYGON((13 53, 13 52, 14 52, 14 53, 13 53))");

        assertIntersects("POINT(13.5 52.5)");
        assertIntersects("MULTIPOINT((13.5 52.5), (12 52))");
        assertIntersects("LINESTRING(12.75 52.5, 13.75 53.05)");
        assertIntersects("MULTILINESTRING((12.75 52.5, 13.75 53.05), (14.10 52.75, 14.5 52.45))");
        assertIntersects("POLYGON((12.90 52.50, 13.25 52.60, 13.25 52.30, 12.90 52.50))");
        assertIntersects("""
            MULTIPOLYGON(
                ((12.90 52.50, 13.25 52.60, 13.25 52.30, 12.90 52.50)),
                ((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);
        assertIntersects("""
            GEOMETRYCOLLECTION(
                POINT(13.5 52.5),
                LINESTRING(14.10 52.75, 14.5 52.45),
                POLYGON((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);
    }

    @Test
    public void test_geo_match_disjoint_for_polygon() {
        createTableWithOneShape("POLYGON((13 53, 13 52, 14 52, 14 53, 13 53))");

        assertDisjoint("POINT(12 52)");
        assertDisjoint("MULTIPOINT((13.5 53.5), (12 52))");
        assertDisjoint("LINESTRING(14.10 52.75, 14.5 52.45)");
        assertDisjoint("MULTILINESTRING((12.10 52.45, 12.5 52.90), (14.10 52.75, 14.5 52.45))");
        assertDisjoint("POLYGON((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))");
        assertDisjoint("""
            MULTIPOLYGON(
                ((12.50 52.50, 12.90 52.60, 12.90 52.50, 12.50 52.50)),
                ((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);
        assertDisjoint("""
            GEOMETRYCOLLECTION(
                POINT(12 52),
                LINESTRING(14.10 52.75, 14.5 52.45),
                POLYGON((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);
    }

    @Test
    public void test_geo_match_within_for_polygon() {
        createTableWithOneShape("POLYGON((13 53, 13 52, 14 52, 14 53, 13 53))");

        assertWithin("POLYGON((14.10 53.05, 12.90 53.05, 12.90 51.95, 14.10 51.95, 14.10 53.05))");
        assertIntersects("""
            MULTIPOLYGON(
                ((14.10 53.05, 12.90 53.05, 12.90 51.95, 14.10 51.95, 14.10 53.05)),
                ((12.10 52.40, 12.10 52.15, 12.50 52.15, 12.50 52.40, 12.10 52.40))
            )
            """);
        assertIntersects("""
            GEOMETRYCOLLECTION(
                POINT(13.5 52.5),
                POLYGON((14.10 53.05, 12.90 53.05, 12.90 51.95, 14.10 51.95, 14.10 53.05))
            )
            """);
    }

    @Test
    public void test_geo_match_intersects_for_multipolygon() {
        createTableWithOneShape("""
            MULTIPOLYGON(
                ((12.50 52.50, 12.90 52.60, 12.90 52.50, 12.50 52.50)),
                ((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);

        assertIntersects("POINT(12.80 52.55)");
        assertIntersects("MULTIPOINT((12.80 52.55), (12 52))");
        assertIntersects("LINESTRING(12.75 52.5, 13.75 53.05)");
        assertIntersects("MULTILINESTRING((12.75 52.5, 13.75 53.05), (14.10 52.75, 14.5 52.45))");
        assertIntersects("POLYGON((12.65 52.60, 12.95 52.40, 13.10 52.55, 12.65 52.60))");
        assertIntersects("""
            MULTIPOLYGON(
                ((12.90 52.50, 13.00 52.60, 13.25 52.30, 12.90 52.50)),
                ((12.65 52.60, 12.75 52.40, 12.80 52.55, 12.65 52.60))
            )
            """);
        assertIntersects("""
            GEOMETRYCOLLECTION(
                POINT(13.5 52.5),
                LINESTRING(14.10 52.75, 14.5 52.45),
                POLYGON((12.65 52.60, 12.95 52.40, 13.10 52.55, 12.65 52.60))
            )
            """);
    }

    @Test
    public void test_geo_match_disjoint_for_multipolygon() {
        createTableWithOneShape("""
            MULTIPOLYGON(
                ((12.50 52.50, 12.90 52.60, 12.90 52.50, 12.50 52.50)),
                ((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);

        assertDisjoint("POINT(12 52)");
        assertDisjoint("MULTIPOINT((13.5 53.5), (12 52))");
        assertDisjoint("LINESTRING(14.10 52.75, 14.5 52.45)");
        assertDisjoint("MULTILINESTRING((12.10 52.45, 12.5 52.90), (14.10 52.75, 14.5 52.45))");
        assertDisjoint("POLYGON((13.20 52.55, 13.20 52.30,13.60 52.30, 13.20 52.55))");
        assertDisjoint("""
            MULTIPOLYGON(
                ((13.65 52.65, 13.65 52.45, 13.95 52.60, 13.65 52.65)),
                ((13.20 52.55, 13.20 52.30,13.60 52.30, 13.20 52.55))
            )
            """);
        assertDisjoint("""
            GEOMETRYCOLLECTION(
                POINT(12 52),
                LINESTRING(14.10 52.75, 14.5 52.45),
                POLYGON((13.20 52.55, 13.20 52.30,13.60 52.30, 13.20 52.55))
            )
            """);
    }

    @Test
    public void test_geo_match_within_for_multipolygon() {
        createTableWithOneShape("""
            MULTIPOLYGON(
                ((12.50 52.50, 12.90 52.60, 12.90 52.50, 12.50 52.50)),
                ((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);

        assertWithin("POLYGON((12 52.75, 12 52.20, 14.60 52.20, 14.60 52.75, 12 52.75))");
        assertWithin("""
            MULTIPOLYGON(
                ((12.35 52.65, 12.35 52.40, 13.05 52.40, 13.05 52.65, 12.35 52.65)),
                ((14.10 52.65, 14.10 52.25, 14.60 52.25, 14.60 52.65, 14.10 52.65))
            )
            """);
        assertWithin("""
            GEOMETRYCOLLECTION(
                POINT(13.5 52.5),
                POLYGON((12 52.75, 12 52.20, 14.60 52.20, 14.60 52.75, 12 52.75))
            )
            """);
    }

    @Test
    public void test_geo_match_intersects_for_geometrycollection() {
        createTableWithOneShape("""
            GEOMETRYCOLLECTION(
                POINT(12 52),
                LINESTRING(14.10 52.75, 14.5 52.45),
                POLYGON((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);

        assertIntersects("POINT(14.25 52.40)");
        assertIntersects("MULTIPOINT((14.25 52.40), (12 52))");
        assertIntersects("LINESTRING(14 52.65, 14.25 52.75, 14.15 52.60)");
        assertIntersects("MULTILINESTRING((14 52.65, 14.25 52.75, 14.15 52.60), (13.65 51.90,13.70 52.40))");
        assertIntersects("POLYGON((11.90 52.10, 11.90 51.90, 12.25 51.95, 11.90 52.10))");
        assertIntersects("""
            MULTIPOLYGON(
                ((11.90 52.10, 11.90 51.90, 12.25 51.95, 11.90 52.10)),
                ((12.65 52.60, 12.75 52.40, 12.80 52.55, 12.65 52.60))
            )
            """);
        assertIntersects("""
            GEOMETRYCOLLECTION(
                POINT(14.25 52.40),
                LINESTRING(14.10 52.75, 14.5 52.45),
                POLYGON((12.65 52.60, 12.95 52.40, 13.10 52.55, 12.65 52.60))
            )
            """);
    }

    @Test
    public void test_geo_match_disjoint_for_geometrycollection() {
        createTableWithOneShape("""
            GEOMETRYCOLLECTION(
                POINT(12 52),
                LINESTRING(14.10 52.75, 14.5 52.45),
                POLYGON((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);

        assertDisjoint("POINT(12.5 52.5)");
        assertDisjoint("MULTIPOINT((13.5 53.5), (12.5 52.5))");
        assertDisjoint("LINESTRING(12.75 52.5, 13.75 53.05)");
        assertDisjoint("MULTILINESTRING((12.10 52.45, 12.5 52.90), (12.75 52.5, 13.75 53.05))");
        assertDisjoint("POLYGON((13.20 52.55, 13.20 52.30,13.60 52.30, 13.20 52.55))");
        assertDisjoint("""
            MULTIPOLYGON(
                ((13.65 52.65, 13.65 52.45, 13.95 52.60, 13.65 52.65)),
                ((13.20 52.55, 13.20 52.30,13.60 52.30, 13.20 52.55))
            )
            """);
        assertDisjoint("""
            GEOMETRYCOLLECTION(
                POINT(12.5 52.5),
                LINESTRING(12.75 52.5, 13.75 53.05),
                POLYGON((13.20 52.55, 13.20 52.30,13.60 52.30, 13.20 52.55))
            )
            """);
    }

    @Test
    public void test_geo_match_within_for_geometrycollection() {
        createTableWithOneShape("""
            GEOMETRYCOLLECTION(
                POINT(12 52),
                LINESTRING(14.10 52.75, 14.5 52.45),
                POLYGON((14.20 52.60, 14.20 52.30, 14.50 52.30, 14.20 52.60))
            )
            """);

        assertWithin("POLYGON((14.15 52.80, 11.90 52, 12.15 51.90, 14.80 52.30, 14.15 52.80))");
        assertWithin("""
            MULTIPOLYGON(
                ((11.95 52.05, 11.90 51.90, 12.20 51.95, 11.95 52.05)),
                ((14 52.80, 14 52.25, 14.60 52.25, 14.60 52.80, 14 52.80))
            )
            """);
        assertWithin("""
            GEOMETRYCOLLECTION(
                POINT(13.5 52.5),
                POLYGON((12 52.85, 12 52.20, 14.60 52.20, 14.60 52.85, 12 52.85)),
                POLYGON((11.95 52.05, 11.90 51.90, 12.20 51.95, 11.95 52.05))
            )
            """);
    }

    private void createTableWithOneShape(String shape) {
        execute("create table t (s geo_shape index using %s) with (number_of_replicas=0)".formatted(indexType()));
        ensureGreen();
        execute("insert into t (s) values ('%s')".formatted(shape));
        execute("refresh table t");
    }

    private void assertIntersects(String shape) {
        execute("select * from t where match(s, '%s')".formatted(shape));
        assertThat(response).hasRowCount(1);
        execute("select * from t where match(s, '%s') using disjoint".formatted(shape));
        assertThat(response).hasRowCount(0);
    }

    private void assertDisjoint(String shape) {
        execute("select * from t where match(s, '%s') using disjoint".formatted(shape));
        assertThat(response).hasRowCount(1);
        execute("select * from t where match(s, '%s')".formatted(shape));
        assertThat(response).hasRowCount(0);
    }

    private void assertWithin(String shape) {
        execute("select * from t where match(s, '%s') using within".formatted(shape));
        assertThat(response).hasRowCount(1);
    }
}

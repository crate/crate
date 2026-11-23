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

package io.crate.expression.symbol;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.joda.time.Period;
import org.junit.Test;
import org.locationtech.spatial4j.shape.Point;

import io.crate.types.DataTypes;

public class LiteralValueFormatterTest {

    @Test
    public void testFormatOnArrayWithNullEntriesDoesNotCauseNPE() throws Exception {
        StringBuilder sb = new StringBuilder();
        LiteralValueFormatter.format(new Object[] { null, null}, sb);
        assertThat(sb.toString()).isEqualTo("[NULL, NULL]");
    }

    @Test
    public void test_format_intervals() throws Exception {
        StringBuilder sb = new StringBuilder();
        LiteralValueFormatter.format(Period.seconds(1), sb);
        assertThat(sb.toString()).isEqualTo("'PT1S'::interval");
    }

    @Test
    public void test_format_geo_point_as_wkt_that_can_be_cast_back() throws Exception {
        Point point = DataTypes.GEO_POINT.implicitCast(List.of(9.7417, 47.4108));
        StringBuilder sb = new StringBuilder();
        LiteralValueFormatter.format(Map.of("p", point), sb);
        assertThat(sb.toString()).isEqualTo("{\"p\"='POINT (9.7417 47.4108)'}");

        assertThat(DataTypes.GEO_POINT.implicitCast("POINT (9.7417 47.4108)")).isEqualTo(point);
    }
}

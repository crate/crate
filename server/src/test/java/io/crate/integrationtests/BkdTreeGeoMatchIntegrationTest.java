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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.Test;

import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.testing.UseJdbc;
import io.crate.types.GeoShapeType;

public class BkdTreeGeoMatchIntegrationTest extends BaseGeoMatchIntegrationTest {

    @Override
    String indexType() {
        return GeoShapeType.Names.TREE_BKD;
    }

    @UseJdbc(0)
    @Test
    public void test_geo_match_within_does_not_support_linestring() {
        execute("create table t (s geo_shape index using bkdtree) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into t (s) values ('POLYGON((13 53, 13 52, 14 52, 14 53, 13 53))')");
        execute("refresh table t");

        List<String> lineStrings = List.of(
            "LINESTRING(13 53, 13 52, 14 52, 14 53, 13 53)",
            "MULTILINESTRING((14.05 53.05, 12.85 53.05, 12.85 51.85, 14.05 51.85), (14.05 51.85, 14.05 53.05))"
        );
        for (String line : lineStrings) {
            assertThatThrownBy(() -> execute("select * from t where match(s, '%s') using within".formatted(line)))
                .isExactlyInstanceOf(UnsupportedFeatureException.class)
                .hasMessage("WITHIN queries with line geometries are not supported");
        }
    }
}

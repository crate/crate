/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.integrationtests;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0)
public class GeoBackwardCompatibilityTest extends SQLTransportIntegrationTest{

    /**
     * Test backward compatibility of within geo query with index versions before 2.3
     */
    @Test
    public void testWithinQuery() throws IOException {
        /*
         * Load the test index:
         * CREATE TABLE legacy_geo_point (
         *      id int primary key,
         *      p geo_point);
         * INSERT INTO legacy_geo_point (id, p) VALUES (1, 'POINT (10 10)');
         */
        Path zippedIndexDir = getDataPath("/indices/bwc/geo_point_1_7_5.zip");
        Settings nodeSettings = prepareBackwardsDataDir(zippedIndexDir);
        internalCluster().startNode(nodeSettings);
        ensureYellow();
        execute("select * from legacy_geo_point where within(p, 'POLYGON (( 5 5, 30 5, 30 30, 5 35, 5 5 ))')");
        assertThat(response.rowCount(), is(1L));
    }

}

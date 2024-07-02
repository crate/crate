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

package org.elasticsearch.cluster.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.junit.Test;

public class MetadataCreateIndexServiceTest extends ESTestCase {

    @Test
    public void testCalculateNumRoutingShards() {
        assertThat(MetadataCreateIndexService.calculateNumRoutingShards(1, Version.CURRENT)).isEqualTo(1024);
        assertThat(MetadataCreateIndexService.calculateNumRoutingShards(2, Version.CURRENT)).isEqualTo(1024);
        assertThat(MetadataCreateIndexService.calculateNumRoutingShards(3, Version.CURRENT)).isEqualTo(768);
        assertThat(MetadataCreateIndexService.calculateNumRoutingShards(9, Version.CURRENT)).isEqualTo(576);
        assertThat(MetadataCreateIndexService.calculateNumRoutingShards(512, Version.CURRENT)).isEqualTo(1024);
        assertThat(MetadataCreateIndexService.calculateNumRoutingShards(1024, Version.CURRENT)).isEqualTo(2048);
        assertThat(MetadataCreateIndexService.calculateNumRoutingShards(2048, Version.CURRENT)).isEqualTo(4096);

        Version latestBeforeChange = VersionUtils.getPreviousVersion(Version.V_5_8_0);
        int numShards = randomIntBetween(1, 1000);
        assertThat(MetadataCreateIndexService.calculateNumRoutingShards(numShards, latestBeforeChange)).isEqualTo(numShards);
        assertThat(MetadataCreateIndexService.calculateNumRoutingShards(numShards,
            VersionUtils.randomVersionBetween(random(), VersionUtils.getFirstVersion(), latestBeforeChange))).isEqualTo(numShards);

        for (int i = 0; i < 1000; i++) {
            int randomNumShards = randomIntBetween(1, 10000);
            int numRoutingShards = MetadataCreateIndexService.calculateNumRoutingShards(randomNumShards, Version.CURRENT);
            if (numRoutingShards <= 1024) {
                assertThat(randomNumShards).isLessThanOrEqualTo(512);
                assertThat(numRoutingShards).isGreaterThan(512);
            } else {
                assertThat(numRoutingShards / 2).isEqualTo(randomNumShards);
            }

            double ratio = numRoutingShards / (double) randomNumShards;
            int intRatio = (int) ratio;
            assertThat((double)(intRatio)).isEqualTo(ratio);
            assertThat(ratio).isGreaterThan(1);
            assertThat(ratio).isLessThanOrEqualTo(1024);
            assertThat(intRatio % 2).isZero();
            assertThat(intRatio).as("ratio is not a power of two").isEqualTo(Integer.highestOneBit(intRatio));
        }
    }
}

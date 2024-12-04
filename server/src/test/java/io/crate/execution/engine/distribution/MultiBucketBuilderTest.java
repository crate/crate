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

package io.crate.execution.engine.distribution;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import io.crate.Streamer;
import io.crate.data.Row1;
import io.crate.data.breaker.RamAccounting;
import io.crate.types.DataTypes;

public class MultiBucketBuilderTest {

    private List<MultiBucketBuilder> builders = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        builders.add(new ModuloBucketBuilder(new Streamer[]{DataTypes.INTEGER.streamer()}, 1, 0, RamAccounting.NO_ACCOUNTING));
        builders.add(new BroadcastingBucketBuilder(new Streamer[]{DataTypes.INTEGER.streamer()}, 1, RamAccounting.NO_ACCOUNTING));
    }

    @Test
    public void testBucketIsEmptyAfterSecondBuildBucket() throws Exception {
        StreamBucket[] buckets = new StreamBucket[1];
        for (MultiBucketBuilder builder : builders) {
            builder.add(new Row1(42));

            builder.build(buckets);
            assertThat(buckets[0]).hasSize(1);

            builder.build(buckets);
            assertThat(buckets[0]).hasSize(0);
        }
    }

    @Test
    public void testSizeIsResetOnBuildBuckets() throws Exception {
        StreamBucket[] buckets = new StreamBucket[1];

        for (MultiBucketBuilder builder : builders) {
            builder.add(new Row1(42));
            builder.add(new Row1(42));
            assertThat(builder.size()).isEqualTo(2);

            builder.build(buckets);
            assertThat(buckets[0]).hasSize(2);
            assertThat(builder.size()).isEqualTo(0);
        }
    }
}

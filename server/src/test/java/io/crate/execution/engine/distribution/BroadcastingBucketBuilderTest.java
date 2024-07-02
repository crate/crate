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

import static io.crate.testing.Asserts.assertThat;

import org.junit.Test;

import io.crate.Streamer;
import io.crate.data.Bucket;
import io.crate.data.Row1;
import io.crate.data.breaker.RamAccounting;
import io.crate.types.DataTypes;

public class BroadcastingBucketBuilderTest {

    @Test
    public void testBucketIsReUsed() throws Exception {
        final BroadcastingBucketBuilder builder = new BroadcastingBucketBuilder(new Streamer[]{DataTypes.INTEGER.streamer()}, 3, RamAccounting.NO_ACCOUNTING);
        builder.add(new Row1(10));

        StreamBucket[] buckets = new StreamBucket[3];
        builder.build(buckets);

        final Bucket rows = buckets[0];
        assertThat(rows).isSameAs(buckets[1]);
        assertThat(rows).isSameAs(buckets[2]);
    }
}

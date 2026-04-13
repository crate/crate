/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.junit.Test;

import io.crate.Streamer;
import io.crate.data.RowN;
import io.crate.testing.PlainRamAccounting;
import io.crate.types.DataTypes;

public class StreamBucketTest {

    @Test
    public void test_accounting() {
        Streamer<?>[] streamers = new Streamer[]{DataTypes.STRING.streamer()};
        var ramAccounting = new PlainRamAccounting();
        StreamBucket.Builder builder = new StreamBucket.Builder(streamers, ramAccounting);
        builder.add(new RowN("0123456789"));
        assertThat(builder.ramBytesUsed()).isEqualTo(1080L); //Used to be 12
    }

    @Test
    public void test_accounting_catches_CBE_per_written_column() throws Exception {
        Streamer<?>[] streamers = new Streamer[]{
            DataTypes.UNTYPED_OBJECT.streamer(),
            DataTypes.UNTYPED_OBJECT.streamer()
        };

        Map<String, Object> object = new HashMap<>();
        // Heavy object to surpass initial ram usage (INITIAL_PAGE_SIZE + shallow size).
        for (int i = 0; i < 100; i++) {
            var str = String.valueOf(i);
            object.put(str, "value" + str);
        }
        // Object uses 1368L ram when written alone.
        long objectSize = 1368;

        // ramAccounting should throw CBE on the first column already
        // instead of proceeding with other, potentially heavy objects.
        var ramAccounting = new PlainRamAccounting(objectSize - 1);
        StreamBucket.Builder builder = new StreamBucket.Builder(streamers, ramAccounting);
        assertThatThrownBy(() -> builder.add(new RowN(object, object)))
            .isExactlyInstanceOf(CircuitBreakingException.class)
            .hasMessageContaining(Long.toString(objectSize)); // Used to be 2456.
    }

}

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

package io.crate.types;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.hamcrest.Matchers;
import org.joda.time.Period;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class IntervalTypeTest {

    @Test
    public void test_interval_value_streaming_round_trip() throws Exception {
        var out = new BytesStreamOutput();
        var period = new Period(1, 2, 3, 4, 5, 6, 7, 8);
        IntervalType.INSTANCE.writeValueTo(out, period);

        var in = out.bytes().streamInput();
        var periodFromStream = IntervalType.INSTANCE.readValueFrom(in);

        assertThat(periodFromStream, is(period));
    }

    @Test
    public void test_stream_null_period() throws Exception {
        var out = new BytesStreamOutput();
        IntervalType.INSTANCE.writeValueTo(out, null);
        var in = out.bytes().streamInput();
        assertThat(IntervalType.INSTANCE.readValueFrom(in), Matchers.nullValue());
    }
}

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

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.joda.time.Period;
import org.junit.Test;

public class IntervalTypeTest extends DataTypeTestCase<Period> {

    @Override
    public DataType<Period> getType() {
        return IntervalType.INSTANCE;
    }

    @Test
    public void test_interval_value_streaming_round_trip() throws Exception {
        var out = new BytesStreamOutput();
        var period = new Period(1, 2, 3, 4, 5, 6, 7, 8);
        IntervalType.INSTANCE.writeValueTo(out, period);

        var in = out.bytes().streamInput();
        var periodFromStream = IntervalType.INSTANCE.readValueFrom(in);

        assertThat(periodFromStream).isEqualTo(period);
    }

    @Test
    public void test_stream_null_period() throws Exception {
        var out = new BytesStreamOutput();
        IntervalType.INSTANCE.writeValueTo(out, null);
        var in = out.bytes().streamInput();
        assertThat(IntervalType.INSTANCE.readValueFrom(in)).isNull();
    }

    @Test
    public void test_compare() {
        Period p1 = new Period(1, 13, 8, 568, 128, 678, 91234, 1234567);
        Period p2 = new Period(1, 16, 68, 64, 20, 59, 8, 567);
        assertThat(IntervalType.INSTANCE.compare(p1, p2)).isZero();
        p2 = new Period(1, 17, 68, 64, 20, 59, 8, 567);
        assertThat(IntervalType.INSTANCE.compare(p1, p2)).isEqualTo(-1);
        p1 = new Period(1, 13, 8, 568, 129, 678, 91234, 1234567);
        p2 = new Period(1, 16, 68, 64, 20, 59, 8, 567);
        assertThat(IntervalType.INSTANCE.compare(p1, p2)).isEqualTo(1);
    }
}

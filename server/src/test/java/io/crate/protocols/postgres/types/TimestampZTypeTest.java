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

package io.crate.protocols.postgres.types;

import static org.assertj.core.api.Assertions.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class TimestampZTypeTest extends BasePGTypeTest<Long> {

    public TimestampZTypeTest() {
        super(TimestampZType.INSTANCE);
    }

    @Test
    public void testBinaryRoundtrip() {
        ByteBuf buffer = Unpooled.buffer();
        try {
            long value = 1467072000000L;
            int written = pgType.writeAsBinary(buffer, value);
            int length = buffer.readInt();
            assertThat(written - 4).isEqualTo(length);
            long readValue = (long) pgType.readBinaryValue(buffer, length);
            assertThat(readValue).isEqualTo(value);
        } finally {
            buffer.release();
        }
    }

    @Test
    public void testEncodeAsUTF8Text() {
        assertThat(new String(TimestampZType.INSTANCE.encodeAsUTF8Text(1467072000000L), UTF_8)).isEqualTo("2016-06-28 00:00:00.000+00");
        assertThat(new String(TimestampZType.INSTANCE.encodeAsUTF8Text(-93661920000000L), UTF_8)).isEqualTo("1000-12-22 00:00:00.000+00 BC");
    }

    @Test
    public void testDecodeUTF8TextWithUnexpectedNumberOfFractionDigits() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot parse more than 9 digits for fraction of a second");
        TimestampZType.INSTANCE.decodeUTF8Text("2016-06-28 00:00:00.0000000001+05:00".getBytes(UTF_8));
    }

    @Test
    public void test_decode_ts_string() throws Exception {
        assertThat(
            TimestampZType.INSTANCE.decodeUTF8Text("2021-01-13T14:37:17.25988Z".getBytes(UTF_8)),
            is(1610548637259L)
        );
        assertThat(
            TimestampZType.INSTANCE.decodeUTF8Text("2016-06-28 00:00:00.000+00".getBytes(UTF_8)),
            is(1467072000000L)
        );
        assertThat(
            TimestampZType.INSTANCE.decodeUTF8Text("1000-12-22 00:00:00.000+00 BC".getBytes(UTF_8)),
            is(-93661920000000L)
        );
    }
}

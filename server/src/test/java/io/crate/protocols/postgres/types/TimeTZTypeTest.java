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

package io.crate.protocols.postgres.types;

import io.crate.types.TimeTZ;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.is;


public class TimeTZTypeTest extends BasePGTypeTest<Long> {

    public TimeTZTypeTest() {
        super(TimeTZType.INSTANCE);
    }

    @Test
    public void testBinaryRoundTrip() {
        ByteBuf buffer = Unpooled.buffer();
        try {
            TimeTZ value = new TimeTZ(53005278000L, 0);
            int written = pgType.writeAsBinary(buffer, value);
            int length = buffer.readInt();
            assertThat(written - 4, is(length));
            TimeTZ readValue = (TimeTZ) pgType.readBinaryValue(buffer, length);
            assertThat(readValue, is(value));
        } finally {
            buffer.release();
        }
    }

    @Test
    public void testEncodeAsUTF8Text() {
        assertThat(TimeTZType.INSTANCE.encodeAsUTF8Text(new TimeTZ(53005278000L, -7320)),
            is("14:43:25.278-02:02".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testDecodeAsUTF8Text() {
        assertThat(TimeTZType.INSTANCE.decodeUTF8Text("04:00:00.123456789+03:00".getBytes()),
                   is(new TimeTZ(14400123456L, 10800)));
    }
}

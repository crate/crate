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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.format.DateTimeParseException;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class DateTypeTest extends BasePGTypeTest<Long> {

    public DateTypeTest() {
        super(DateType.INSTANCE);
    }

    @Test
    public void testBinaryRoundtrip() {
        ByteBuf buffer = Unpooled.buffer();
        try {
            Long value = 1467072000000L;
            int written = pgType.writeAsBinary(buffer, value);
            int length = buffer.readInt();
            assertThat(written - 4).isEqualTo(length);
            Long readValue = (Long) pgType.readBinaryValue(buffer, length);
            assertThat(readValue).isEqualTo(value);
        } finally {
            buffer.release();
        }
    }

    @Test
    public void testEncodeAsUTF8Text() {
        assertThat(new String(DateType.INSTANCE.encodeAsUTF8Text(1467072000000L), UTF_8)).isEqualTo("2016-06-28");
        assertThat(new String(DateType.INSTANCE.encodeAsUTF8Text(-93661920000000L), UTF_8)).isEqualTo("1000-12-22");
    }

    @Test
    public void testDecodeUTF8TextWithUnexpectedFormat() {
        Assertions.assertThatThrownBy(() -> DateType.INSTANCE.decodeUTF8Text("2016.06.28".getBytes(UTF_8)))
            .isExactlyInstanceOf(DateTimeParseException.class)
            .hasMessageContaining("");
    }

    @Test
    public void testDecodeUTF8Text() {
        assertThat(DateType.INSTANCE.decodeUTF8Text("2020-02-09".getBytes(UTF_8))).isEqualTo(1581206400000L);
    }

}

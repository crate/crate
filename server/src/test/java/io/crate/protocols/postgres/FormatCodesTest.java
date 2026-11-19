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


package io.crate.protocols.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class FormatCodesTest {

    private static FormatCodes.FormatCode[] readFormatCodes(int count, FormatCodes.FormatCode code) {
        ByteBuf buffer = Unpooled.buffer(2 + count * 2);
        try {
            buffer.writeShort(count); // writes the low 16 bits, like a client sending an unsigned int16
            for (int i = 0; i < count; i++) {
                buffer.writeShort(code.ordinal());
            }
            return FormatCodes.fromBuffer(buffer);
        } finally {
            buffer.release();
        }
    }

    @Test
    public void test_zero_format_codes_returns_empty_array() {
        assertThat(readFormatCodes(0, FormatCodes.FormatCode.TEXT)).isEmpty();
    }

    @Test
    public void test_max_signed_short_format_codes() {
        assertThat(readFormatCodes(32_767, FormatCodes.FormatCode.BINARY)).hasSize(32_767);
    }

    @Test
    public void test_format_code_count_above_32767_is_read_as_unsigned() {
        FormatCodes.FormatCode[] codes = readFormatCodes(32_768, FormatCodes.FormatCode.BINARY);
        assertThat(codes).hasSize(32_768);
        assertThat(codes[32_767]).isEqualTo(FormatCodes.FormatCode.BINARY);
    }

    @Test
    public void test_max_unsigned_short_format_codes() {
        assertThat(readFormatCodes(65_535, FormatCodes.FormatCode.TEXT)).hasSize(65_535);
    }
}

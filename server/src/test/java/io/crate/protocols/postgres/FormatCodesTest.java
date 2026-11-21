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

import io.crate.protocols.postgres.FormatCodes.FormatCode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class FormatCodesTest {

    @Test
    public void test_from_buffer_with_zero_format_codes() {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeShort(0);
        FormatCode[] formatCodes = FormatCodes.fromBuffer(buffer);
        assertThat(formatCodes).isEmpty();
    }

    @Test
    public void test_from_buffer_with_regular_format_codes() {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeShort(2);
        buffer.writeShort(0); // TEXT
        buffer.writeShort(1); // BINARY
        FormatCode[] formatCodes = FormatCodes.fromBuffer(buffer);
        assertThat(formatCodes).containsExactly(FormatCode.TEXT, FormatCode.BINARY);
    }

    @Test
    public void test_from_buffer_with_more_than_32767_format_codes() {
        int count = 33000;
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeShort(count);
        for (int i = 0; i < count; i++) {
            buffer.writeShort(0);
        }
        FormatCode[] formatCodes = FormatCodes.fromBuffer(buffer);
        assertThat(formatCodes.length).isEqualTo(count);
        assertThat(formatCodes[0]).isEqualTo(FormatCode.TEXT);
    }
}

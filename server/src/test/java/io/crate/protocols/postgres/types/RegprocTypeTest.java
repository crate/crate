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

import static io.crate.protocols.postgres.types.PGType.INT32_BYTE_SIZE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;

import org.junit.Test;

import io.crate.types.Regproc;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class RegprocTypeTest extends BasePGTypeTest<Regproc> {

    public RegprocTypeTest() {
        super(RegprocType.INSTANCE);
    }

    @Test
    public void test_write_binary_value_writes_only_oid() {
        var regproc = Regproc.of(1234, "1234");
        assertBytesWritten(
            regproc,
            new byte[]{0, 0, 0, 4,0, 0, 4, -46},
            RegprocType.INSTANCE.typeLen() + INT32_BYTE_SIZE);
    }

    @Test
    public void test_read_binary_value_reads_only_oid() {
        var oid = 1234;
        var regproc = Regproc.of(oid, String.valueOf(oid));
        assertBytesReadBinary(
            ByteBuffer.allocate(4).putInt(regproc.oid()).array(),
            regproc
        );
    }

    @Test
    public void test_read_and_write_text_value() {
        var regproc = Regproc.of("func");
        ByteBuf buffer = Unpooled.buffer();
        try {
            //noinspection unchecked
            pgType.writeAsText(buffer, regproc);
            assertThat(
                pgType.readTextValue(buffer, buffer.readInt())).isEqualTo(regproc);
        } finally {
            buffer.release();
        }
    }

    @Test
    public void test_encode_as_utf8() {
        var regproc = Regproc.of("test");
        //noinspection unchecked
        byte[] bytes = pgType.encodeAsUTF8Text(regproc);
        assertThat(new String(bytes, UTF_8)).isEqualTo("test");
    }
}

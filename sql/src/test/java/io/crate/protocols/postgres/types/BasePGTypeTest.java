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

import io.crate.protocols.postgres.FormatCodes;
import io.crate.test.integration.CrateUnitTest;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import static org.hamcrest.Matchers.is;

public abstract class BasePGTypeTest<T> extends CrateUnitTest {

    private PGType pgType;

    BasePGTypeTest(PGType pgType) {
        this.pgType = pgType;
    }

    void assertBytesWritten(Object value, byte[] expectedBytes) {
        assertBytesWritten(value, expectedBytes, PGType.INT32_BYTE_SIZE + pgType.typeLen());
    }

    void assertBytesWritten(Object value, byte[] expectedBytes, int expectedLength) {
        ChannelBuffer writeBuffer = ChannelBuffers.dynamicBuffer();
        int bytesWritten = pgType.writeAsBytes(writeBuffer, value);
        assertThat(bytesWritten, is(expectedLength));

        byte[] bytes = new byte[expectedLength];
        writeBuffer.getBytes(0, bytes);
        assertThat(bytes, is(expectedBytes));
    }

    void assertBytesReadBinary(byte[] value, T expectedValue) {
        assertBytesReadBinary(value, expectedValue, pgType.typeLen());
    }

    void assertBytesReadBinary(byte[] value, T expectedValue, int pos) {
        assertBytesRead(value, expectedValue, pos, FormatCodes.FormatCode.BINARY);
    }

    void assertBytesReadText(byte[] value, T expectedValue) {
        assertBytesReadText(value, expectedValue, pgType.typeLen());
    }

    void assertBytesReadText(byte[] value, T expectedValue, int pos) {
        assertBytesRead(value, expectedValue, pos, FormatCodes.FormatCode.TEXT);
    }

    @SuppressWarnings("unchecked")
    private void assertBytesRead(byte[] value, T expectedValue, int pos, FormatCodes.FormatCode formatCode) {
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(value);
        T readValue;
        if (formatCode == FormatCodes.FormatCode.BINARY) {
            readValue = (T) pgType.readBinaryValue(buffer, pos);
        } else {
            readValue = (T) pgType.readTextValue(buffer, pos);
        }
        assertThat(readValue, is(expectedValue));
    }
}

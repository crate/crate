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

package io.crate.protocols.postgres;

import com.google.common.base.Throwables;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.jboss.netty.buffer.ChannelBuffer;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

abstract class PGType {

    private static final int INT32_BYTE_SIZE = Integer.SIZE / 8;

    final int oid;
    final int typeLen;
    final int typeMod;

    static class FormatCode {
        static final short TEXT = 0;
        static final short BINARY = 1;
    }

    /**
     */
    private PGType(int oid, int typeLen, int typeMod) {
        this.oid = oid;
        this.typeLen = typeLen;
        this.typeMod = typeMod;
    }

    /**
     * write | int32 len | byteN value onto the buffer
     *
     * @return the number of bytes written. (4 + N)
     */
    abstract int writeTextValue(ChannelBuffer buffer, @Nonnull Object value);

    abstract Object readTextValue(ChannelBuffer buffer, int valueLength);

    abstract int writeBinaryValue(ChannelBuffer buffer, @Nonnull Object value);

    abstract Object readBinaryValue(ChannelBuffer buffer, int valueLength);

    static class StringType extends PGType {

        final static int OID = 1043;

        StringType() {
            super(OID, -1, -1);
        }

        @Override
        int writeTextValue(ChannelBuffer buffer, @Nonnull Object value) {
            BytesRef bytesRef = (BytesRef) value;
            buffer.writeInt(bytesRef.length);
            buffer.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            return INT32_BYTE_SIZE + bytesRef.length;
        }

        @Override
        Object readTextValue(ChannelBuffer buffer, int valueLength) {
            BytesRef bytesRef = new BytesRef(valueLength);
            bytesRef.length = valueLength;
            buffer.readBytes(bytesRef.bytes);
            return bytesRef;
        }

        @Override
        int writeBinaryValue(ChannelBuffer buffer, @Nonnull Object value) {
            return writeTextValue(buffer, value);
        }

        @Override
        Object readBinaryValue(ChannelBuffer buffer, int valueLength) {
            return readTextValue(buffer, valueLength);
        }
    }

    static class BooleanType extends PGType {

        final static int OID = 16;

        private final static List<String> TRUTH_VALUES = Arrays.asList(
            "t", "true", "o", "on", "1");

        BooleanType() {
            super(OID, 1, -1);
        }

        @Override
        int writeTextValue(ChannelBuffer buffer, @Nonnull Object value) {
            byte byteValue = (byte) ((boolean) value ? 't' : 'f');
            buffer.writeInt(typeLen);
            buffer.writeByte(byteValue);
            return INT32_BYTE_SIZE + typeLen;
        }

        @Override
        Object readTextValue(ChannelBuffer buffer, int valueLength) {
            byte[] bytes = new byte[valueLength];
            buffer.readBytes(bytes);
            String value = new String(bytes).toLowerCase();
            return TRUTH_VALUES.contains(value);
        }

        @Override
        int writeBinaryValue(ChannelBuffer buffer, @Nonnull Object value) {
            byte byteValue = (byte) ((boolean) value ? 1 : 0);
            buffer.writeInt(typeLen);
            buffer.writeByte(byteValue);
            return INT32_BYTE_SIZE + typeLen;
        }

        @Override
        Object readBinaryValue(ChannelBuffer buffer, int valueLength) {
            byte value = buffer.readByte();
            return value != 0;
        }
    }

    static class JsonType extends PGType {

        final static int OID = 114;

        JsonType() {
            super(OID, -1, -1);
        }

        @Override
        int writeTextValue(ChannelBuffer buffer, @Nonnull Object value) {
            try {
                XContentBuilder builder = JsonXContent.contentBuilder();
                builder.map((Map) value);
                builder.close();
                BytesReference bytes = builder.bytes();
                buffer.writeInt(bytes.length());
                buffer.writeBytes(bytes.toChannelBuffer());
                return INT32_BYTE_SIZE + bytes.length();
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        Object readTextValue(ChannelBuffer buffer, int valueLength) {
            byte[] bytes = new byte[valueLength];
            buffer.readBytes(bytes);
            try {
                return JsonXContent.jsonXContent.createParser(bytes).map();
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        int writeBinaryValue(ChannelBuffer buffer, @Nonnull Object value) {
            return writeTextValue(buffer, value);
        }

        @Override
        Object readBinaryValue(ChannelBuffer buffer, int valueLength) {
            return readTextValue(buffer, valueLength);
        }
    }
}

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
import java.util.Map;

abstract class PGType {

    private static final int INT32_BYTE_SIZE = Integer.SIZE / 8;

    final int oid;
    final int typeLen;
    final int typeMod;
    final FormatCode formatCode;

    enum FormatCode {
        TEXT,   // 0
        BINARY  // 1
    }

    /**
     */
    private PGType(int oid,  int typeLen, int typeMod, FormatCode formatCode) {
        this.oid = oid;
        this.typeLen = typeLen;
        this.typeMod = typeMod;
        this.formatCode = formatCode;
    }

    /**
     * write | int32 len | byteN value onto the buffer
     * @return the number of bytes written. (4 + N)
     */
    abstract int writeValue(ChannelBuffer buffer, @Nonnull Object value);

    abstract Object readValue(ChannelBuffer buffer, int valueLength);

    static class StringType extends PGType {

        final static int OID = 1043;

        StringType() {
            super(OID, -1, -1, FormatCode.BINARY);
        }

        @Override
        int writeValue(ChannelBuffer buffer, @Nonnull Object value) {
            BytesRef bytesRef = (BytesRef) value;
            buffer.writeInt(bytesRef.length);
            buffer.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            return INT32_BYTE_SIZE + bytesRef.length;
        }

        @Override
        Object readValue(ChannelBuffer buffer, int valueLength) {
            BytesRef bytesRef = new BytesRef(valueLength);
            bytesRef.length = valueLength;
            buffer.readBytes(bytesRef.bytes);
            return bytesRef;
        }
    }

    static class BooleanType extends PGType {

        final static int OID = 16;

        BooleanType() {
            super(OID, 1, -1, FormatCode.BINARY);
        }

        @Override
        int writeValue(ChannelBuffer buffer, @Nonnull Object value) {
            byte byteValue = (byte) ((boolean) value ? 1 : 0);
            buffer.writeInt(typeLen);
            buffer.writeByte(byteValue);
            return INT32_BYTE_SIZE + typeLen;
        }

        @Override
        Object readValue(ChannelBuffer buffer, int valueLength) {
            byte value = buffer.readByte();
            return value != 0;
        }
    }

    static class JsonType extends PGType {

        final static int OID = 114;

        JsonType() {
            super(OID, -1, -1, FormatCode.TEXT);
        }

        @Override
        int writeValue(ChannelBuffer buffer, @Nonnull Object value) {
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
        Object readValue(ChannelBuffer buffer, int valueLength) {
            byte[] bytes = new byte[valueLength];
            buffer.readBytes(bytes);
            try {
                return JsonXContent.jsonXContent.createParser(bytes).map();
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}

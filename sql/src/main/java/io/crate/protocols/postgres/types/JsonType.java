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

import com.google.common.base.Throwables;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.jboss.netty.buffer.ChannelBuffer;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;

class JsonType extends PGType {

    static final int OID = 114;

    private static final int TYPE_LEN = -1;
    private static final int TYPE_MOD = -1;

    JsonType() {
        super(OID, TYPE_LEN, TYPE_MOD);
    }

    @Override
    public int writeAsBytes(ChannelBuffer buffer, @Nonnull Object value) {
        byte[] bytes = asUTF8StringBytes(value);
        buffer.writeInt(bytes.length);
        buffer.writeBytes(bytes);
        return INT32_BYTE_SIZE + bytes.length;
    }

    @Override
    protected byte[] asUTF8StringBytes(@Nonnull Object value) {
        try {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.map((Map) value);
            builder.close();
            BytesReference bytes = builder.bytes();
            return bytes.toBytes();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Object readBinaryValue(ChannelBuffer buffer, int valueLength) {
        byte[] bytes = new byte[valueLength];
        buffer.readBytes(bytes);
        return valueFromUTF8Bytes(bytes);
    }

    @Override
    Object valueFromUTF8Bytes(byte[] bytes) {
        try {
            return JsonXContent.jsonXContent.createParser(bytes).map();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}

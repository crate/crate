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

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import io.netty.buffer.ByteBuf;

public class PgUUIDType extends PGType<UUID> {

    public static final int OID = 2950;
    public static final PgUUIDType INSTANCE = new PgUUIDType();

    public PgUUIDType() {
        super(OID, 16, -1, "uuid");
    }

    @Override
    public int typArray() {
        return PGArray.UUID_ARRAY.oid();
    }

    @Override
    public String typeCategory() {
        return TypeCategory.USER_DEFINED_TYPES.code();
    }

    @Override
    public String type() {
        return Type.BASE.code();
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, UUID value) {
        long mostSignificantBits = value.getMostSignificantBits();
        long leastSignificantBits = value.getLeastSignificantBits();

        buffer.writeInt(typeLen);
        buffer.writeLong(mostSignificantBits);
        buffer.writeLong(leastSignificantBits);

        return INT32_BYTE_SIZE + typeLen;
    }

    @Override
    public UUID readBinaryValue(ByteBuf buffer, int valueLength) {
        long mostSigBits = buffer.readLong();
        long leastSigBits = buffer.readLong();
        return new UUID(mostSigBits, leastSigBits);
    }

    @Override
    byte[] encodeAsUTF8Text(UUID value) {
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    UUID decodeUTF8Text(byte[] bytes) {
        return UUID.fromString(new String(bytes, StandardCharsets.UTF_8));
    }
}

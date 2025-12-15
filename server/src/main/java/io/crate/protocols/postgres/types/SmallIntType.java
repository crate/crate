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

import io.netty.buffer.ByteBuf;

class SmallIntType extends PGType<Short> {

    public static final SmallIntType INSTANCE = new SmallIntType();
    static final int OID = 21;

    private static final int TYPE_LEN = 2;
    private static final int TYPE_MOD = -1;

    private SmallIntType() {
        super(OID, TYPE_LEN, TYPE_MOD, "int2");
    }

    @Override
    public int typArray() {
        return PGArray.INT2_ARRAY.oid();
    }

    @Override
    public String typeCategory() {
        return TypeCategory.NUMERIC.code();
    }

    @Override
    public String type() {
        return Type.BASE.code();
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, Short value) {
        buffer.writeInt(TYPE_LEN);
        buffer.writeShort(value);
        return INT32_BYTE_SIZE + TYPE_LEN;
    }

    @Override
    protected byte[] encodeAsUTF8Text(Short value) {
        return Short.toString(value).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Short readBinaryValue(ByteBuf buffer, int valueLength) {
        assert
            valueLength == TYPE_LEN :
            "length should be " + TYPE_LEN + " because short is int16. Actual length: " + valueLength;
        return buffer.readShort();
    }

    @Override
    Short decodeUTF8Text(byte[] bytes) {
        return Short.parseShort(new String(bytes, StandardCharsets.UTF_8));
    }
}

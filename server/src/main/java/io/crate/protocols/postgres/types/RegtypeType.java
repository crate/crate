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

import io.crate.metadata.RelationLookup;
import io.crate.types.Regtype;
import io.netty.buffer.ByteBuf;

class RegtypeType extends PGType<Regtype> {

    static final int OID = 2206;

    private static final int TYPE_LEN = 4;
    private static final int TYPE_MOD = -1;

    public static final RegtypeType INSTANCE = new RegtypeType();

    private RegtypeType() {
        super(OID, TYPE_LEN, TYPE_MOD, "regtype");
    }

    @Override
    public int typArray() {
        return PGArray.REGTYPE_ARRAY.oid();
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
    public int writeAsBinary(ByteBuf buffer, Regtype value) {
        buffer.writeInt(TYPE_LEN);
        buffer.writeInt(value.oid());
        return INT32_BYTE_SIZE + TYPE_LEN;
    }

    @Override
    public Regtype readBinaryValue(ByteBuf buffer, int valueLength) {
        int oid = buffer.readInt();
        return new Regtype(oid);
    }

    @Override
    byte[] encodeAsUTF8Text(Regtype value) {
        return String.valueOf(value.name()).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    Regtype decodeUTF8Text(byte[] bytes, RelationLookup relationLookup) {
        String str = new String(bytes, StandardCharsets.UTF_8);
        try {
            int oid = Integer.parseInt(str);
            return new Regtype(oid);
        } catch (NumberFormatException e) {
            return new Regtype(str);
        }
    }
}

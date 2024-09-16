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

import io.crate.metadata.IndexName;
import io.crate.types.Regclass;
import io.netty.buffer.ByteBuf;

public class RegclassType extends PGType<Regclass> {

    static final int OID = 2205;

    private static final int TYPE_LEN = 4;
    private static final int TYPE_MOD = -1;

    public static final RegclassType INSTANCE = new RegclassType();

    private RegclassType() {
        super(OID, TYPE_LEN, TYPE_MOD, "regclass");
    }

    @Override
    public int typArray() {
        return PGArray.REGCLASS_ARRAY.oid();
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
    public int writeAsBinary(ByteBuf buffer, Regclass value) {
        buffer.writeInt(TYPE_LEN);
        buffer.writeInt(value.oid());
        return INT32_BYTE_SIZE + TYPE_LEN;
    }

    @Override
    public Regclass readBinaryValue(ByteBuf buffer, int valueLength) {
        int oid = buffer.readInt();
        return new Regclass(oid, String.valueOf(oid));
    }

    @Override
    byte[] encodeAsUTF8Text(Regclass value) {
        return String.valueOf(value.oid()).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    Regclass decodeUTF8Text(byte[] bytes) {
        String oidStr = new String(bytes, StandardCharsets.UTF_8);
        try {
            int oid = Integer.parseInt(oidStr);
            return new Regclass(oid, oidStr);
        } catch (NumberFormatException e) {
            var indexParts = IndexName.decode(oidStr);
            return Regclass.fromRelationName(indexParts.toRelationName());
        }
    }
}

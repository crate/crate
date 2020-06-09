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


import io.crate.metadata.pgcatalog.OidHash;
import io.crate.types.Regproc;
import io.netty.buffer.ByteBuf;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;

class RegprocType extends PGType<Regproc> {

    static final int OID = 24;

    private static final int TYPE_LEN = 4;
    private static final int TYPE_MOD = -1;

    public static final RegprocType INSTANCE = new RegprocType();

    private RegprocType() {
        super(OID, TYPE_LEN, TYPE_MOD, "regproc");
    }

    @Override
    public int typArray() {
        return PGArray.REGPROC_ARRAY.oid();
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
    public int writeAsBinary(ByteBuf buffer, @Nonnull Regproc value) {
        buffer.writeInt(TYPE_LEN);
        buffer.writeBytes(value.name().getBytes());
        return value.name().length() + TYPE_LEN;
    }

    @Override
    public int writeAsText(ByteBuf buffer, @Nonnull Regproc value) {
        return writeAsBinary(buffer, value);
    }

    @Override
    public Regproc readBinaryValue(ByteBuf buffer, int valueLength) {
        byte[] utf8 = new byte[valueLength];
        buffer.readBytes(utf8);
        var name = new String(utf8, StandardCharsets.UTF_8);
        return Regproc.of(OidHash.functionOid(name), name);
    }

    @Override
    byte[] encodeAsUTF8Text(@Nonnull Regproc value) {
        return value.name().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    Regproc decodeUTF8Text(byte[] bytes) {
        var name = new String(bytes, StandardCharsets.UTF_8);
        return Regproc.of(OidHash.functionOid(name), name);
    }
}

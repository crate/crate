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

import org.jetbrains.annotations.NotNull;

import io.crate.types.DataTypes;
import io.crate.types.Regproc;
import io.netty.buffer.ByteBuf;

public class UnknownType extends PGType<Object> {

    public static final UnknownType INSTANCE = new UnknownType();
    static final int OID = 705;

    UnknownType() {
        super(OID, -2, -1, "unknown");
    }

    @Override
    public int typArray() {
        return 0;
    }

    @Override
    public String typeCategory() {
        return TypeCategory.UNKNOWN.code();
    }

    @Override
    public String type() {
        return Type.PSEUDO.code();
    }

    @Override
    public Regproc typReceive() {
        return Regproc.of("unknownrecv");
    }

    @Override
    public Regproc typSend() {
        return Regproc.of("unknownsend");
    }

    @Override
    public Regproc typOutput() {
        return Regproc.of("unknownout");
    }

    @Override
    public Regproc typInput() {
        return Regproc.of("unknownin");
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, @NotNull Object value) {
        String string = DataTypes.STRING.implicitCast(value);
        int writerIndex = buffer.writerIndex();
        buffer.writeInt(0);
        int bytesWritten = buffer.writeCharSequence(string, StandardCharsets.UTF_8);
        buffer.setInt(writerIndex, bytesWritten);
        return INT32_BYTE_SIZE + bytesWritten;
    }

    @Override
    public String readBinaryValue(ByteBuf buffer, int valueLength) {
        int readerIndex = buffer.readerIndex();
        buffer.readerIndex(readerIndex + valueLength);
        return buffer.toString(readerIndex, valueLength, StandardCharsets.UTF_8);
    }

    @Override
    byte[] encodeAsUTF8Text(@NotNull Object value) {
        return DataTypes.STRING.implicitCast(value).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    String decodeUTF8Text(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}

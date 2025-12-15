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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.crate.types.Regproc;
import io.netty.buffer.ByteBuf;

public abstract class PGType<T> {

    enum Type {

        BASE("b"),
        COMPOSITE("c"),
        ENUM("e"),
        DOMAIN("d"),
        PSEUDO("p"),
        RANGE("r");

        private final String code;

        Type(String code) {
            this.code = code;
        }

        public String code() {
            return code;
        }
    }

    enum TypeCategory {

        ARRAY("A"),
        BOOLEAN("B"),
        COMPOSITE("C"),
        DATETIME("D"),
        GEOMETRIC("G"),
        NETWORK("I"),
        NUMERIC("N"),
        PSEUDO("P"),
        RANGE("R"),
        STRING("S"),
        TIMESPAN("T"),
        USER_DEFINED_TYPES("U"),
        BIT_STRING("V"),
        UNKNOWN("X");

        private final String code;

        TypeCategory(String code) {
            this.code = code;
        }

        public String code() {
            return code;
        }
    }

    static final int INT32_BYTE_SIZE = Integer.SIZE / 8;
    private static final Logger LOGGER = LogManager.getLogger(PGType.class);

    private final int oid;
    protected final int typeLen;
    private final int typeMod;
    private final String typName;

    PGType(int oid, int typeLen, int typeMod, String typName) {
        this.oid = oid;
        this.typeLen = typeLen;
        this.typeMod = typeMod;
        this.typName = typName;
    }

    public int oid() {
        return oid;
    }

    public short typeLen() {
        return (short) typeLen;
    }

    public abstract int typArray();

    public int typeMod() {
        return typeMod;
    }

    public String typName() {
        return typName;
    }

    public Regproc typInput() {
        if (typArray() == 0) {
            return Regproc.of("array_in");
        } else {
            return Regproc.of(typName() + "_in");
        }
    }

    public Regproc typOutput() {
        if (typArray() == 0) {
            return Regproc.of("array_out");
        } else {
            return Regproc.of(typName() + "_out");
        }
    }

    public Regproc typReceive() {
        if (typArray() == 0) {
            return Regproc.of("array_recv");
        } else {
            return Regproc.of(typName() + "recv");
        }
    }

    public Regproc typSend() {
        if (typArray() == 0) {
            return Regproc.of("array_send");
        } else {
            return Regproc.of(typName() + "send");
        }
    }

    public int typElem() {
        return 0;
    }

    public String typDelim() {
        return ",";
    }

    public abstract String typeCategory();

    public abstract String type();

    /**
     * Write the value as text into the buffer.
     * <p>
     * Format:
     * <pre>
     *  | int32 len (excluding len itself) | byte<b>N</b> value onto the buffer
     *  </pre>
     *
     * @return the number of bytes written. (4 (int32)  + N)
     */
    public int writeAsText(ByteBuf buffer, T value) {
        byte[] bytes = encodeAsUTF8Text(value);
        buffer.writeInt(bytes.length);
        buffer.writeBytes(bytes);
        return INT32_BYTE_SIZE + bytes.length;
    }

    public T readTextValue(ByteBuf buffer, int valueLength) {
        byte[] bytes = new byte[valueLength];
        buffer.readBytes(bytes);
        try {
            return decodeUTF8Text(bytes);
        } catch (Throwable t) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("decodeUTF8Text failed. input={} type={}",
                    new String(bytes, StandardCharsets.UTF_8), typName);
            }
            throw t;
        }
    }

    /**
     * Write the value as binary into the buffer.
     * <p>
     * Format:
     * <pre>
     *  | int32 len (excluding len itself) | byte<b>N</b> value onto the buffer
     *  </pre>
     *
     * @return the number of bytes written. (4 (int32)  + N)
     */
    public abstract int writeAsBinary(ByteBuf buffer, T value);

    public abstract T readBinaryValue(ByteBuf buffer, int valueLength);


    /**
     * Return the UTF8 encoded text representation of the value
     */
    abstract byte[] encodeAsUTF8Text(T value);

    /**
     * Convert a UTF8 encoded text representation into the actual value
     */
    abstract T decodeUTF8Text(byte[] bytes);

}

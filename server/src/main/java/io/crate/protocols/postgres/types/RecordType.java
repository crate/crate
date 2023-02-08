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

import java.util.List;

import com.carrotsearch.hppc.ByteArrayList;

import io.crate.data.Row;
import io.crate.types.Regproc;
import io.netty.buffer.ByteBuf;

public class RecordType extends PGType<Row> {

    static final int OID = 2249;
    static final String NAME = "record";

    static final RecordType EMPTY_RECORD = new RecordType(List.of());

    private final List<PGType<?>> fieldTypes;

    RecordType(List<PGType<?>> fieldTypes) {
        super(OID, -1, -1, NAME);
        this.fieldTypes = fieldTypes;
    }

    @Override
    public int typArray() {
        return 2287;
    }

    @Override
    public String typeCategory() {
        return TypeCategory.PSEUDO.code();
    }

    @Override
    public String type() {
        return Type.PSEUDO.code();
    }

    @Override
    public Regproc typSend() {
        return Regproc.of(NAME + "_send");
    }

    @Override
    public Regproc typReceive() {
        return Regproc.of(NAME + "_recv");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public int writeAsBinary(ByteBuf buffer, Row record) {
        final int startWriterIndex = buffer.writerIndex();
        buffer.writeInt(0); // reserve space for the length of the record; updated later
        buffer.writeInt(fieldTypes.size());
        int bytesWritten = 4;
        for (int i = 0; i < fieldTypes.size(); i++) {
            PGType fieldType = fieldTypes.get(i);

            buffer.writeInt(fieldType.oid());
            bytesWritten += 4;

            var value = record.get(i);
            if (value == null) {
                buffer.writeInt(-1); // -1 data length signals a NULL
                continue;
            }

            bytesWritten += fieldType.writeAsBinary(buffer, value);
        }
        buffer.setInt(startWriterIndex, bytesWritten);
        return 4 + bytesWritten;
    }

    @Override
    public Row readBinaryValue(ByteBuf buffer, int valueLength) {
        throw new UnsupportedOperationException("Input of anonymous record type values is not implemented");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    byte[] encodeAsUTF8Text(Row record) {
        ByteArrayList bytes = new ByteArrayList();
        // See PostgreSQL src/backend/utils/adt/rowtypes.c record_out(PG_FUNCTION_ARGS)
        bytes.add((byte) '(');
        for (int i = 0; i < record.numColumns(); i++) {
            PGType fieldType = fieldTypes.get(i);
            var value = record.get(i);

            if (i > 0) {
                bytes.add((byte) ',');
            }
            if (value == null) {
                continue;
            }

            byte[] encodedValue = fieldType.encodeAsUTF8Text(value);
            boolean needQuotes = encodedValue.length == 0;
            for (int j = 0; j < encodedValue.length; j++) {
                char c = (char) encodedValue[j];
                if (c == '"' || c == '\\' || c == '(' || c == ')' || c == ',' || Character.isWhitespace(c)) {
                    needQuotes = true;
                    break;
                }
            }
            if (needQuotes) {
                bytes.add((byte) '\"');
            }
            bytes.add(encodedValue);
            if (needQuotes) {
                bytes.add((byte) '\"');
            }
        }
        bytes.add((byte) ')');
        return bytes.toArray();
    }

    @Override
    Row decodeUTF8Text(byte[] bytes) {
        throw new UnsupportedOperationException("Input of record type values is not implemented");
    }
}

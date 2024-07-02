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

package io.crate.protocols.postgres;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

import io.netty.buffer.ByteBuf;

/**
 * Class to encode postgres client messages and write them onto a buffer.
 *
 * For more information about the messages see {@link Messages} and {@link PostgresWireProtocol} or refer to
 * the PostgreSQL protocol documentation.
 */
class ClientMessages {

    static ByteBuf writeSSLReqMessage(ByteBuf buffer) {
        buffer.writeInt(PgDecoder.MIN_STARTUP_LENGTH);
        buffer.writeInt(PgDecoder.SSL_REQUEST_CODE);
        return buffer;
    }

    static ByteBuf writePasswordMessage(ByteBuf buffer, String password) {
        byte[] passwordBytes = password.getBytes(StandardCharsets.UTF_8);
        buffer.writeByte('p');
        buffer.writeInt(4 + passwordBytes.length + 1);
        buffer.writeBytes(passwordBytes);
        buffer.writeByte(0);
        return buffer;
    }

    static ByteBuf sendStartupMessage(ByteBuf buffer, String dbName, Map<String, String> properties) {
        // length itself and protocol version number
        // updated later to include the body
        int length = 8;
        int protocolVersion = 3 << 16;
        final int lengthIndex = buffer.writerIndex();
        buffer.writeInt(length);
        buffer.writeInt(protocolVersion);

        byte[] dbKey = "database".getBytes(StandardCharsets.UTF_8);
        length += dbKey.length + 1;
        buffer.writeBytes(dbKey);
        buffer.writeByte(0);

        byte[] dbValue = dbName.getBytes(StandardCharsets.UTF_8);
        length += dbValue.length + 1;
        buffer.writeBytes(dbValue);
        buffer.writeByte(0);

        for (var entry : properties.entrySet()) {
            byte[] key = entry.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] value = entry.getValue().getBytes(StandardCharsets.UTF_8);
            length += key.length + value.length + 2;

            buffer.writeBytes(key);
            buffer.writeByte(0);
            buffer.writeBytes(value);
            buffer.writeByte(0);
        }
        buffer.setInt(lengthIndex, length);
        return buffer;
    }

    static void sendParseMessage(ByteBuf buffer, String stmtName, String query, int[] paramOids) {
        buffer.writeByte('P');
        byte[] stmtNameBytes = stmtName.getBytes(StandardCharsets.UTF_8);
        byte[] queryBytes = query.getBytes(StandardCharsets.UTF_8);
        buffer.writeInt(
            4 +
            stmtNameBytes.length + 1 +
            queryBytes.length + 1 +
            2 +
            paramOids.length * 4);
        writeCString(buffer, stmtNameBytes);
        writeCString(buffer, queryBytes);
        buffer.writeShort(paramOids.length);
        for (int paramOid : paramOids) {
            buffer.writeInt(paramOid);
        }
    }

    private static void writeCString(ByteBuf buffer, byte[] bytes) {
        buffer.writeBytes(bytes);
        buffer.writeByte(0);
    }

    static void sendFlush(ByteBuf buffer) {
        buffer.writeByte('H');
        buffer.writeInt(4);
    }

    static void sendBindMessage(ByteBuf buffer,
                                String portalName,
                                String statementName,
                                List<Object> params) {
        buffer.writeByte('B');
        byte[] portalBytes = portalName.getBytes(StandardCharsets.UTF_8);
        byte[] statementBytes = statementName.getBytes(StandardCharsets.UTF_8);

        final int beforeLengthWriterIndex = buffer.writerIndex();
        buffer.writeInt(0);
        writeCString(buffer, portalBytes);
        writeCString(buffer, statementBytes);
        buffer.writeShort(0); // formatCode use 0 to default to text for all
        buffer.writeShort(params.size());

        int paramsLength = 0;
        for (Object param : params) {
            BytesRef value = BytesRefs.toBytesRef(param);
            buffer.writeInt(value.length);
            // the strings here are _not_ zero-padded because we specify the length upfront
            buffer.writeBytes(value.bytes, value.offset, value.length);
            paramsLength += 4 + value.length;
        }
        buffer.writeShort(0); // numResultFormatCodes - 0 to default to text for all

        buffer.setInt(beforeLengthWriterIndex,
            4 +
            portalBytes.length + 1 +
            statementBytes.length + 1 +
            2 + // numFormatCodes
            2 + // numParams
            paramsLength +
            2); // numResultColumnFormatCodes
    }

    enum DescribeType {
        PORTAL('P'),
        STATEMENT('S');

        private final char identifier;

        DescribeType(char identifier) {
            this.identifier = identifier;
        }
    }

    static void sendDescribeMessage(ByteBuf buffer,
                                    DescribeType describeType,
                                    String portalOrStatement) {
        byte[] portalOrStatementBytes = portalOrStatement.getBytes(StandardCharsets.UTF_8);
        int length = 4 + 1 + portalOrStatementBytes.length + 1;
        buffer.writeByte('D');
        buffer.writeInt(length);
        buffer.writeByte(describeType.identifier);
        buffer.writeBytes(portalOrStatementBytes);
        buffer.writeByte(0);
    }

    static void sendPasswordMessage(ByteBuf buffer, String password) {
        byte[] passwordBytes = password.getBytes(StandardCharsets.UTF_8);
        int length = 4 + passwordBytes.length + 1;
        buffer.writeByte('p');
        buffer.writeInt(length);
        buffer.writeBytes(passwordBytes);
        buffer.writeByte(0);
    }

    static void sendTermination(ByteBuf buffer) {
        buffer.writeByte('X');
        buffer.writeInt(4);
    }

    static void sendCancelRequest(ByteBuf buffer, KeyData keyData) {
        buffer.writeInt(16);
        buffer.writeInt(1234 << 16 | 5678); // == 80877102
        buffer.writeInt(keyData.pid());
        buffer.writeInt(keyData.secretKey());
    }

    static void sendExecute(ByteBuf buffer, String portalName, int numRows) {
        byte[] portalNameBytes = portalName.getBytes(StandardCharsets.UTF_8);
        int length = 4 + portalNameBytes.length + 1 + 4;
        buffer.writeByte('E');
        buffer.writeInt(length);
        buffer.writeBytes(portalNameBytes);
        buffer.writeByte(0);
        buffer.writeInt(numRows);
    }
}

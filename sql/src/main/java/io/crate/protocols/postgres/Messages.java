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

package io.crate.protocols.postgres;

import io.crate.analyze.symbol.Field;
import io.crate.core.collections.Row;
import io.crate.types.DataType;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

/**
 * Regular data packet is in the following format:
 *
 * +----------+-----------+----------+
 * | char tag | int32 len | payload  |
 * +----------+-----------+----------+
 *
 * The tag indicates the message type, the second field is the length of the packet
 * (excluding the tag, but including the length itself)
 *
 *
 * See https://www.postgresql.org/docs/9.2/static/protocol-message-formats.html
 */
class Messages {

    private final static ESLogger LOGGER = Loggers.getLogger(Messages.class);


    static void sendAuthenticationOK(Channel channel) {
        ChannelBuffer buffer = ChannelBuffers.buffer(9);
        buffer.writeByte('R');
        buffer.writeInt(8); // size excluding char
        buffer.writeInt(0);
        ChannelFuture channelFuture = channel.write(buffer);
        channelFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                LOGGER.trace("sentAuthenticationOK");
            }
        });
    }

    /**
     * | 'C' | int32 len | str commandTag
     *
     * @param commandTag:single word that identifies the SQL command that was completed
     */
    static void sendCommandComplete(Channel channel, String commandTag) {
        byte[] commandTagBytes = commandTag.getBytes(StandardCharsets.UTF_8);
        int length = 4 + commandTagBytes.length + 1;
        ChannelBuffer buffer = ChannelBuffers.buffer(length + 1);
        buffer.writeByte('C');
        buffer.writeInt(length);
        writeCString(buffer, commandTagBytes);
        channel.write(buffer).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                LOGGER.trace("sentCommandComplete");
            }
        });
    }

    /**
     * ReadyForQuery (B)
     *
     * Byte1('Z')
     *      Identifies the message type. ReadyForQuery is sent whenever the
     *      backend is ready for a new query cycle.
     *
     * Int32(5)
     *      Length of message contents in bytes, including self.
     *
     * Byte1
     *      Current backend transaction status indicator. Possible values are
     *      'I' if idle (not in a transaction block); 'T' if in a transaction
     *      block; or 'E' if in a failed transaction block (queries will be
     *      rejected until block is ended).
     */
    static void sendReadyForQuery(Channel channel) {
        ChannelBuffer buffer = ChannelBuffers.buffer(6);
        buffer.writeByte('Z');
        buffer.writeInt(5);
        buffer.writeByte('I');
        channel.write(buffer).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                LOGGER.trace("sentReadyForQuery");
            }
        });
    }

    /**
     * | 'S' | int32 len | str name | str value
     *
     * See https://www.postgresql.org/docs/9.2/static/protocol-flow.html#PROTOCOL-ASYNC
     *
     * > At present there is a hard-wired set of parameters for which ParameterStatus will be generated: they are
     *
     *  - server_version,
     *  - server_encoding,
     *  - client_encoding,
     *  - application_name,
     *  - is_superuser,
     *  - session_authorization,
     *  - DateStyle,
     *  - IntervalStyle,
     *  - TimeZone,
     *  - integer_datetimes,
     *  - standard_conforming_string
     */
    static void sendParameterStatus(Channel channel, final String name, final String value) {
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

        int length = 4 + nameBytes.length + 1 + valueBytes.length + 1;
        ChannelBuffer buffer = ChannelBuffers.buffer(length + 1);
        buffer.writeByte('S');
        buffer.writeInt(length);
        writeCString(buffer, nameBytes);
        writeCString(buffer, valueBytes);
        channel.write(buffer).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                LOGGER.trace("sentParameterStatus {}={}", name, value);
            }
        });
    }

    /**
     * 'E' | int32 len | char code | str value | \0 | char code | str value | \0 | ... | \0
     *
     * char code / str value -> key-value fields
     * example error fields are: message, detail, hint, error position
     *
     * See https://www.postgresql.org/docs/9.2/static/protocol-error-fields.html for a list of error codes
     *
     */
    static void sendErrorResponse(Channel channel, final String message) {
        byte[] msgBytes = message.getBytes(StandardCharsets.UTF_8);
        int length = 4 + 1 + msgBytes.length + 1 + 1;
        ChannelBuffer buffer = ChannelBuffers.buffer(length + 2);
        buffer.writeByte('E');
        buffer.writeInt(length);
        buffer.writeByte('S');
        writeCString(buffer, msgBytes);
        channel.write(buffer).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                LOGGER.trace("sentErrorResponse msg={}", message);
            }
        });
        buffer.writeByte(0);
    }

    /**
     *
     * Byte1('D')
     * Identifies the message as a data row.
     *
     * Int32
     * Length of message contents in bytes, including self.
     *
     * Int16
     * The number of column values that follow (possibly zero).
     *
     * Next, the following pair of fields appear for each column:
     *
     * Int32
     * The length of the column value, in bytes (this count does not include itself).
     * Can be zero. As a special case, -1 indicates a NULL column value. No value bytes follow in the NULL case.
     *
     * ByteN
     * The value of the column, in the format indicated by the associated format code. n is the above length.
     */
    static void sendDataRow(Channel channel, Row row, List<? extends DataType> columnTypes) {
        int length = 4 + 2;

        ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
        buffer.writeByte('D');
        buffer.writeInt(0); // will be set at the end
        buffer.writeShort(row.size());

        for (int i = 0; i < row.size(); i++) {
            DataType dataType = columnTypes.get(i);
            PGType pgType = PGTypes.CRATE_TO_PG_TYPES.get(dataType);
            Object value = row.get(i);
            if (value == null) {
                buffer.writeInt(-1);
            } else {
                length += pgType.writeValue(buffer, value);
            }
        }

        buffer.setInt(1, length);
        channel.write(buffer).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                LOGGER.trace("sentDataRow");
            }
        });
    }

    private static void writeCString(ChannelBuffer buffer, byte[] valBytes) {
        buffer.writeBytes(valBytes);
        buffer.writeByte(0);
    }

    /**
     * RowDescription (B)
     *
     *  | 'T' | int32 len | int16 numCols
     *
     *  For each field:
     *
     *  | string name | int32 table_oid | int16 attr_num | int32 oid | int16 typlen | int32 type_modifier | int16 format_code
     *
     * See https://www.postgresql.org/docs/current/static/protocol-message-formats.html
     */
    static void sendRowDescription(Channel channel, Collection<Field> columns) {
        int length = 4 + 2;
        int columnSize = 4 + 2 + 4 + 2 + 4 + 2;
        ChannelBuffer buffer = ChannelBuffers.dynamicBuffer(
            length + (columns.size() * (10 + columnSize))); // use 10 as an estimate for columnName length

        buffer.writeByte('T');
        buffer.writeInt(0); // will be set at the end
        buffer.writeShort(columns.size());

        for (Field column : columns) {
            byte[] nameBytes = column.path().outputName().getBytes(StandardCharsets.UTF_8);
            length += nameBytes.length + 1;
            length += columnSize;

            writeCString(buffer, nameBytes);
            buffer.writeInt(0);     // table_oid
            buffer.writeShort(0);   // attr_num

            PGType pgType = PGTypes.CRATE_TO_PG_TYPES.get(column.valueType());
            buffer.writeInt(pgType.oid);
            buffer.writeShort(pgType.typeLen);
            buffer.writeInt(pgType.typeMod);
            buffer.writeShort(pgType.formatCode);
        }

        buffer.setInt(1, length);
        channel.write(buffer).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                LOGGER.trace("sentRowDescription");
            }
        });
    }

    /**
     * ParseComplete
     * | '1' | int32 len |
     */
    static void sendParseComplete(Channel channel) {
        ChannelBuffer buffer = ChannelBuffers.buffer(5);
        buffer.writeByte('1');
        buffer.writeInt(4);

        channel.write(buffer).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                LOGGER.trace("sentParseComplete");
            }
        });
    }

    /**
     * BindComplete
     * | '2' | int32 len |
     */
    static void sendBindComplete(Channel channel) {
        ChannelBuffer buffer = ChannelBuffers.buffer(5);
        buffer.writeByte('2');
        buffer.writeInt(4);

        channel.write(buffer).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                LOGGER.trace("sentBindComplete");
            }
        });
    }
}

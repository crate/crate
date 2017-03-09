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
import io.crate.data.Row;
import io.crate.exceptions.SQLExceptions;
import io.crate.protocols.postgres.types.PGType;
import io.crate.protocols.postgres.types.PGTypes;
import io.crate.types.DataType;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * Regular data packet is in the following format:
 * <p>
 * +----------+-----------+----------+
 * | char tag | int32 len | payload  |
 * +----------+-----------+----------+
 * <p>
 * The tag indicates the message type, the second field is the length of the packet
 * (excluding the tag, but including the length itself)
 * <p>
 * <p>
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
        if (LOGGER.isTraceEnabled()) {
            channelFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    LOGGER.trace("sentAuthenticationOK");
                }
            });
        }
    }

    /**
     * | 'C' | int32 len | str commandTag
     *
     * @param query    :the query
     * @param rowCount : number of rows in the result set or number of rows affected by the DML statement
     */
    static void sendCommandComplete(Channel channel, String query, long rowCount) {
        query = query.split(" ", 2)[0].toUpperCase(Locale.ENGLISH);
        String commandTag;
        /*
         * from https://www.postgresql.org/docs/current/static/protocol-message-formats.html:
         *
         * For an INSERT command, the tag is INSERT oid rows, where rows is the number of rows inserted.
         * oid is the object ID of the inserted row if rows is 1 and the target table has OIDs; otherwise oid is 0.
         */
        if ("BEGIN".equals(query)) {
            commandTag = "BEGIN";
        } else if ("INSERT".equals(query)) {
            commandTag = "INSERT 0 " + rowCount;
        } else {
            commandTag = query + " " + rowCount;
        }

        byte[] commandTagBytes = commandTag.getBytes(StandardCharsets.UTF_8);
        int length = 4 + commandTagBytes.length + 1;
        ChannelBuffer buffer = ChannelBuffers.buffer(length + 1);
        buffer.writeByte('C');
        buffer.writeInt(length);
        writeCString(buffer, commandTagBytes);
        ChannelFuture channelFuture = channel.write(buffer);
        if (LOGGER.isTraceEnabled()) {
            channelFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    LOGGER.trace("sentCommandComplete");
                }
            });
        }
    }

    /**
     * ReadyForQuery (B)
     * <p>
     * Byte1('Z')
     * Identifies the message type. ReadyForQuery is sent whenever the
     * backend is ready for a new query cycle.
     * <p>
     * Int32(5)
     * Length of message contents in bytes, including self.
     * <p>
     * Byte1
     * Current backend transaction status indicator. Possible values are
     * 'I' if idle (not in a transaction block); 'T' if in a transaction
     * block; or 'E' if in a failed transaction block (queries will be
     * rejected until block is ended).
     */
    static void sendReadyForQuery(Channel channel) {
        ChannelBuffer buffer = ChannelBuffers.buffer(6);
        buffer.writeByte('Z');
        buffer.writeInt(5);
        buffer.writeByte('I');
        ChannelFuture channelFuture = channel.write(buffer);
        if (LOGGER.isTraceEnabled()) {
            channelFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    LOGGER.trace("sentReadyForQuery");
                }
            });
        }
    }

    /**
     * | 'S' | int32 len | str name | str value
     * <p>
     * See https://www.postgresql.org/docs/9.2/static/protocol-flow.html#PROTOCOL-ASYNC
     * <p>
     * > At present there is a hard-wired set of parameters for which ParameterStatus will be generated: they are
     * <p>
     * - server_version,
     * - server_encoding,
     * - client_encoding,
     * - application_name,
     * - is_superuser,
     * - session_authorization,
     * - DateStyle,
     * - IntervalStyle,
     * - TimeZone,
     * - integer_datetimes,
     * - standard_conforming_string
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
        ChannelFuture channelFuture = channel.write(buffer);
        if (LOGGER.isTraceEnabled()) {
            channelFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    LOGGER.trace("sentParameterStatus {}={}", name, value);
                }
            });
        }
    }

    /**
     * 'E' | int32 len | char code | str value | \0 | char code | str value | \0 | ... | \0
     * <p>
     * char code / str value -> key-value fields
     * example error fields are: message, detail, hint, error position
     * <p>
     * See https://www.postgresql.org/docs/9.2/static/protocol-error-fields.html for a list of error codes
     */
    static void sendErrorResponse(Channel channel, Throwable throwable) {
        final String message = SQLExceptions.messageOf(throwable);
        byte[] msg = message.getBytes(StandardCharsets.UTF_8);
        byte[] severity = "ERROR".getBytes(StandardCharsets.UTF_8);
        byte[] lineNumber = null;
        byte[] fileName = null;
        byte[] methodName = null;

        StackTraceElement[] stackTrace = throwable.getStackTrace();
        if (stackTrace != null && stackTrace.length > 0) {
            StackTraceElement stackTraceElement = stackTrace[0];
            lineNumber = String.valueOf(stackTraceElement.getLineNumber()).getBytes(StandardCharsets.UTF_8);
            if (stackTraceElement.getFileName() != null) {
                fileName = stackTraceElement.getFileName().getBytes(StandardCharsets.UTF_8);
            }
            if (stackTraceElement.getMethodName() != null) {
                methodName = stackTraceElement.getMethodName().getBytes(StandardCharsets.UTF_8);
            }
        }

        // See https://www.postgresql.org/docs/9.2/static/errcodes-appendix.html
        // need to add a throwable -> error code mapping later on
        byte[] errorCode;
        if (throwable instanceof IllegalArgumentException || throwable instanceof UnsupportedOperationException) {
            // feature_not_supported
            errorCode = "0A000".getBytes(StandardCharsets.UTF_8);
        } else {
            // internal_error
            errorCode = "XX000".getBytes(StandardCharsets.UTF_8);
        }
        int length = 4 +
            1 + (severity.length + 1) +
            1 + (msg.length + 1) +
            1 + (errorCode.length + 1) +
            (fileName != null ? 1 + (fileName.length + 1) : 0) +
            (lineNumber != null ? 1 + (lineNumber.length + 1) : 0) +
            (methodName != null ? 1 + (methodName.length + 1) : 0) +
            1;
        ChannelBuffer buffer = ChannelBuffers.buffer(length + 1);
        buffer.writeByte('E');
        buffer.writeInt(length);
        buffer.writeByte('S');
        writeCString(buffer, severity);
        buffer.writeByte('M');
        writeCString(buffer, msg);
        buffer.writeByte(('C'));
        writeCString(buffer, errorCode);
        if (fileName != null) {
            buffer.writeByte('F');
            writeCString(buffer, fileName);
        }
        if (lineNumber != null) {
            buffer.writeByte('L');
            writeCString(buffer, lineNumber);
        }
        if (methodName != null) {
            buffer.writeByte('R');
            writeCString(buffer, methodName);
        }
        buffer.writeByte(0);
        ChannelFuture channelFuture = channel.write(buffer);
        if (LOGGER.isTraceEnabled()) {
            channelFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    LOGGER.trace("sentErrorResponse msg={}", message);
                }
            });
        }
    }

    /**
     * Byte1('D')
     * Identifies the message as a data row.
     * <p>
     * Int32
     * Length of message contents in bytes, including self.
     * <p>
     * Int16
     * The number of column values that follow (possibly zero).
     * <p>
     * Next, the following pair of fields appear for each column:
     * <p>
     * Int32
     * The length of the column value, in bytes (this count does not include itself).
     * Can be zero. As a special case, -1 indicates a NULL column value. No value bytes follow in the NULL case.
     * <p>
     * ByteN
     * The value of the column, in the format indicated by the associated format code. n is the above length.
     */
    static void sendDataRow(Channel channel, Row row, List<? extends DataType> columnTypes, @Nullable FormatCodes.FormatCode[] formatCodes) {
        int length = 4 + 2;
        assert columnTypes.size() == row.numColumns()
            : "Number of columns in the row must match number of columnTypes. Row: " + row + " types: " + columnTypes;

        ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
        buffer.writeByte('D');
        buffer.writeInt(0); // will be set at the end
        buffer.writeShort(row.numColumns());

        for (int i = 0; i < row.numColumns(); i++) {
            DataType dataType = columnTypes.get(i);
            PGType pgType = PGTypes.get(dataType);
            Object value = row.get(i);
            if (value == null) {
                buffer.writeInt(-1);
                length += 4;
            } else {
                FormatCodes.FormatCode formatCode = FormatCodes.getFormatCode(formatCodes, i);
                switch (formatCode) {
                    case TEXT:
                        length += pgType.writeAsText(buffer, value);
                        break;
                    case BINARY:
                        length += pgType.writeAsBinary(buffer, value);
                        break;

                    default:
                        throw new AssertionError("Unrecognized formatCode: " + formatCode);
                }
            }
        }

        buffer.setInt(1, length);
        channel.write(buffer);
    }

    static void writeCString(ChannelBuffer buffer, byte[] valBytes) {
        buffer.writeBytes(valBytes);
        buffer.writeByte(0);
    }

    /**
     * RowDescription (B)
     * <p>
     * | 'T' | int32 len | int16 numCols
     * <p>
     * For each field:
     * <p>
     * | string name | int32 table_oid | int16 attr_num | int32 oid | int16 typlen | int32 type_modifier | int16 format_code
     * <p>
     * See https://www.postgresql.org/docs/current/static/protocol-message-formats.html
     */
    static void sendRowDescription(Channel channel, Collection<Field> columns, @Nullable FormatCodes.FormatCode[] formatCodes) {
        int length = 4 + 2;
        int columnSize = 4 + 2 + 4 + 2 + 4 + 2;
        ChannelBuffer buffer = ChannelBuffers.dynamicBuffer(
            length + (columns.size() * (10 + columnSize))); // use 10 as an estimate for columnName length

        buffer.writeByte('T');
        buffer.writeInt(0); // will be set at the end
        buffer.writeShort(columns.size());

        int idx = 0;
        for (Field column : columns) {
            byte[] nameBytes = column.path().outputName().getBytes(StandardCharsets.UTF_8);
            length += nameBytes.length + 1;
            length += columnSize;

            writeCString(buffer, nameBytes);
            buffer.writeInt(0);     // table_oid
            buffer.writeShort(0);   // attr_num

            PGType pgType = PGTypes.get(column.valueType());
            buffer.writeInt(pgType.oid());
            buffer.writeShort(pgType.typeLen());
            buffer.writeInt(pgType.typeMod());
            buffer.writeShort(FormatCodes.getFormatCode(formatCodes, idx).ordinal());

            idx++;
        }

        buffer.setInt(1, length);
        ChannelFuture channelFuture = channel.write(buffer);
        if (LOGGER.isTraceEnabled()) {
            channelFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    LOGGER.trace("sentRowDescription");
                }
            });
        }
    }

    /**
     * ParseComplete
     * | '1' | int32 len |
     */
    static void sendParseComplete(Channel channel) {
        sendShortMsg(channel, '1', "sentParseComplete");
    }

    /**
     * BindComplete
     * | '2' | int32 len |
     */
    static void sendBindComplete(Channel channel) {
        sendShortMsg(channel, '2', "sentBindComplete");
    }

    /**
     * EmptyQueryResponse
     * | 'I' | int32 len |
     */
    static void sendEmptyQueryResponse(Channel channel) {
        sendShortMsg(channel, 'I', "sentEmptyQueryResponse");
    }

    /**
     * NoData
     * | 'n' | int32 len |
     */
    static void sendNoData(Channel channel) {
        sendShortMsg(channel, 'n', "sentNoData");
    }

    /**
     * Send a message that just contains the msgType and the msg length
     */
    private static void sendShortMsg(Channel channel, char msgType, final String traceLogMsg) {
        ChannelBuffer buffer = ChannelBuffers.buffer(5);
        buffer.writeByte(msgType);
        buffer.writeInt(4);

        ChannelFuture channelFuture = channel.write(buffer);
        if (LOGGER.isTraceEnabled()) {
            channelFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    LOGGER.trace(traceLogMsg);
                }
            });
        }
    }

    static void sendPortalSuspended(Channel channel) {
        sendShortMsg(channel, 's', "sentPortalSuspended");
    }

    /**
     * CloseComplete
     * | '3' | int32 len |
     */
    static void sendCloseComplete(Channel channel) {
        sendShortMsg(channel, '3', "sentCloseComplete");
    }
}

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

import java.util.List;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslContext;

public class PgDecoder extends ByteToMessageDecoder {

    private static final Logger LOGGER = LogManager.getLogger(PgDecoder.class);

    static final int CANCEL_REQUEST_CODE = 80877102;
    static final int SSL_REQUEST_CODE = 80877103;
    // Version 3.0  ((3 << 16) | 0)
    static final int PROTOCOL_VERSION_REQUEST_CODE = 196608;

    static final int MIN_STARTUP_LENGTH = 8;
    static final int MIN_MSG_LENGTH = 5;

    /*
     * In protocol 3.0 and later, the startup packet length is not fixed, but
     * we set an arbitrary limit on it anyway.  This is just to prevent simple
     * denial-of-service attacks via sending enough data to run the server
     * out of memory.
     */
    static final int MAX_STARTUP_LENGTH = 10000;

    public enum State {

        /**
         * In this state we expect a startup message.
         *
         * Startup payload is
         *  int32: length
         *  int32: requestCode (ssl | protocol version | cancel)
         *  [payload (parameters)] | [KeyData(int32, int32) in case of cancel]
         */
        STARTUP,

        /**
         * In this state the handler must consume the startup payload and then call {@link #startupDone()
         */
        STARTUP_PARAMETERS,

        /**
         * In this state the handler must read the KeyData and cancel the query
         */
        CANCEL,


        /**
         * In this state we expect a message.
         *
         * Message payload is
         *  byte: message type
         *  int32: length
         *  [payload]
         */
        MSG,
    }


    {
        setCumulator(COMPOSITE_CUMULATOR);
    }

    private final Supplier<SslContext> getSslContext;

    private State state = State.STARTUP;
    private byte msgType;
    private int payloadLength;


    public PgDecoder(Supplier<SslContext> getSslContext) {
        this.getSslContext = getSslContext;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        ByteBuf decode = decode(ctx, in);
        if (decode != null) {
            out.add(decode);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        super.channelRead(ctx, msg);
    }

    private ByteBuf decode(ChannelHandlerContext ctx, ByteBuf in) {
        switch (state) {
            case STARTUP: {
                LOGGER.trace("Readable bytes: {}", in.readableBytes());
                if (in.readableBytes() == 0) {
                    // No data at all, don't bother with any complaints logged as such cases often occur for legitimate
                    // reasons. E.g. some monitoring solutions may open and close the connections without sending any data.
                    Channel channel = ctx.channel();
                    ByteBuf out = ctx.alloc().buffer(1);
                    channel.writeAndFlush(out.writeByte('0'));
                    return null;
                }

                in.markReaderIndex();
                payloadLength = in.readInt();

                if (payloadLength < MIN_STARTUP_LENGTH || payloadLength > MAX_STARTUP_LENGTH) {
                    in.skipBytes(in.readableBytes());
                    throw new IllegalStateException("invalid length of startup packet");
                }
                payloadLength -= 8; // length includes itself and the request code

                int requestCode = in.readInt();

                LOGGER.trace("Received startup message code={}, length={}", requestCode, payloadLength);

                switch (requestCode) {
                    case SSL_REQUEST_CODE:
                        SslContext sslContext = getSslContext.get();
                        Channel channel = ctx.channel();
                        ByteBuf out = ctx.alloc().buffer(1);
                        if (sslContext == null) {
                            channel.writeAndFlush(out.writeByte('N'));
                        } else {
                            channel.writeAndFlush(out.writeByte('S'));
                            ctx.pipeline().addFirst(sslContext.newHandler(ctx.alloc()));
                        }
                        if (in.readableBytes() < MIN_STARTUP_LENGTH) {
                            // more data needed before we can decode the next message
                            return null;
                        }
                        return decode(ctx, in);
                    case PROTOCOL_VERSION_REQUEST_CODE,
                         CANCEL_REQUEST_CODE:
                        if (in.readableBytes() < payloadLength) {
                            LOGGER.trace("Readable bytes: {} < payloadLength: {}. Reset buffer", in.readableBytes(), payloadLength);
                            in.resetReaderIndex();
                            return null;
                        }
                        state = requestCode == CANCEL_REQUEST_CODE ? State.CANCEL : State.STARTUP_PARAMETERS;
                        return in.readBytes(payloadLength);
                    default:
                        // bad message, skip any remaining data
                        in.skipBytes(in.readableBytes());
                        int major = requestCode >> 16;
                        int minor = requestCode & 0xFFFF;
                        throw new IllegalStateException("Unsupported frontend protocol " + major + "." + minor + ": server supports 3.0 to 3.0");
                }
            }

            /**
              * Message payload is
              *  byte: message type
              *  int32: length (excluding message type, including length itself)
              *  payload
              **/
            case MSG: {
                if (in.readableBytes() < MIN_MSG_LENGTH) {
                    return null;
                }
                in.markReaderIndex();
                msgType = in.readByte();
                payloadLength = in.readInt() - 4; // exclude length itself

                if (in.readableBytes() < payloadLength) {
                    in.resetReaderIndex();
                    return null;
                }
                return in.readBytes(payloadLength);
            }
            default:
                // bad message, skip any remaining data
                in.skipBytes(in.readableBytes());
                throw new IllegalStateException("Invalid state " + state);
        }
    }

    public void startupDone() {
        assert state == State.STARTUP_PARAMETERS
            : "Must only call startupDone if state == STARTUP_PARAMETERS";
        state = State.MSG;
    }

    public byte msgType() {
        return msgType;
    }

    public int payloadLength() {
        return payloadLength;
    }

    public State state() {
        return state;
    }
}

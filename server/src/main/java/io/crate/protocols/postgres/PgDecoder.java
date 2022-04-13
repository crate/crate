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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslContext;

public class PgDecoder extends ByteToMessageDecoder {

    static final int CANCEL_CODE = 80877102;
    static final int SSL_REQUEST_CODE = 80877103;

    static final int MIN_STARTUP_LENGTH = 8;
    static final int MIN_MSG_LENGTH = 5;

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

    private ByteBuf decode(ChannelHandlerContext ctx, ByteBuf in) {
        switch (state) {
            case STARTUP: {
                if (in.readableBytes() < MIN_STARTUP_LENGTH) {
                    return null;
                }

                in.markReaderIndex();
                payloadLength = in.readInt() - 8;
                int requestCode = in.readInt();

                if (requestCode == SSL_REQUEST_CODE) {
                    SslContext sslContext = getSslContext.get();
                    Channel channel = ctx.channel();
                    ByteBuf out = ctx.alloc().buffer(1);
                    if (sslContext == null) {
                        channel.writeAndFlush(out.writeByte('N'));
                    } else {
                        channel.writeAndFlush(out.writeByte('S'));
                        ctx.pipeline().addFirst(sslContext.newHandler(ctx.alloc()));
                    }
                    in.markReaderIndex();
                    return decode(ctx, in);
                } else {
                    if (in.readableBytes() < payloadLength) {
                        in.resetReaderIndex();
                        return null;
                    }
                    state = requestCode == CANCEL_CODE ? State.CANCEL : State.STARTUP_PARAMETERS;
                    return in.readBytes(payloadLength);
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

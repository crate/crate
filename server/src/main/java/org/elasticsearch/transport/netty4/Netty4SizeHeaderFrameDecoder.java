/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.TooLongFrameException;
import org.elasticsearch.transport.TcpHeader;
import org.elasticsearch.transport.TcpTransport;

import java.util.List;

final class Netty4SizeHeaderFrameDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        try {
            boolean continueProcessing = TcpTransport.validateMessageHeader(Netty4Utils.toBytesReference(in));
            final ByteBuf message = in.skipBytes(TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE);
            if (!continueProcessing) return;
            out.add(message);
        } catch (IllegalArgumentException ex) {
            throw new TooLongFrameException(ex);
        } catch (IllegalStateException ex) {
            /* decode will be called until the ByteBuf is fully consumed; when it is fully
             * consumed, transport#validateMessageHeader will throw an IllegalStateException which
             * is okay, it means we have finished consuming the ByteBuf and we can get out
             */
        }
    }

}

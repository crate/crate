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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.transport.StatsTracker;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

@ChannelHandler.Sharable
public class Netty4OutboundStatsHandler extends ChannelOutboundHandlerAdapter implements Releasable {

    private final StatsTracker statsTracker;

    final Logger logger;

    public Netty4OutboundStatsHandler(StatsTracker statsTracker, Logger logger) {
        this.statsTracker = statsTracker;
        this.logger = logger;
    }

    @Override
    public void close() {
        // nothing to close
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof ByteBuf bb) {
            statsTracker.incrementBytesSent(bb.readableBytes());
        } else {
            logger.warn("Message received is: {} and not a ByteBuf, cannot track sent bytes or message count",
                msg.getClass().getCanonicalName());
        }
        ctx.write(msg, promise);
    }
}

package io.crate.protocols.postgres;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;

@Sharable
public final class ReadSuppressingHandler extends ChannelOutboundHandlerAdapter {

    private static final Logger LOGGER = LogManager.getLogger(ReadSuppressingHandler.class);
    public static final ReadSuppressingHandler INSTANCE = new ReadSuppressingHandler();

    // @Override
    // public void read(ChannelHandlerContext ctx) throws Exception {
    //     boolean autoRead = ctx.channel().config().isAutoRead();
    //     LOGGER.warn("autoRead={}", autoRead);
    //     if (autoRead) {
    //         super.read(ctx);
    //     }
    // }
}

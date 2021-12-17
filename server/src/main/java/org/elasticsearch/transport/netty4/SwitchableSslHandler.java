package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;
import java.util.List;

public class SwitchableSslHandler extends SslHandler {
    private static final Logger LOGGER = LogManager.getLogger(SwitchableSslHandler.class);

     final AttributeKey<Boolean> CAN_SWITCH_TO_PLAINTEXT = AttributeKey.valueOf("switch_to_plaintext");


    public SwitchableSslHandler(SSLEngine engine) {
        super(engine);
    }

    @Override
    public void write(final ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (ctx.channel().attr(CAN_SWITCH_TO_PLAINTEXT).get() != null) { // if not null it's set to the true in the only place - HostBasedAuthhandler
            ctx.pipeline().remove(this);
            ctx.write(msg); //forward
        } else {
            super.write(ctx, msg, promise); // Keep encrypting outgoing messages
        }
    }
}

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class ClientStartTLSHandler extends ByteToMessageDecoder {

    public final static String CLIENT_STARTTLS_HANDLER = "client-starttls-handler";
    public final static int STARTTLS_MSG_LENGTH = 8;


    private final SslContext sslContext;

    // Same length to unify/simplify decode condition.
    public final static String STARTTLS_YES_REQUEST = "STARTTLS";
    public final static String STARTTLS_NO_REQUEST = "NOSTART!";



    public ClientStartTLSHandler(SslContext sslContext) {
        this.sslContext = sslContext;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        ByteBuf buf = Unpooled.buffer(STARTTLS_MSG_LENGTH);
        buf.writeCharSequence(STARTTLS_YES_REQUEST, StandardCharsets.UTF_8);
        ctx.writeAndFlush(buf);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < STARTTLS_MSG_LENGTH) {
            return;
        }

        if (in.getCharSequence(in.readerIndex(), STARTTLS_MSG_LENGTH, StandardCharsets.UTF_8).equals(STARTTLS_YES_REQUEST)) {
            SslHandler sslHandler = sslContext.newHandler(ctx.alloc());
            sslHandler.engine().setUseClientMode(true);
            // By docs startTls must be false but SslContextProvider.clientContext()
            // already puts false to it and SslHandler is created from context.

            // We are not doing sslHandler.engine().beginHandshake() as per docs
            // since SslHandler starts handshake on handlerAdded
            ctx.pipeline().addAfter(CLIENT_STARTTLS_HANDLER, "client-ssl-handler", sslHandler);
            ctx.pipeline().remove(this);

        } else {
            // Server didn't accept STARTTLS request from this client
            // based on HBA config - it means nodes can continue communicating with plaintext.
            ctx.pipeline().remove(this);
        }

    }


}

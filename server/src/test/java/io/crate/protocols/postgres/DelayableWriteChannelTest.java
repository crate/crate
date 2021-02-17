
package io.crate.protocols.postgres;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.CompletableFuture;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

public class DelayableWriteChannelTest {

    @Test
    public void test_delayed_writes_are_released_on_close() throws Exception {
        var channel = new DelayableWriteChannel(new EmbeddedChannel());
        var future = new CompletableFuture<>();
        channel.delayWritesUntil(future);
        ByteBuf buffer = Unpooled.buffer();
        channel.write(buffer);
        channel.close();
        assertThat(buffer.refCnt(), is(0));
    }
}

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

package io.crate.http.netty;

import io.crate.test.integration.CrateUnitTest;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class CrateNettyHttpServerTransportTest extends CrateUnitTest {

    private static CrateNettyHttpServerTransport.ChannelPipelineItem channelPipelineItem(String base, String name) {
        return new CrateNettyHttpServerTransport.ChannelPipelineItem(base, name, () -> new ChannelHandler() {
            @Override
            public void handlerAdded(ChannelHandlerContext ctx) throws Exception {

            }

            @Override
            public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {

            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

            }
        });
    }

    @Test
    public void testAddSortedToPipline() throws Exception {
        CrateNettyHttpServerTransport httpTransport = new CrateNettyHttpServerTransport(Settings.EMPTY,
            mock(NetworkService.class),
            mock(BigArrays.class),
            mock(ThreadPool.class));

        httpTransport.addBefore(channelPipelineItem("c", "d"));
        httpTransport.addBefore(channelPipelineItem("a", "b"));
        httpTransport.addBefore(channelPipelineItem("d", "e"));
        httpTransport.addBefore(channelPipelineItem("b", "c"));

        int size = httpTransport.addBefore().size();
        assertThat(size, is(4));

        String[] names = new String[httpTransport.addBefore().size()];
        for (int i = 0; i < size; i++) {
            names[i] = httpTransport.addBefore().get(i).name;
        }
        assertThat(names, is(new String[]{"b", "c", "d", "e"}));
    }

}

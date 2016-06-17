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

import io.crate.action.sql.SQLOperations;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

@Singleton
public class PostgresNetty extends AbstractLifecycleComponent {

    private final SQLOperations sqlOperations;
    private ServerBootstrap bootstrap;
    private ExecutorService bossExecutor;
    private ExecutorService workerExecutor;

    @Inject
    public PostgresNetty(Settings settings, SQLOperations sqlOperations) {
        super(settings);
        this.sqlOperations = sqlOperations;
    }

    @Override
    protected void doStart() {
        if (!settings.getAsBoolean("network.psql", false)) {
            return;
        }

        bossExecutor = Executors.newCachedThreadPool(daemonThreadFactory(settings, "postgres-netty-boss"));
        workerExecutor = Executors.newCachedThreadPool(daemonThreadFactory(settings, "postgres-netty-worker"));
        bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(bossExecutor, workerExecutor));

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();

                ConnectionContext connectionContext = new ConnectionContext(sqlOperations);
                pipeline.addLast("frame-decoder", connectionContext.decoder);
                pipeline.addLast("handler", connectionContext.handler);
                return pipeline;
            }
        });
        bootstrap.bind(new InetSocketAddress("127.0.0.1", 4242));
    }

    @Override
    protected void doStop() {
        if (bootstrap != null) {
            bootstrap.shutdown();
            workerExecutor.shutdown();
            bossExecutor.shutdown();

            try {
                workerExecutor.awaitTermination(2, TimeUnit.SECONDS);
                bossExecutor.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    protected void doClose() {

    }
}

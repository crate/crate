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

package io.crate.netty;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.netty4.Netty4Transport;

import io.crate.common.collections.BorrowedItem;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;

@Singleton
public class EventLoopGroups {

    public static final String WORKER_THREAD_PREFIX = "netty-worker";
    private static final Logger LOGGER = LogManager.getLogger(EventLoopGroups.class);

    private int refs = 0;
    private EventLoopGroup worker;

    public synchronized BorrowedItem<EventLoopGroup> getEventLoopGroup(Settings settings) {
        if (worker == null) {
            ThreadFactory workerThreads = daemonThreadFactory(settings, WORKER_THREAD_PREFIX);
            int workerCount = Netty4Transport.WORKER_COUNT.get(settings);
            if (Epoll.isAvailable()) {
                worker = new EpollEventLoopGroup(workerCount, workerThreads);
            } else {
                worker = new NioEventLoopGroup(workerCount, workerThreads);
            }
        }
        refs++;
        return new BorrowedItem<>(worker, () -> {
            synchronized (EventLoopGroups.this) {
                refs--;
                if (refs == 0) {
                    Future<?> shutdownGracefully = worker.shutdownGracefully(0, 5, TimeUnit.SECONDS);
                    shutdownGracefully.awaitUninterruptibly();
                    if (shutdownGracefully.isSuccess() == false) {
                        LOGGER.warn("Error closing netty event loop group", shutdownGracefully.cause());
                    }
                    worker = null;
                }
            }
        });
    }
}

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

package io.crate.plugin;

import com.google.common.annotations.VisibleForTesting;
import io.crate.enterprise.loader.EnterpriseLoader;
import io.crate.enterprise.loader.LoadableValue;
import io.crate.enterprise.loader.StringLoadable;
import io.crate.protocols.ssl.SslConfigSettings;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * The PipelineRegistry takes care of adding handlers to the existing
 * ES ChannelPipeline at the appropriate positions.
 *
 * This singleton is injected in the AuthenticationProvider (see users module)
 * and is provided by the {@link HttpTransportPlugin}.
 */
@Singleton
public final class PipelineRegistry {

    private static final LoadableValue<SslContext, Object> SSL_CONTEXT_LOADABLE = new SslContextLoadable();

    private final List<ChannelPipelineItem> addBeforeList;
    private final SslContext sslContext;

    @Inject
    public PipelineRegistry(Settings settings) {
        this.addBeforeList = new ArrayList<>();
        this.sslContext = loadSslContext(settings);
    }

    /**
     * A data structure for items that can be added to the channel pipeline of the HTTP transport.
     */
    public static class ChannelPipelineItem {

        final String base;
        final String name;
        final Supplier<ChannelHandler> handlerFactory;

        /**
         * @param base              the name of the existing handler in the pipeline before/after which the new handler should be added
         * @param name              the name of the new handler that should be added to the pipeline
         * @param handlerFactory    a supplier that provides a new instance of the handler
         */
        public ChannelPipelineItem(String base, String name, Supplier<ChannelHandler> handlerFactory) {
            this.base = base;
            this.name = name;
            this.handlerFactory = handlerFactory;
        }

        @Override
        public String toString() {
            return "ChannelPipelineItem{" +
                   "base='" + base + '\'' +
                   ", name='" + name + '\'' +
                   '}';
        }
    }

    public void addBefore(ChannelPipelineItem item) {
        synchronized (addBeforeList) {
            addSorted(addBeforeList, item);
        }
    }

    public void registerItems(ChannelPipeline pipeline) {
        for (PipelineRegistry.ChannelPipelineItem item : addBeforeList) {
            pipeline.addBefore(item.base, item.name, item.handlerFactory.get());
        }
        if (sslContext != null) {
            SslHandler sslHandler = sslContext.newHandler(pipeline.channel().alloc());
            pipeline.addFirst(sslHandler);
        }
    }

    List<ChannelPipelineItem> addBeforeList() {
        return addBeforeList;
    }

    /**
     * Add a new {@link ChannelPipelineItem} to an existing list.
     * An item has base on which it depends on and after which it must be added to the pipeline.
     * Since items may be added at different times and from different places the dependencies of items may not be present
     * when they are added.
     * The sorting works as follows:
     *   * first, add new item to existing list
     *   * create a copy on which we can iterate because we may need to modify the order of items of the existing list
     *   * iterate over items and check if an item already exists which depends on the iterated item - add new item after
     *   * iterate over items and check if an item already exists on which the iterated item depends on - add new item before
     *   * if non of the previous predicates apply, add the iterated item at the end of the list
     *
     * @param pipelineItems   list to add newItem to
     * @param newItem         new pipeline item to be added
     */
    private void addSorted(List<ChannelPipelineItem> pipelineItems, ChannelPipelineItem newItem) {
        pipelineItems.add(newItem);

        if (pipelineItems.size() < 2) {
            return;
        }

        ArrayList<ChannelPipelineItem> copy = new ArrayList<>(pipelineItems.size());
        copy.addAll(pipelineItems);

        for (ChannelPipelineItem item : copy) {
            pipelineItems.remove(item);

            boolean prev = false;
            int prevIdx = 0;
            for (ChannelPipelineItem o : pipelineItems) {
                if (o.name.equals(item.base)) {
                    prev = true;
                    break;
                }
                prevIdx++;
            }
            if (prev) {
                pipelineItems.add(prevIdx + 1, item);
                continue;
            }

            boolean next = false;
            int nextIdx = 0;
            for (ChannelPipelineItem o : pipelineItems) {
                if (o.base.equals(item.name)) {
                    next = true;
                    break;
                }
                nextIdx++;
            }
            if (next) {
                pipelineItems.add(nextIdx, item);
                continue;
            }

            pipelineItems.add(item);
        }
    }

    @VisibleForTesting
    public static SslContext loadSslContext(Settings settings) {
        return EnterpriseLoader.loadValue(
            settings, SslConfigSettings.SSL_HTTP_ENABLED, SSL_CONTEXT_LOADABLE, settings);
    }

    private static class SslContextLoadable
            extends StringLoadable<Object>
            implements LoadableValue<SslContext, Object> {

        @Override
        public Class<Object> getBaseClass() {
            return Object.class;
        }

        @Override
        public String getFQClassName() {
            return "io.crate.protocols.ssl.SslConfiguration";
        }

        @Override
        public String methodName() {
            return "buildSslContext";
        }

        @Override
        public Class<?>[] getMethodParams() {
            return new Class[] { Settings.class };
        }

        @Override
        public Class<SslContext> getReturnType() {
            return SslContext.class;
        }
    }
}

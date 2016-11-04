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

package io.crate.module;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.ClusterIdService;
import io.crate.plugin.IndexEventListenerProxy;
import io.crate.rest.CrateRestMainAction;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.matcher.AbstractMatcher;
import org.elasticsearch.common.inject.spi.InjectionListener;
import org.elasticsearch.common.inject.spi.TypeEncounter;
import org.elasticsearch.common.inject.spi.TypeListener;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.action.RestMainAction;

public class CrateCoreModule extends AbstractModule {

    private final Logger logger;
    private final IndexEventListenerProxy indexEventListenerProxy;

    public CrateCoreModule(Settings settings, IndexEventListenerProxy indexEventListenerProxy) {
        logger = Loggers.getLogger(getClass().getPackage().getName(), settings);
        this.indexEventListenerProxy = indexEventListenerProxy;
    }

    @Override
    protected void configure() {
        /*
         * This is a rather hacky method to overwrite the handler for "/"
         * The ES plugins are loaded before the core ES components. That means that the registration for
         * "/" in {@link CrateRestMainAction} is overwritten once {@link RestMainAction} is instantiated.
         *
         * By using a listener that is called after {@link RestMainAction} is instantiated we can call
         * {@link io.crate.rest.CrateRestMainAction#registerHandler()}  and overwrite it "back".
         */

        // the crateListener is used to retrieve the CrateRestMainAction instance.
        // otherwise there is no way to retrieve it at this point.
        CrateRestMainActionListener crateListener = new CrateRestMainActionListener();
        bindListener(
            new SubclassOfMatcher(CrateRestMainAction.class),
            crateListener);

        // this listener will use the CrateRestMainAction instance and call registerHandler
        // after RestMainAction is created.
        bindListener(
            new SubclassOfMatcher(RestMainAction.class),
            new RestMainActionListener(crateListener.instanceFuture));

        bind(ClusterIdService.class).asEagerSingleton();
        bind(IndexEventListenerProxy.class).toInstance(indexEventListenerProxy);
    }

    private class RestMainActionListener implements TypeListener {

        private final SettableFuture<CrateRestMainAction> instanceFuture;

        RestMainActionListener(SettableFuture<CrateRestMainAction> instanceFuture) {
            this.instanceFuture = instanceFuture;
        }

        @Override
        public <I> void hear(TypeLiteral<I> type, TypeEncounter<I> encounter) {
            encounter.register(new InjectionListener<I>() {
                @Override
                public void afterInjection(I injectee) {
                    Futures.addCallback(instanceFuture, new FutureCallback<CrateRestMainAction>() {
                        @Override
                        public void onSuccess(CrateRestMainAction result) {
                            result.registerHandler();
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            logger.error("Could not register CrateRestMainAction handler", t);
                        }
                    });
                }
            });
        }
    }

    private static class SubclassOfMatcher extends AbstractMatcher<TypeLiteral<?>> {

        private final Class<?> klass;

        SubclassOfMatcher(Class<?> klass) {
            this.klass = klass;
        }

        @Override
        public boolean matches(TypeLiteral<?> typeLiteral) {
            return klass.isAssignableFrom(typeLiteral.getRawType());
        }
    }

    private static class CrateRestMainActionListener implements TypeListener {

        private final SettableFuture<CrateRestMainAction> instanceFuture;

        CrateRestMainActionListener() {
            this.instanceFuture = SettableFuture.create();

        }

        @Override
        public <I> void hear(TypeLiteral<I> type, TypeEncounter<I> encounter) {
            encounter.register(new InjectionListener<I>() {
                @Override
                public void afterInjection(I injectee) {
                    instanceFuture.set((CrateRestMainAction) injectee);
                }
            });
        }
    }
}

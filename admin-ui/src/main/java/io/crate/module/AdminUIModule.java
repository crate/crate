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

import io.crate.rest.action.admin.AdminUIFrontpageAction;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.matcher.AbstractMatcher;
import org.elasticsearch.common.inject.spi.InjectionListener;
import org.elasticsearch.common.inject.spi.TypeEncounter;
import org.elasticsearch.common.inject.spi.TypeListener;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.rest.action.RestMainAction;

import java.util.concurrent.CompletableFuture;


public class AdminUIModule extends AbstractModule {

    private final Logger logger = Loggers.getLogger(getClass());

    public AdminUIModule() {
    }

    @Override
    protected void configure() {
        AdminUIFrontpageActionListener adminUIListener = new AdminUIFrontpageActionListener();
        bindListener(
            new SubclassOf(AdminUIFrontpageAction.class),
            adminUIListener);

        // this listener will use the AdminUIFrontpageAction instance and call registerHandler
        // after RestMainAction is created.
        bindListener(
            new SubclassOf(RestMainAction.class),
            new RestMainActionListener(adminUIListener.instanceFuture));
    }

    private class RestMainActionListener implements TypeListener {

        private final CompletableFuture<AdminUIFrontpageAction> instanceFuture;

        RestMainActionListener(CompletableFuture<AdminUIFrontpageAction> instanceFuture) {
            this.instanceFuture = instanceFuture;
        }

        @Override
        public <I> void hear(TypeLiteral<I> type, TypeEncounter<I> encounter) {
            encounter.register((InjectionListener<I>) injectee ->
                instanceFuture.whenComplete((adminUIFrontpageAction, throwable) -> {
                    if (throwable == null) {
                        adminUIFrontpageAction.registerHandler();
                    } else {
                        logger.error("Could not register AdminUIFrontpageAction handler", throwable);
                    }
                }));
        }
    }

    private static class SubclassOf extends AbstractMatcher<TypeLiteral<?>> {

        private final Class<?> classz;

        SubclassOf(Class<?> classz) {
            this.classz = classz;
        }

        @Override
        public boolean matches(TypeLiteral<?> typeLiteral) {
            return classz.isAssignableFrom(typeLiteral.getRawType());
        }
    }

    private static class AdminUIFrontpageActionListener implements TypeListener {

        private final CompletableFuture<AdminUIFrontpageAction> instanceFuture;

        AdminUIFrontpageActionListener() {
            this.instanceFuture = new CompletableFuture<>();
        }

        @Override
        public <I> void hear(TypeLiteral<I> type, TypeEncounter<I> encounter) {
            encounter.register((InjectionListener<I>) injectee ->
                instanceFuture.complete((AdminUIFrontpageAction) injectee));
        }
    }
}

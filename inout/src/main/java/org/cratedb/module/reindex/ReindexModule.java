/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.cratedb.module.reindex;

import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import org.cratedb.action.reindex.ReindexAction;
import org.cratedb.action.reindex.ReindexParser;
import org.cratedb.action.reindex.TransportReindexAction;

public class ReindexModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(TransportReindexAction.class).asEagerSingleton();

        bind(ReindexParser.class).asEagerSingleton();

        MapBinder<GenericAction, TransportAction> transportActionsBinder =
                MapBinder.newMapBinder(
                        binder(), GenericAction.class, TransportAction.class);

        transportActionsBinder.addBinding(ReindexAction.INSTANCE).to(
                TransportReindexAction.class).asEagerSingleton();

        MapBinder<String, GenericAction> actionsBinder = MapBinder
                .newMapBinder(binder(), String.class, GenericAction.class);
        actionsBinder.addBinding(ReindexAction.NAME).toInstance(
                ReindexAction.INSTANCE);

    }

}

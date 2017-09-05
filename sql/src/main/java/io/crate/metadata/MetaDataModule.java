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

package io.crate.metadata;

import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class MetaDataModule extends AbstractModule {

    @Override
    protected void configure() {
        bindFunctions();
        bindSchemas();
        bind(DDLClusterStateService.class).asEagerSingleton();
    }

    private void bindFunctions() {
        MapBinder.newMapBinder(binder(), String.class, FunctionResolver.class);
        MapBinder.newMapBinder(binder(), String.class, TableFunctionImplementation.class);
        bind(Functions.class).asEagerSingleton();
    }

    private void bindSchemas() {
        MapBinder.newMapBinder(binder(), String.class, SchemaInfo.class);
        bind(Schemas.class).asEagerSingleton();
    }
}

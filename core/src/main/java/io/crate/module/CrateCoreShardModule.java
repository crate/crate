/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.module;

import com.google.common.collect.Lists;
import io.crate.core.CrateComponentLoader;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.PreProcessModule;
import org.elasticsearch.common.settings.Settings;

import java.util.Collection;
import java.util.List;

public class CrateCoreShardModule extends AbstractModule implements PreProcessModule {

    private final Settings settings;
    private final CrateComponentLoader crateComponentLoader;

    public CrateCoreShardModule(Settings settings) {
        this.settings = settings;
        crateComponentLoader = CrateComponentLoader.getInstance(settings);
    }

    /* TODO: FIX ME! Remove this and merge it somehow somewhere.
    @Override
    public Iterable<? extends Module> spawnModules() {
        List<Module> modules = Lists.newArrayList();
        Collection<Class<? extends Module>> modulesClasses = crateComponentLoader.shardModules();
        for (Class<? extends Module> moduleClass : modulesClasses) {
            modules.add(createModule(moduleClass, settings));
        }
        modules.addAll(crateComponentLoader.shardModules(settings));
        return modules;
    }*/

    @Override
    public void processModule(Module module) {
        crateComponentLoader.processModule(module);
    }

    @Override
    protected void configure() {
    }
}

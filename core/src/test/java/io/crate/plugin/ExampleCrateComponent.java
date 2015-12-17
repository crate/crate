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

import com.google.common.collect.ImmutableList;
import io.crate.CrateComponent;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;

public class ExampleCrateComponent implements CrateComponent {
    @Override
    public Plugin createPlugin(Settings settings) {
        return new AbstractPlugin() {
            @Override
            public String name() {
                return "example";
            }

            @Override
            public String description() {
                return "example plugin";
            }

            @Override
            public Collection<Module> modules(Settings settings) {
                return ImmutableList.<Module>of(new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(IBound.class).to(Bound.class).asEagerSingleton();
                    }
                });
            }
        };
    }

    public interface IBound {}

    public static class Bound implements IBound {}
}

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

import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.junit.Test;
import org.mockito.Mock;

import static org.hamcrest.Matchers.instanceOf;

public class CrateCorePluginTest extends CrateUnitTest {

    @Mock
    public ClusterService clusterService;

    private final class TestModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(ClusterService.class).toInstance(clusterService);
        }
    }

    @Test
    public void testPluginLoadsCrateComponentModulesWithSettings() throws Exception {

        CrateCorePlugin plugin = new CrateCorePlugin(ImmutableSettings.EMPTY);
        ModulesBuilder modulesBuilder = new ModulesBuilder();

        // mock ClusterService for ClusterIdService, which is bound in CrateCoreModule
        modulesBuilder.add(new TestModule());

        for (Module module : plugin.modules(ImmutableSettings.EMPTY)) {
            modulesBuilder.add(module);
        }
        ExampleCrateComponent.IBound iBoundImpl = modulesBuilder.createInjector()
                .getInstance(ExampleCrateComponent.IBound.class);
        assertThat(iBoundImpl, instanceOf(ExampleCrateComponent.Bound.class));
    }
}

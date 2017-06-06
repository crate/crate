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

import io.crate.Plugin;
import io.crate.module.SigarModule;
import io.crate.monitor.SigarService;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;

import java.util.Collection;
import java.util.Collections;

public class SigarPlugin implements Plugin {

    private final SigarService sigarService;

    public SigarPlugin(Settings settings) {
        sigarService = new SigarService(settings);
    }

    @Override
    public String name() {
        return "crate-sigar";
    }

    @Override
    public String description() {
        return "Provide operating system information and statistics using the SIGAR library";
    }

    @Override
    public Settings additionalSettings() {
        return Settings.EMPTY;
    }

    @Override
    public Collection<Module> createGuiceModules() {
        if (sigarService.sigarAvailable()) {
            return Collections.singletonList(new SigarModule(sigarService));
        }
        return Collections.emptyList();
    }
}

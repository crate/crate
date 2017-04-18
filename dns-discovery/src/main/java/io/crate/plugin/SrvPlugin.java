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

import io.crate.discovery.SrvUnicastHostsProvider;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import java.util.Arrays;
import java.util.List;

public class SrvPlugin extends Plugin {

    private final Settings settings;

    public SrvPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            SrvUnicastHostsProvider.DISCOVERY_SRV_QUERY,
            SrvUnicastHostsProvider.DISCOVERY_SRV_RESOLVER
        );
    }

    // FIXME replace with the new DiscoveryPlugin infrastructure
//    public void onModule(DiscoveryModule discoveryModule) {
//        /**
//         * Different types of discovery modules can be defined on startup using the `discovery.type` setting.
//         * This SrvDiscoveryModule can be loaded using `-Cdiscovery.type=srv`
//         */
//        if (SrvDiscovery.SRV.equals(settings.get("discovery.type"))) {
//            discoveryModule.addDiscoveryType(SrvDiscovery.SRV, SrvDiscovery.class);
//            discoveryModule.addUnicastHostProvider(SrvDiscovery.SRV, SrvUnicastHostsProvider.class);
//        }
//    }
}

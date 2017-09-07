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

import io.crate.ClusterIdService;
import io.crate.plugin.IndexEventListenerProxy;
import io.crate.settings.SharedSettings;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

public class CrateCoreModule extends AbstractModule {

    private final IndexEventListenerProxy indexEventListenerProxy;

    public CrateCoreModule(Settings settings, IndexEventListenerProxy indexEventListenerProxy) {
        Logger logger = Loggers.getLogger(getClass().getPackage().getName(), settings);
        this.indexEventListenerProxy = indexEventListenerProxy;
        if (SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().get(settings) &&
            "".equals(SharedSettings.LICENSE_IDENT_SETTING.setting().get(settings))) {
            logger.info("CrateDB Enterprise features are active. Please request a license before deploying in " +
                        "production or deactivate the Enterprise features. https://crate.io/enterprise");
        }
    }

    @Override
    protected void configure() {
        bind(ClusterIdService.class).asEagerSingleton();
        bind(IndexEventListenerProxy.class).toInstance(indexEventListenerProxy);
    }
}

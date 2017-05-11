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

package io.crate.operation.auth;

import io.crate.settings.SharedSettings;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;

import java.util.Iterator;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

public abstract class UserServiceFactoryLoader {

    private final UserServiceFactory userServiceFactory;

    public UserServiceFactoryLoader(Settings settings) {
        if (!SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().get(settings)) {
            this.userServiceFactory = null;
            return;
        }
        Iterator<UserServiceFactory> authIterator = ServiceLoader.load(UserServiceFactory.class).iterator();
        UserServiceFactory factory = null;
        while (authIterator.hasNext()) {
            if (factory != null) {
                throw new ServiceConfigurationError("UserManagerFactory found twice");
            }
            factory = authIterator.next();
        }
        userServiceFactory = factory;
    }

    @Nullable
    public UserServiceFactory userServiceFactory() {
        return userServiceFactory;
    }
}

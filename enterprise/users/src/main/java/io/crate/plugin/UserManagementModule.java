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

import io.crate.operation.user.TransportCreateUserAction;
import io.crate.operation.user.TransportDropUserAction;
import io.crate.operation.user.TransportPrivilegesAction;
import io.crate.operation.user.UserLookup;
import io.crate.operation.user.UserManager;
import io.crate.operation.user.UserManagerService;
import org.elasticsearch.common.inject.AbstractModule;

public class UserManagementModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(TransportCreateUserAction.class).asEagerSingleton();
        bind(TransportDropUserAction.class).asEagerSingleton();
        bind(TransportPrivilegesAction.class).asEagerSingleton();
        bind(UserManager.class).to(UserManagerService.class).asEagerSingleton();
        bind(UserLookup.class).to(UserManagerService.class).asEagerSingleton();
    }
}

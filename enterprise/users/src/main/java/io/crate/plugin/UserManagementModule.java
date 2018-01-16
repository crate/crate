/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.plugin;

import io.crate.auth.user.TransportAlterUserAction;
import io.crate.auth.user.TransportCreateUserAction;
import io.crate.auth.user.TransportDropUserAction;
import io.crate.auth.user.TransportPrivilegesAction;
import io.crate.auth.user.UserLookup;
import io.crate.auth.user.UserManager;
import io.crate.auth.user.UserManagerService;
import org.elasticsearch.common.inject.AbstractModule;

public class UserManagementModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(TransportCreateUserAction.class).asEagerSingleton();
        bind(TransportDropUserAction.class).asEagerSingleton();
        bind(TransportAlterUserAction.class).asEagerSingleton();
        bind(TransportPrivilegesAction.class).asEagerSingleton();
        bind(UserManager.class).to(UserManagerService.class).asEagerSingleton();
        bind(UserLookup.class).to(UserManagerService.class).asEagerSingleton();
    }
}

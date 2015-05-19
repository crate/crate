/*
 * Copyright (c) 2015 Grant Rodgers
 *
 *     Permission is hereby granted, free of charge, to any person obtaining
 *     a copy of this software and associated documentation files (the "Software"),
 *     to deal in the Software without restriction, including without limitation
 *     the rights to use, copy, modify, merge, publish, distribute, sublicense,
 *     and/or sell copies of the Software, and to permit persons to whom the Software
 *     is furnished to do so, subject to the following conditions:
 *
 *     The above copyright notice and this permission notice shall be included in
 *     all copies or substantial portions of the Software.
 *
 *     THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *     EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 *     OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 *     IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 *     CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 *     TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
 *     OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package org.elasticsearch.discovery.srv;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.zen.ZenDiscoveryModule;

/**
 * This module is loaded by the DiscoveryModule.
 * Different types of discovery modules can be defined on startup using the `discovery.type` setting.
 * This SrvDiscoveryModule can be loaded using `-Des.discovery.type=srv`
 */
public class SrvDiscoveryModule extends ZenDiscoveryModule {

    @Inject
    public SrvDiscoveryModule(Settings settings) {
        addUnicastHostProvider(SrvUnicastHostsProvider.class);
    }

    @Override
    protected void bindDiscovery() {
        bind(Discovery.class).to(SrvDiscovery.class).asEagerSingleton();
    }
}

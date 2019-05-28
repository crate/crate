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

package io.crate.protocols.ssl;

import io.netty.handler.ssl.SslContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

@Singleton
public class SslContextProviderImpl implements SslContextProvider {

    private static final Logger LOGGER = LogManager.getLogger(SslContextProvider.class);

    private volatile SslContext sslContext;

    private final Settings settings;

    @Inject
    public SslContextProviderImpl(Settings settings) {
        this.settings = settings;
    }

    @Override
    public SslContext getSslContext() {
        var localRef = sslContext;
        if (localRef == null) {
            synchronized (this) {
                localRef = sslContext;
                if (localRef == null) {
                    sslContext = localRef = SslConfiguration.buildSslContext(this.settings);
                }
            }
        }
        return localRef;
    }

    @Override
    public void reloadSslContext() {
        synchronized (this) {
            sslContext = SslConfiguration.buildSslContext(this.settings);
            LOGGER.info("SSL configuration is reloaded.");
        }
    }
}

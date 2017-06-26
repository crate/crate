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

package io.crate.protocols.http;

import io.crate.protocols.ssl.SslConfiguration;
import io.netty.handler.ssl.SslContext;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

/**
 * Takes care of initializing the {@link SslContext}
 * and adding its handler to the pipeline.
 */
@SuppressWarnings("unused")
public class HttpsConfiguringHandler implements HttpsHandler {

    private final Logger LOGGER;
    private final SslContext sslContext;

    public HttpsConfiguringHandler(Settings settings) {
        this(settings, SslConfiguration.buildSslContext(settings));
    }

    private HttpsConfiguringHandler(Settings settings, SslContext sslContext) {
        this.LOGGER = Loggers.getLogger(HttpsConfiguringHandler.class, settings);
        LOGGER.info("HTTP SSL support is enabled.");
        this.sslContext = sslContext;
    }

    @Override
    public SslContext get() {
        return sslContext;
    }
}

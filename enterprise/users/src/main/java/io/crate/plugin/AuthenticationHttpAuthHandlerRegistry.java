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

import io.crate.auth.Authentication;
import io.crate.protocols.http.HttpAuthUpstreamHandler;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

/**
 * Class that if instantiated causes the {@link HttpAuthUpstreamHandler} to be registered in the {@link PipelineRegistry}
 */
@Singleton
public class AuthenticationHttpAuthHandlerRegistry {

    @Inject
    public AuthenticationHttpAuthHandlerRegistry(Settings settings,
                                                 PipelineRegistry pipelineRegistry,
                                                 Authentication authentication) {
        PipelineRegistry.ChannelPipelineItem pipelineItem = new PipelineRegistry.ChannelPipelineItem(
            "blob_handler",
            "auth_handler",
            () -> new HttpAuthUpstreamHandler(settings, authentication)
        );
        pipelineRegistry.addBefore(pipelineItem);
    }
}

/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.auth;

import io.crate.netty.channel.PipelineRegistry;
import io.crate.role.Roles;

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
                                                 Authentication authentication,
                                                 Roles roles) {
        PipelineRegistry.ChannelPipelineItem pipelineItem = new PipelineRegistry.ChannelPipelineItem(
            "blob_handler",
            "auth_handler",
            ignored -> new HttpAuthUpstreamHandler(settings, authentication, roles)
        );
        pipelineRegistry.addBefore(pipelineItem);
    }
}

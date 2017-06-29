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

package io.crate.rest;

import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;

/**
 * RestFilter that disables all endpoints but the ones listed in SUPPORTED_ENDPOINTS
 */
public class CrateRestFilter extends RestFilter {

    public static final Setting<Boolean> ES_API_ENABLED_SETTING = Setting.boolSetting(
        "es.api.enabled", false, Setting.Property.NodeScope);

    private static final Set<String> SUPPORTED_ENDPOINTS = ImmutableSet.of(
        "/index.html",
        "/static",
        "/admin",
        "/_sql",
        "/_plugin",
        "/_blobs"
    );

    // handle possible (wrong) URL '//' too
    // as some http clients create wrong requests to the ``root`` path '/' with '//'
    // we do handle arbitrary numbers of '/' in the path
    private static final Pattern MAIN_PATTERN = Pattern.compile(String.format(Locale.ENGLISH, "^%s+$", CrateRestMainAction.PATH));

    private final boolean esApiEnabled;

    @Inject
    public CrateRestFilter(Settings settings) {
        this.esApiEnabled = ES_API_ENABLED_SETTING.get(settings);
        Logger logger = Loggers.getLogger(getClass().getPackage().getName(), settings);
        logger.info("Elasticsearch HTTP REST API {}enabled", esApiEnabled ? "" : "not ");
    }

    @Override
    public void process(RestRequest request, RestChannel channel, NodeClient client, RestFilterChain filterChain) throws Exception {
        if (esApiEnabled || endpointAllowed(request.rawPath())) {
            filterChain.continueProcessing(request, channel, client);
        } else {
            channel.sendResponse(
                new BytesRestResponse(
                    BAD_REQUEST,
                    String.format(Locale.ENGLISH,
                        "No handler found for uri [%s] and method [%s]",
                        request.uri(),
                        request.method())
                ));
        }

    }

    private static boolean endpointAllowed(String rawPath) {
        if (MAIN_PATTERN.matcher(rawPath).matches()) {
            return true;
        }
        for (String supported : SUPPORTED_ENDPOINTS) {
            if (rawPath.startsWith(supported)) {
                return true;
            }
        }
        return false;
    }
}

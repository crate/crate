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

package io.crate.expression.scalar.string;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.common.Strings;

import io.crate.data.Input;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

public final class ParseURLFunction extends Scalar<Object, String> {

    private static final String NAME = "parse_url";

    public static void register(Functions.Builder module) {
        module.add(
            Signature.scalar(
                    NAME,
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.UNTYPED_OBJECT.getTypeSignature()
                ).withFeature(Feature.DETERMINISTIC)
                .withFeature(Scalar.Feature.NULLABLE),
            ParseURLFunction::new
        );
    }

    public ParseURLFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    @SafeVarargs
    public final Object evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<String>... args) {
        String url = args[0].value();
        if (url == null) {
            return null;
        }
        return parseURL(url);
    }

    private final Object parseURL(String urlText) {
        final Map<String, Object> urlMap = new HashMap<>();

        URL url = null;

        try {
            url = URL.of(new URI(urlText), null);
        } catch (MalformedURLException | URISyntaxException e1) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                            "unable to parse url %s",
                                                             urlText));
        }

        urlMap.put("scheme", url.getProtocol());
        urlMap.put("userinfo", decodeText(url.getUserInfo()));
        urlMap.put("hostname", url.getHost());
        urlMap.put("port", url.getPort() == -1 ? null : url.getPort());
        urlMap.put("path", decodeText(url.getPath()));
        urlMap.put("query", decodeText(url.getQuery()));
        urlMap.put("parameters", parseQuery(url.getQuery()));
        urlMap.put("fragment", decodeText(url.getRef()));

        return urlMap;
    }

    private final Map<String,List<String>> parseQuery(String query) {
        if (Strings.isNullOrEmpty(query)) {
            return null;
        }

        Map<String,List<String>> queryMap = new HashMap<String,List<String>>();
        String[] parameters = query.split("&(?!amp)");
        for (String parameter : parameters) {
            final int idx = parameter.indexOf("=");
            final String key = idx > 0 ? decodeText(parameter.substring(0, idx)) : decodeText(parameter);
            final String value = idx > 0 && parameter.length() > idx + 1 ? decodeText(parameter.substring(idx + 1)) : null;
            queryMap.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
        }
        return queryMap;
    }

    private final String decodeText(String text) {
        return text != null ? URLDecoder.decode(text, StandardCharsets.UTF_8) : null;
    }

}

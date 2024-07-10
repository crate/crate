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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import io.crate.data.Input;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

public final class ParseURIFunction extends Scalar<Object, String> {

    private static final String NAME = "parse_uri";

    public static void register(Functions.Builder module) {
        module.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.UNTYPED_OBJECT.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            ParseURIFunction::new
        );
    }

    public ParseURIFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    @SafeVarargs
    public final Object evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<String>... args) {
        String uri = args[0].value();
        if (uri == null) {
            return null;
        }
        return parseURI(uri);
    }

    private final Object parseURI(String uriText) {
        final Map<String, Object> uriMap = new HashMap<>();

        URI uri = null;

        try {
            uri = new URI(uriText);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                         "unable to parse uri %s",
                                                         uriText));
        }

        uriMap.put("scheme", uri.getScheme());
        uriMap.put("userinfo", uri.getUserInfo());
        uriMap.put("hostname", uri.getHost());
        uriMap.put("port", uri.getPort() == -1 ? null : uri.getPort());
        uriMap.put("path", uri.getPath());
        uriMap.put("query", uri.getQuery());
        uriMap.put("fragment", uri.getFragment());

        return uriMap;
    }
}

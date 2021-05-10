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

package io.crate.exceptions;

import io.crate.sql.Identifiers;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public class SchemaUnknownException extends ResourceUnknownException implements SchemaScopeException {

    private static final String MESSAGE_TMPL = "Schema '%s' unknown";

    private final String schemaName;

    public static SchemaUnknownException of(String schema, List<String> candidates) {
        switch (candidates.size()) {
            case 0:
                return new SchemaUnknownException(schema);

            case 1:
                return new SchemaUnknownException(
                    schema,
                    "Schema '" + schema + "' unknown. Maybe you meant '" + Identifiers.quoteIfNeeded(candidates.get(0)) + "'");

            default:
                String errorMsg = "Schema '" + schema + "' unknown. Maybe you meant one of: " + candidates.stream()
                    .map(Identifiers::quoteIfNeeded)
                    .collect(Collectors.joining(", "));
                return new SchemaUnknownException(schema, errorMsg);
        }
    }

    public SchemaUnknownException(String schema) {
        this(schema, String.format(Locale.ENGLISH, MESSAGE_TMPL, schema));
    }

    private SchemaUnknownException(String schema, String errorMessage) {
        super(errorMessage);
        this.schemaName = schema;
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }
}

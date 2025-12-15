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

package io.crate.analyze.relations;

import io.crate.analyze.ValuesResolver;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import org.jspecify.annotations.Nullable;
import java.util.List;

/**
 * A wrapper for an existing FieldProvider (normally NamedFieldProvider) which
 *
 *  a) checks if a QualifiedName matches the excluded table
 *  b) resolves the column name to Literal specified in the VALUES part of INSERT INTO
 *
 * Otherwise, it just calls the wrapped field provider.
 */
public class ExcludedFieldProvider implements FieldProvider<Symbol> {

    private final ValuesResolver valuesResolver;
    private final FieldProvider<?> fieldProvider;

    public ExcludedFieldProvider(FieldProvider<?> fieldProvider, ValuesResolver valuesResolver) {
        this.fieldProvider = fieldProvider;
        this.valuesResolver = valuesResolver;
    }

    @Override
    public Symbol resolveField(QualifiedName qualifiedName,
                               @Nullable List<String> path,
                               Operation operation,
                               boolean errorOnUnknownObjectKey) {
        List<String> parts = qualifiedName.getParts();
        if (parts.size() == 2 && parts.get(0).equals("excluded")) {
            String colName = parts.get(1);
            Symbol symbol = fieldProvider.resolveField(new QualifiedName(colName), path, operation, errorOnUnknownObjectKey);
            return valuesResolver.allocateAndResolve(symbol);
        }
        return fieldProvider.resolveField(qualifiedName, path, operation, errorOnUnknownObjectKey);
    }
}

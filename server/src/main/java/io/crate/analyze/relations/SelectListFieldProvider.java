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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import io.crate.analyze.relations.select.SelectAnalysis;
import io.crate.exceptions.AmbiguousColumnAliasException;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

public class SelectListFieldProvider implements FieldProvider<Symbol> {

    private final SelectAnalysis selectAnalysis;
    private final FieldProvider<? extends Symbol> fallback;

    public SelectListFieldProvider(SelectAnalysis selectAnalysis, FieldProvider<? extends Symbol> fallback) {
        this.selectAnalysis = selectAnalysis;
        this.fallback = fallback;
    }

    @Override
    public Symbol resolveField(QualifiedName qualifiedName,
                               List<String> path,
                               Operation operation,
                               boolean errorOnUnknownObjectKey) {
        List<String> parts = qualifiedName.getParts();
        if (parts.size() == 1 && (path == null || path.isEmpty())) {
            String part = parts.get(0);
            Symbol result = getOneOrAmbiguous(selectAnalysis.outputMultiMap(), part);
            if (result != null) {
                return result;
            }
        }
        return fallback.resolveField(qualifiedName, path, operation, errorOnUnknownObjectKey);
    }

    @Nullable
    public static Symbol getOneOrAmbiguous(Map<String, Set<Symbol>> selectList, String key) throws AmbiguousColumnAliasException {
        Collection<Symbol> symbols = selectList.get(key);
        if (symbols == null) {
            return null;
        }
        if (symbols.size() > 1) {
            throw new AmbiguousColumnAliasException(key, symbols);
        }
        return symbols.iterator().next();
    }
}

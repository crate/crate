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

import java.util.Locale;

import io.crate.analyze.RelationNames;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;

public class AmbiguousColumnAliasException extends RuntimeException implements TableScopeException {

    private final Iterable<? extends Symbol> relatedSymbols;

    public AmbiguousColumnAliasException(String alias, Iterable<? extends Symbol> relatedSymbols) {
        super(String.format(Locale.ENGLISH, "Column alias \"%s\" is ambiguous", alias));
        this.relatedSymbols = relatedSymbols;
    }

    @Override
    public Iterable<RelationName> getTableIdents() {
        return RelationNames.getDeep(relatedSymbols);
    }
}

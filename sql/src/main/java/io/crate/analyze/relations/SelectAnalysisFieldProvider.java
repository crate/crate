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

package io.crate.analyze.relations;

import com.google.common.collect.Iterables;
import io.crate.analyze.relations.select.SelectAnalysis;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.AmbiguousColumnAliasException;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

/**
 * Field provider that takes into account the select analysis output map. If first tries to resolve the symbol given the
 * provided {@link SelectAnalysis}, in case a symbol is not found it will return the symbol resolved by the delegate
 * {@link FieldProvider}
 */
public class SelectAnalysisFieldProvider implements FieldProvider<Symbol> {

    private final SelectAnalysis selectAnalysis;
    private final FieldProvider<? extends Symbol> delegate;

    public SelectAnalysisFieldProvider(SelectAnalysis selectAnalysis, FieldProvider<? extends Symbol> delegate) {
        this.selectAnalysis = selectAnalysis;
        this.delegate = delegate;
    }

    public Symbol resolveField(QualifiedName qualifiedName, Operation operation) {
        return resolveField(qualifiedName, null, operation);
    }

    public Symbol resolveField(QualifiedName qualifiedName, @Nullable List<String> path, Operation operation) {
        List<String> parts = qualifiedName.getParts();

        if (parts.size() == 1) {
            String element = Iterables.getOnlyElement(parts);
            Collection<Symbol> symbols = selectAnalysis.outputMultiMap().get(element);
            if (symbols.size() > 1) {
                throw new AmbiguousColumnAliasException(element, symbols);
            }
            if (!symbols.isEmpty()) {
                return symbols.iterator().next();
            }
        }

        return delegate.resolveField(qualifiedName, path, operation);
    }
}

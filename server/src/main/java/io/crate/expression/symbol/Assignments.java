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

package io.crate.expression.symbol;

import java.util.Map;

import org.jetbrains.annotations.NotNull;

import io.crate.analyze.ConstraintsValidator;
import io.crate.analyze.expressions.ValueNormalizer;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.SubQueryAndParamBinder;
import io.crate.planner.operators.SubQueryResults;

public final class Assignments {

    private final String[] targetNames;
    private final Reference[] targetColumns;
    private final Symbol[] sources;
    private final EvaluatingNormalizer normalizer;

    /**
     * convert assignments into a tuple of fqn column names and the symbols.
     * <p>
     * <pre>
     *     {
     *         users.age:  users.age + 1,
     *         users.name: users.name || 'foo'
     *
     *     }
     * </pre>
     * becomes
     * <pre>
     *     ( [users.age, users.name], [ Ref[users.age], Ref[users.name] ] [users.age + 1, users.name || 'foo'] )
     * </pre>
     *
     * @return a tuple or null if the input is null.
     */
    public static Assignments convert(@NotNull Map<Reference, ? extends Symbol> assignments, NodeContext nodeCtx) {
        String[] targetNames = new String[assignments.size()];
        Reference[] targetColumns = new Reference[assignments.size()];
        Symbol[] assignmentSymbols = new Symbol[assignments.size()];
        int i = 0;
        for (Map.Entry<Reference, ? extends Symbol> entry : assignments.entrySet()) {
            Reference key = entry.getKey();
            targetNames[i] = key.column().fqn();
            assignmentSymbols[i] = entry.getValue();
            targetColumns[i] = key;
            i++;
        }
        return new Assignments(targetNames, targetColumns, assignmentSymbols, nodeCtx);
    }

    private Assignments(String[] targetNames, Reference[] targetColumns, Symbol[] sources, NodeContext nodeCtx) {
        this.targetNames = targetNames;
        this.targetColumns = targetColumns;
        this.sources = sources;
        this.normalizer = EvaluatingNormalizer.functionOnlyNormalizer(nodeCtx);
    }

    public String[] targetNames() {
        return targetNames;
    }

    public Symbol[] sources() {
        return sources;
    }

    public Symbol[] bindSources(DocTableInfo tableInfo, Row params, SubQueryResults subQueryResults) {
        Symbol[] boundSources = new Symbol[targetColumns.length];
        SubQueryAndParamBinder binder = new SubQueryAndParamBinder(params, subQueryResults);
        for (int i = 0; i < boundSources.length; i++) {
            Symbol source = ValueNormalizer.normalizeInputForReference(
                binder.apply(sources[i]),
                targetColumns[i],
                tableInfo,
                s -> normalizer.normalize(s, CoordinatorTxnCtx.systemTransactionContext())
            );
            if (source instanceof Input<?> input) {
                ConstraintsValidator.validate(
                    input.value(), targetColumns[i], tableInfo.notNullColumns());
            }
            boundSources[i] = source;
        }
        return boundSources;
    }

}

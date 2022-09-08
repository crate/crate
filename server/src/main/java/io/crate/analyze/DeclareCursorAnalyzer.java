/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze;

import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.protocols.postgres.Portals;
import io.crate.sql.tree.DeclareCursor;

public class DeclareCursorAnalyzer {

    private final RelationAnalyzer relationAnalyzer;

    public DeclareCursorAnalyzer(RelationAnalyzer relationAnalyzer) {
        this.relationAnalyzer = relationAnalyzer;
    }

    public AnalyzedDeclareCursor analyze(DeclareCursor declareCursor,
                                         ParamTypeHints paramTypeHints,
                                         CoordinatorTxnCtx txnCtx,
                                         Portals portals) {
        String cursorName = declareCursor.getCursorName();
        if (portals.containsKey(cursorName)) {
            throw new IllegalArgumentException("The cursor '" + cursorName + "' already declared.");
        }
        return new AnalyzedDeclareCursor(
            declareCursor.getCursorName(),
            relationAnalyzer.analyze(declareCursor.getQuery(), txnCtx, paramTypeHints));
    }
}

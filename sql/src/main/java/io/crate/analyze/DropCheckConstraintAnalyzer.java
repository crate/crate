/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import io.crate.common.collections.Lists2;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.CheckConstraint;
import io.crate.sql.tree.Table;

import java.util.List;
import java.util.Locale;

class DropCheckConstraintAnalyzer {

    private final Schemas schemas;

    DropCheckConstraintAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    public AnalyzedAlterTableDropCheckConstraint analyze(Table<?> table, String name, CoordinatorTxnCtx txnCtx) {
        DocTableInfo tableInfo = (DocTableInfo) schemas.resolveTableInfo(
            table.getName(),
            Operation.ALTER,
            txnCtx.sessionContext().user(),
            txnCtx.sessionContext().searchPath());
        List<CheckConstraint<Symbol>> checkConstraints = tableInfo.checkConstraints();
        for (int i = 0; i < checkConstraints.size(); i++) {
            if (name.equals(checkConstraints.get(i).name())) {
                return new AnalyzedAlterTableDropCheckConstraint(tableInfo, name);
            }
        }
        throw new IllegalArgumentException(String.format(
            Locale.ENGLISH,
            "Cannot find a CHECK CONSTRAINT named [%s], available constraints are: %s",
            name,
            Lists2.map(checkConstraints, CheckConstraint::name)));
    }
}

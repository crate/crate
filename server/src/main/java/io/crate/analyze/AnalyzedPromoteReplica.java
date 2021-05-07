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

package io.crate.analyze;

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.table.ShardedTable;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.PromoteReplica;

import java.util.List;
import java.util.function.Consumer;

public class AnalyzedPromoteReplica extends RerouteAnalyzedStatement {

    private final PromoteReplica<Symbol> promoteReplica;
    private final Symbol acceptDataLoss;

    AnalyzedPromoteReplica(ShardedTable tableInfo,
                           List<Assignment<Symbol>> partitionProperties,
                           PromoteReplica<Symbol> promoteReplica,
                           Symbol acceptDataLoss) {
        super(tableInfo, partitionProperties);
        this.acceptDataLoss = acceptDataLoss;
        this.promoteReplica = promoteReplica;
    }

    public PromoteReplica<Symbol> promoteReplica() {
        return promoteReplica;
    }

    public Symbol acceptDataLoss() {
        return acceptDataLoss;
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        super.visitSymbols(consumer);
        consumer.accept(promoteReplica.shardId());
        consumer.accept(promoteReplica.node());
        consumer.accept(acceptDataLoss);
        // promoteReplica.properties() is not visited, because the `acceptDataLoss` value was extracted
        // from it and other properties are not permited
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitReroutePromoteReplica(this, context);
    }
}

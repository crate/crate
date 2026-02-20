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

import java.util.List;
import java.util.function.Consumer;

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.table.ShardedTable;
import io.crate.sql.tree.AllocatePrimaryShard;
import io.crate.sql.tree.Assignment;

public class AnalyzedRerouteAllocatePrimaryShard extends RerouteAnalyzedStatement {

    private final AllocatePrimaryShard<Symbol> allocatePrimaryShard;
    private final Symbol acceptDataLoss;

    AnalyzedRerouteAllocatePrimaryShard(ShardedTable tableInfo,
                                        List<Assignment<Symbol>> partitionProperties,
                                        AllocatePrimaryShard<Symbol> allocatePrimaryShard,
                                        Symbol acceptDataLoss) {
        super(tableInfo, partitionProperties);
        this.allocatePrimaryShard = allocatePrimaryShard;
        this.acceptDataLoss = acceptDataLoss;
    }

    public AllocatePrimaryShard<Symbol> rerouteAllocatePrimaryShard() {
        return allocatePrimaryShard;
    }

    public Symbol acceptDataLoss() {
        return acceptDataLoss;
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        super.visitSymbols(consumer);
        consumer.accept(allocatePrimaryShard.shardId());
        consumer.accept(allocatePrimaryShard.node());
        consumer.accept(acceptDataLoss);
        // allocatePrimaryShard.properties() is not visited, because the `acceptDataLoss` value was extracted
        // from it and other properties are not permitted
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitRerouteAllocatePrimaryShard(this, context);
    }
}

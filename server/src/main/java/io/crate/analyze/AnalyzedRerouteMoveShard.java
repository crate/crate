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
import io.crate.sql.tree.RerouteMoveShard;

import java.util.List;
import java.util.function.Consumer;

public class AnalyzedRerouteMoveShard extends RerouteAnalyzedStatement {

    private final RerouteMoveShard<Symbol> rerouteMoveShard;

    AnalyzedRerouteMoveShard(ShardedTable tableInfo,
                             List<Assignment<Symbol>> partitionProperties,
                             RerouteMoveShard<Symbol> rerouteMoveShard) {
        super(tableInfo, partitionProperties);
        this.rerouteMoveShard = rerouteMoveShard;
    }

    public RerouteMoveShard<Symbol> rerouteMoveShard() {
        return rerouteMoveShard;
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        super.visitSymbols(consumer);
        consumer.accept(rerouteMoveShard.shardId());
        consumer.accept(rerouteMoveShard.fromNodeIdOrName());
        consumer.accept(rerouteMoveShard.toNodeIdOrName());
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitRerouteMoveShard(this, context);
    }
}

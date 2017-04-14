/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner.node.dql;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.symbol.DefaultTraversalSymbolVisitor;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;

import java.util.List;

public class GroupByConsumer {

    private static final GroupByValidator GROUP_BY_VALIDATOR = new GroupByValidator();

    public static boolean groupedByClusteredColumnOrPrimaryKeys(DocTableRelation tableRelation, WhereClause whereClause, List<Symbol> groupBySymbols) {
        if (groupBySymbols.size() > 1) {
            return groupedByPrimaryKeys(tableRelation, groupBySymbols);
        }

        /**
         * if the table has more than one partition there are multiple shards which might even be on different nodes
         * so one shard doesn't contain all "clustered by" values
         * -> need to use a distributed group by.
         */
        if (tableRelation.tableInfo().isPartitioned() && whereClause.partitions().size() != 1) {
            return false;
        }

        // this also handles the case if there is only one primary key.
        // as clustered by column == pk column  in that case
        Symbol groupByKey = groupBySymbols.get(0);
        return (groupByKey instanceof Reference
                && ((Reference) groupByKey).ident().columnIdent()
                    .equals(tableRelation.tableInfo().clusteredBy()));
    }

    private static boolean groupedByPrimaryKeys(DocTableRelation tableRelation, List<Symbol> groupBy) {
        List<ColumnIdent> primaryKeys = tableRelation.tableInfo().primaryKey();
        if (groupBy.size() != primaryKeys.size()) {
            return false;
        }
        for (int i = 0, groupBySize = groupBy.size(); i < groupBySize; i++) {
            Symbol groupBySymbol = groupBy.get(i);
            if (groupBySymbol instanceof Reference) {
                ColumnIdent columnIdent = ((Reference) groupBySymbol).ident().columnIdent();
                ColumnIdent pkIdent = primaryKeys.get(i);
                if (!pkIdent.equals(columnIdent)) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    public static void validateGroupBySymbols(List<Symbol> groupBySymbols) {
        for (Symbol symbol : groupBySymbols) {
            GROUP_BY_VALIDATOR.process(symbol, null);
        }
    }

    private static class GroupByValidator extends DefaultTraversalSymbolVisitor<Void, Void> {

        @Override
        public Void visitReference(Reference symbol, Void context) {
            if (symbol.indexType() == Reference.IndexType.ANALYZED) {
                throw new IllegalArgumentException(
                    SymbolFormatter.format("Cannot GROUP BY '%s': grouping on analyzed/fulltext columns is not possible", symbol));
            } else if (symbol.indexType() == Reference.IndexType.NO) {
                throw new IllegalArgumentException(
                    SymbolFormatter.format("Cannot GROUP BY '%s': grouping on non-indexed columns is not possible", symbol));
            }
            return null;
        }

        @Override
        public Void visitField(Field field, Void context) {
            throw new UnsupportedOperationException("Field must have been resolved to Reference already");
        }
    }
}

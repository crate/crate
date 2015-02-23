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

import io.crate.analyze.relations.TableRelation;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.Routing;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;

import java.util.List;
import java.util.Map;

public class GroupByConsumer {

    private static final GroupByValidator GROUP_BY_VALIDATOR = new GroupByValidator();

    public static boolean requiresDistribution(TableInfo tableInfo, Routing routing) {
        if (tableInfo.rowGranularity().ordinal() < RowGranularity.DOC.ordinal()) return false;
        if (!routing.hasLocations()) return false;
        Map<String, Map<String, List<Integer>>> locations = routing.locations();
        return (locations != null && locations.size() > 1);
    }

    public static boolean groupedByClusteredColumnOrPrimaryKeys(TableRelation tableRelation, List<Symbol> groupBySymbols) {
        if (groupBySymbols.size() > 1) {
            return groupedByPrimaryKeys(tableRelation, groupBySymbols);
        }

        // this also handles the case if there is only one primary key.
        // as clustered by column == pk column  in that case
        Symbol groupByKey = groupBySymbols.get(0);
        return (groupByKey instanceof Reference
                && ((Reference) groupByKey).info().ident().columnIdent()
                    .equals(tableRelation.tableInfo().clusteredBy()));
    }

    private static boolean groupedByPrimaryKeys(TableRelation tableRelation, List<Symbol> groupBy) {
        List<ColumnIdent> primaryKeys = tableRelation.tableInfo().primaryKey();
        if (groupBy.size() != primaryKeys.size()) {
            return false;
        }
        for (int i = 0, groupBySize = groupBy.size(); i < groupBySize; i++) {
            Symbol groupBySymbol = groupBy.get(i);
            if (groupBySymbol instanceof Reference) {
                ColumnIdent columnIdent = ((Reference) groupBySymbol).info().ident().columnIdent();
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

    public static void validateGroupBySymbols(TableRelation tableRelation, List<Symbol> groupBySymbols) {
        for (Symbol symbol : groupBySymbols) {
            GROUP_BY_VALIDATOR.process(symbol, tableRelation);
        }
    }

    private static class GroupByValidator extends SymbolVisitor<TableRelation, Void> {

        @Override
        public Void visitFunction(Function symbol, TableRelation context) {
            for (Symbol arg : symbol.arguments()) {
                process(arg, context);
            }
            return null;
        }

        @Override
        public Void visitReference(Reference symbol, TableRelation context) {
            if (symbol.info().indexType() == ReferenceInfo.IndexType.ANALYZED) {
                throw new IllegalArgumentException(
                        String.format("Cannot GROUP BY '%s': grouping on analyzed/fulltext columns is not possible",
                                SymbolFormatter.format(symbol)));
            } else if (symbol.info().indexType() == ReferenceInfo.IndexType.NO) {
                throw new IllegalArgumentException(
                        String.format("Cannot GROUP BY '%s': grouping on non-indexed columns is not possible",
                                SymbolFormatter.format(symbol)));
            }
            return null;
        }

        @Override
        public Void visitField(Field field, TableRelation context) {
            return process(context.resolveField(field), context);
        }
    }
}

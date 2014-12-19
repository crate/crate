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

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Routing;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class GroupByConsumer {

    public static boolean requiresDistribution(TableInfo tableInfo, List<Symbol> groupBySymbols, Routing routing) {
        if (tableInfo.rowGranularity().ordinal() < RowGranularity.DOC.ordinal()) return false;
        if (!routing.hasLocations()) return false;
        if (groupedByClusteredColumnOrPrimaryKeys(tableInfo, groupBySymbols)) return false;
        Map<String, Map<String, Set<Integer>>> locations = routing.locations();
        return (locations != null && locations.size() > 1);
    }

    public static boolean groupedByClusteredColumnOrPrimaryKeys(TableInfo tableInfo, List<Symbol> groupBySymbols) {
        if (groupBySymbols.size() > 1) {
            return groupedByPrimaryKeys(groupBySymbols, tableInfo.primaryKey());
        }

        // this also handles the case if there is only one primary key.
        // as clustered by column == pk column  in that case
        Symbol groupByKey = groupBySymbols.get(0);
        return (groupByKey instanceof Reference
                && ((Reference) groupByKey).info().ident().columnIdent().equals(tableInfo.clusteredBy()));
    }

    private static boolean groupedByPrimaryKeys(List<Symbol> groupBy, List<ColumnIdent> primaryKeys) {
        if (groupBy.size() != primaryKeys.size()) {
            return false;
        }
        for (int i = 0, groupBySize = groupBy.size(); i < groupBySize; i++) {
            Symbol groupBySymbol = groupBy.get(i);
            if (groupBySymbol instanceof Reference) {
                ColumnIdent pkIdent = primaryKeys.get(i);
                if (!pkIdent.equals(((Reference) groupBySymbol).info().ident().columnIdent())) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }
}

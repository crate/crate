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

package io.crate.expression.tablefunctions;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.sql.Identifiers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.elasticsearch.cluster.ClusterState;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

public final class PgGetKeywordsFunction extends TableFunctionImplementation<List<Object>> {

    public static final String NAME = "pg_get_keywords";
    private static final FunctionName FUNCTION_NAME = new FunctionName(PgCatalogSchemaInfo.NAME, NAME);
    private static final RelationName REL_NAME = new RelationName(PgCatalogSchemaInfo.NAME, NAME);
    private static final PgGetKeywordsFunction INSTANCE = new PgGetKeywordsFunction();
    private final FunctionInfo info;

    public static void register(TableFunctionModule module) {
        module.register(
            FUNCTION_NAME,
            new BaseFunctionResolver(FuncParams.NONE) {

                @Override
                public FunctionImplementation getForTypes(List<DataType> argTypes) throws IllegalArgumentException {
                    assert argTypes.isEmpty() : "argument types for pg_get_keywords must be empty due to FuncParams definition";
                    return PgGetKeywordsFunction.INSTANCE;
                }
            }
        );
    }

    static class Columns {
        static final ColumnIdent WORD = new ColumnIdent("word");
        static final ColumnIdent CATCODE = new ColumnIdent("catcode");
        static final ColumnIdent CATDESC = new ColumnIdent("catdesc");
    }

    public PgGetKeywordsFunction() {
        info = new FunctionInfo(
            new FunctionIdent(FUNCTION_NAME, List.of()),
            ObjectType.untyped(),
            FunctionInfo.Type.TABLE
        );
    }

    @Override
    public TableInfo createTableInfo() {
        LinkedHashMap<ColumnIdent, Reference> columnMap = new LinkedHashMap<>();
        columnMap.put(
            Columns.WORD,
            new Reference(new ReferenceIdent(REL_NAME, Columns.WORD), RowGranularity.DOC, DataTypes.STRING, 0, null)
        );
        columnMap.put(
            Columns.CATCODE,
            new Reference(new ReferenceIdent(REL_NAME, Columns.CATCODE), RowGranularity.DOC, DataTypes.STRING, 1, null)
        );
        columnMap.put(
            Columns.CATDESC,
            new Reference(new ReferenceIdent(REL_NAME, Columns.CATDESC), RowGranularity.DOC, DataTypes.STRING, 2, null)
        );
        return new StaticTableInfo<>(REL_NAME, columnMap, columnMap.values(), List.of()) {

            @Override
            public Routing getRouting(ClusterState state,
                                      RoutingProvider routingProvider,
                                      WhereClause whereClause,
                                      RoutingProvider.ShardSelection shardSelection,
                                      SessionContext sessionContext) {
                return Routing.forTableOnSingleNode(REL_NAME, state.getNodes().getLocalNodeId());
            }
        };
    }

    @Override
    public Iterable<Row> evaluate(TransactionContext txnCtx, Input<List<Object>>... args) {
        return () -> Identifiers.KEYWORDS.stream()
            .map(new Function<Identifiers.Keyword, Row>() {

                final Object[] columns = new Object[3];
                final RowN row = new RowN(columns);

                @Override
                public Row apply(Identifiers.Keyword keyword) {
                    columns[0] = keyword.getWord().toLowerCase(Locale.ENGLISH);
                    if (keyword.isReserved()) {
                        columns[1] = "R";
                        columns[2] = "reserved";
                    } else {
                        columns[1] = "U";
                        columns[2] = "unreserved";
                    }
                    return row;
                }
            }).iterator();
    }

    @Override
    public FunctionInfo info() {
        return info;
    }
}

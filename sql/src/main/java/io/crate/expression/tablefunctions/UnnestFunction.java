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
import io.crate.data.Bucket;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.ObjectType;
import org.elasticsearch.cluster.ClusterState;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static io.crate.metadata.functions.params.Param.ANY_ARRAY;

public class UnnestFunction {

    public static final String NAME = "unnest";
    public static final RelationName TABLE_IDENT = new RelationName("", NAME);

    static class UnnestTableFunctionImplementation extends TableFunctionImplementation<List<Object>> {

        private final FunctionInfo info;

        private UnnestTableFunctionImplementation(FunctionInfo info) {
            this.info = info;
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        /**
         *
         * @param arguments collection of array-literals
         *                  e.g. [ [1, 2], [Marvin, Trillian] ]
         * @return Bucket containing the unnested rows.
         * [ [1, Marvin], [2, Trillian] ]
         */
        @SafeVarargs
        @Override
        public final Bucket evaluate(TransactionContext txnCtx, Input<List<Object>>... arguments) {
            final List<List<Object>> values = extractValues(arguments);
            final int numCols = arguments.length;
            final int numRows = maxLength(values);

            return new Bucket() {
                final Object[] cells = new Object[numCols];
                final RowN row = new RowN(cells);

                @Override
                public int size() {
                    return numRows;
                }

                @Override
                @Nonnull
                public Iterator<Row> iterator() {
                    return new Iterator<>() {

                        int currentRow = 0;

                        @Override
                        public boolean hasNext() {
                            return currentRow < numRows;
                        }

                        @Override
                        public Row next() {
                            if (!hasNext()) {
                                throw new NoSuchElementException("No more rows");
                            }
                            for (int c = 0; c < numCols; c++) {
                                List<Object> columnValues = values.get(c);
                                if (columnValues == null || columnValues.size() <= currentRow) {
                                    cells[c] = null;
                                } else {
                                    cells[c] = columnValues.get(currentRow);
                                }
                            }
                            currentRow++;
                            return row;
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException("remove is not supported for " +
                                                                    UnnestFunction.class.getSimpleName() + "$iterator");
                        }
                    };
                }
            };
        }

        @SafeVarargs
        private static List<List<Object>> extractValues(Input<List<Object>> ... arguments) {
            List<List<Object>> values = new ArrayList<>(arguments.length);
            for (Input<List<Object>> argument : arguments) {
                values.add(argument.value());
            }
            return values;
        }

        @Override
        public TableInfo createTableInfo() {
            int noElements = info.ident().argumentTypes().size();
            Map<ColumnIdent, Reference> columnMap = new LinkedHashMap<>(noElements);
            Collection<Reference> columns = new ArrayList<>(noElements);

            for (int i = 0; i < info.ident().argumentTypes().size(); i++) {
                ColumnIdent columnIdent = new ColumnIdent("col" + (i + 1));
                DataType dataType = ((ArrayType) info.ident().argumentTypes().get(i)).innerType();
                Reference reference = new Reference(
                    new ReferenceIdent(TABLE_IDENT, columnIdent), RowGranularity.DOC, dataType, i, null
                );

                columns.add(reference);
                columnMap.put(columnIdent, reference);
            }
            return new StaticTableInfo(TABLE_IDENT, columnMap, columns, Collections.emptyList()) {
                @Override
                public RowGranularity rowGranularity() {
                    return RowGranularity.DOC;
                }

                @Override
                public Routing getRouting(ClusterState state,
                                          RoutingProvider routingProvider,
                                          WhereClause whereClause,
                                          RoutingProvider.ShardSelection shardSelection,
                                          SessionContext sessionContext) {
                    return Routing.forTableOnSingleNode(TABLE_IDENT, state.getNodes().getLocalNodeId());
                }
            };
        }
    }

    private static int maxLength(List<List<Object>> values) {
        int length = 0;
        for (List<Object> value : values) {
            if (value != null && value.size() > length) {
                length = value.size();
            }
        }
        return length;
    }

    public static void register(TableFunctionModule module) {
        module.register(NAME, new BaseFunctionResolver(
            FuncParams.builder().withIndependentVarArgs(ANY_ARRAY).build()) {

            @Override
            public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
                DataType returnType = dataTypes.size() == 1 ? ArrayType.unnest(dataTypes.get(0)) : ObjectType.untyped();
                return new UnnestTableFunctionImplementation(
                    new FunctionInfo(new FunctionIdent(NAME, dataTypes), returnType, FunctionInfo.Type.TABLE));
            }
        });
    }
}

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

package io.crate.operation.tablefunctions;

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
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Signature;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class UnnestFunction {

    private static final String NAME = "unnest";
    private static final TableIdent TABLE_IDENT = new TableIdent("", NAME);

    static class UnnestTableFunctionImplementation implements TableFunctionImplementation {

        private final FunctionInfo info;

        private UnnestTableFunctionImplementation(FunctionInfo info) {
            this.info = info;
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        /**
         * @param arguments collection of array-literals
         *                  e.g. [ [1, 2], [Marvin, Trillian] ]
         * @return Bucket containing the unnested rows.
         * [ [1, Marvin], [2, Trillian] ]
         */
        @Override
        public Bucket execute(Collection<? extends Input> arguments) {
            final List<Object[]> values = extractValues(arguments);
            final int numCols = arguments.size();
            final int numRows = maxLength(values);

            return new Bucket() {
                final Object[] cells = new Object[numCols];
                final RowN row = new RowN(cells);

                @Override
                public int size() {
                    return numRows;
                }

                @Override
                public Iterator<Row> iterator() {
                    return new Iterator<Row>() {

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
                                Object[] columnValues = values.get(c);
                                if (columnValues.length > currentRow) {
                                    cells[c] = columnValues[currentRow];
                                } else {
                                    cells[c] = null;
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

        private static List<Object[]> extractValues(Collection<? extends Input> arguments) {
            List<Object[]> values = new ArrayList<>(arguments.size());
            for (Input argument : arguments) {
                Object value = argument.value();
                assert value instanceof Object[] : "must be an array because unnest only accepts array arguments";
                Object[] columnValues = (Object[]) value;
                values.add(columnValues);
            }
            return values;
        }

        @Override
        public TableInfo createTableInfo(ClusterService clusterService) {
            int noElements = info.ident().argumentTypes().size();
            Map<ColumnIdent, Reference> columnMap = new LinkedHashMap<>(noElements);
            Collection<Reference> columns = new ArrayList<>(noElements);

            for (int i = 0; i < info.ident().argumentTypes().size(); i++) {
                ColumnIdent columnIdent = new ColumnIdent("col" + (i + 1));
                DataType dataType = ((CollectionType) info.ident().argumentTypes().get(i)).innerType();
                Reference reference = new Reference(
                    new ReferenceIdent(TABLE_IDENT, columnIdent),
                    RowGranularity.DOC, dataType);

                columns.add(reference);
                columnMap.put(columnIdent, reference);
            }
            final String localNodeId = clusterService.localNode().getId();
            return new StaticTableInfo(TABLE_IDENT, columnMap, columns, Collections.emptyList()) {
                @Override
                public RowGranularity rowGranularity() {
                    return RowGranularity.DOC;
                }

                @Override
                public Routing getRouting(WhereClause whereClause,
                                          @Nullable String preference,
                                          SessionContext sessionContext) {
                    return Routing.forTableOnSingleNode(TABLE_IDENT, localNodeId);
                }
            };
        }
    }

    private static int maxLength(List<Object[]> values) {
        int length = 0;
        for (Object[] value : values) {
            if (value.length > length) {
                length = value.length;
            }
        }
        return length;
    }

    public static void register(TableFunctionModule module){
        module.register(NAME, new BaseFunctionResolver(
            Signature.withLenientVarArgs(Signature.ArgMatcher.ANY_ARRAY)) {

            @Override
            public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
                return new UnnestTableFunctionImplementation(
                    new FunctionInfo(new FunctionIdent(NAME, dataTypes), DataTypes.OBJECT, FunctionInfo.Type.TABLE));
            }
        });
    }
}

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

import io.crate.analyze.WhereClause;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.operation.Input;
import io.crate.types.ArrayType;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import org.elasticsearch.cluster.service.ClusterService;

import javax.annotation.Nullable;
import java.util.*;

public class Unnest implements TableFunctionImplementation {

    private final static String NAME = "unnest";

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
                                                                Unnest.class.getSimpleName() + "$iterator");
                    }
                };
            }
        };
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
    public TableInfo createTableInfo(ClusterService clusterService, List<? extends DataType> argumentTypes) {
        validateTypes(argumentTypes);

        final TableIdent tableIdent = new TableIdent(null, NAME);
        ColumnRegistrar columnRegistrar = new ColumnRegistrar(tableIdent, RowGranularity.DOC);
        for (int i = 0; i < argumentTypes.size(); i++) {
            columnRegistrar.register(new ColumnIdent(
                "col" + (i + 1)), ((CollectionType) argumentTypes.get(i)).innerType());
        }

        final String localNodeId = clusterService.localNode().getId();
        return new StaticTableInfo(tableIdent, columnRegistrar, Collections.<ColumnIdent>emptyList()) {
            @Override
            public RowGranularity rowGranularity() {
                return RowGranularity.DOC;
            }

            @Override
            public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
                return Routing.forTableOnSingleNode(tableIdent, localNodeId);
            }
        };
    }

    private static void validateTypes(List<? extends DataType> argumentTypes) {
        ListIterator<? extends DataType> it = argumentTypes.listIterator();
        if (!it.hasNext()) {
            throw new IllegalArgumentException("unnest expects at least 1 argument of type array. Got 0");
        }
        while (it.hasNext()) {
            DataType dataType = it.next();
            if (dataType.id() != ArrayType.ID) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "unnest expects arguments of type array. " +
                    "Got an argument of type '%s' at position %d instead.", dataType, it.previousIndex()));
            }
        }
    }

    public static void register(TableFunctionModule tableFunctionModule) {
        tableFunctionModule.register(NAME, new Unnest());
    }
}

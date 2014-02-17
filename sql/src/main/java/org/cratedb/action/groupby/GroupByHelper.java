/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.cratedb.action.groupby;

import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.cratedb.action.collect.ColumnReferenceExpression;
import org.cratedb.action.collect.Expression;
import org.cratedb.action.collect.scope.GlobalExpressionDescription;
import org.cratedb.action.collect.scope.ScopedExpression;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.parser.ColumnDescription;
import org.cratedb.action.parser.ColumnReferenceDescription;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.core.collections.LimitingCollectionIterator;
import org.cratedb.mapper.FieldMapper;

import java.util.*;

public class GroupByHelper {

    public static Collection<GroupByRow> trimRows(List<GroupByRow> rows,
                                                  Comparator<GroupByRow> comparator,
                                                  int totalLimit) {
        // sorting/trim is only called if if something CAN be trimmed.
        // Otherwise the sorting would just be overhead because the Handler node will sort everything anyway.
        if (rows.size() > totalLimit) {
            return sortAndTrimRows(rows, comparator, totalLimit);
        }
        return rows;
    }

    public static Collection<GroupByRow> sortAndTrimRows(List<GroupByRow> rows,
                                                         Comparator<GroupByRow> comparator,
                                                         int totalLimit) {
        Collections.sort(rows, comparator);
        return new LimitingCollectionIterator<>(rows, totalLimit);
    }

    /**
     * this method can be used to built a GroupByFieldExtractor array which provides fast value
     * lookup on GroupByRows.
     *
     * The Array is in the same order as the parsedStatements resultColumnList
     *
     * E.g.
     *
     *  GroupByFieldExtractor[1].getValue(groupByRow) will return the value for the second column in
     *  the resultColumnList.
     *
     * see also {@link GroupByFieldExtractor}
     */
    public static GroupByFieldExtractor[] buildFieldExtractor(ParsedStatement parsedStatement,
                                                              final FieldMapper fieldMapper) {
        GroupByFieldExtractor[] extractors = new GroupByFieldExtractor[parsedStatement
                .resultColumnList().size()];

        int colIdx = 0;
        int aggStateIdx = 0;
        int keyValIdx;

        for (final ColumnDescription columnDescription : parsedStatement.resultColumnList()) {
            if (columnDescription instanceof AggExpr) {
                // fieldMapper is null in case of group by on information schema

                if (fieldMapper != null && (columnDescription).returnType() == DataType.STRING)
                {
                    extractors[colIdx] = new GroupByFieldExtractor(aggStateIdx) {
                        @Override
                        public Object getValue(GroupByRow row) {
                            BytesRef bytesRef = (BytesRef)row.aggStates.get(idx).value();
                            if (bytesRef != null) {
                                return bytesRef.utf8ToString();
                            }
                            return null;
                        }
                    };
                } else {
                    extractors[colIdx] = new GroupByFieldExtractor(aggStateIdx) {
                        @Override
                        public Object getValue(GroupByRow row) {
                            return row.aggStates.get(idx).value();
                        }
                    };
                }
                aggStateIdx++;
            } else if (columnDescription instanceof ColumnReferenceDescription) {

                String colName =  columnDescription.name();
                keyValIdx = 0;
                for (Expression e: parsedStatement.groupByExpressions()){
                    if (e instanceof ColumnReferenceExpression &&
                            colName.equals(((ColumnReferenceExpression) e).columnName())){

                        if (fieldMapper != null && e.returnType() == DataType.STRING) {
                            extractors[colIdx] = new GroupByFieldExtractor(keyValIdx) {
                                @Override
                                public Object getValue(GroupByRow row) {
                                    BytesRef bytesRef = (BytesRef)row.key.get(idx);
                                    if (bytesRef != null) {
                                        return bytesRef.utf8ToString();
                                    }
                                    return null;
                                }
                            };
                        } else {
                            extractors[colIdx] = new GroupByFieldExtractor(keyValIdx) {
                                @Override
                                public Object getValue(GroupByRow row) {
                                    return row.key.get(idx);
                                }
                            };
                        }
                        break;
                    }
                    keyValIdx++;
                }

            } else if (columnDescription instanceof GlobalExpressionDescription) {
                String colName = columnDescription.name();
                int globalExprIdx = 0;
                for (final ScopedExpression<?> e : parsedStatement.globalExpressions()) {
                    if (colName.equals(e.name())) {
                        if (e.returnType() == DataType.STRING) {
                            extractors[colIdx] = new GroupByFieldExtractor(globalExprIdx) {
                                @Override
                                public Object getValue(GroupByRow row) {
                                    BytesRef bytesRef = (BytesRef)e.evaluate();
                                    if (bytesRef != null) {
                                        return bytesRef.utf8ToString();
                                    }
                                    return null;
                                }
                            };
                        } else {
                            extractors[colIdx] = new GroupByFieldExtractor(globalExprIdx) {
                                @Override
                                public Object getValue(GroupByRow row) {
                                    return e.evaluate();
                                }
                            };
                        }
                    }
                    globalExprIdx++;
                }
            }
            colIdx++;
        }

        return extractors;
    }

    public static Object[][] sortedRowsToObjectArray(Collection<GroupByRow> rows,
                                                     ParsedStatement parsedStatement,
                                                     GroupByFieldExtractor[] fieldExtractors) {
        int rowCount = Math.max(0, rows.size() - parsedStatement.offset());
        Object[][] result = new Object[parsedStatement.isGlobalAggregate() ? 1 : rowCount][parsedStatement.outputFields().size()];
        int currentRow = -1;
        int remainingOffset = parsedStatement.offset();
        if (parsedStatement.isGlobalAggregate()) {
            if (rowCount == 0) {
                // fill with initial (mostly NULL) values
                int aggExprIdx = 0;
                int globalExprIdx = 0;
                int colIdx = 0;
                for (ColumnDescription columnDescription : parsedStatement.resultColumnList()) {
                    if (columnDescription instanceof AggExpr) {
                        result[0][colIdx++] = parsedStatement.aggregateExpressions().get(aggExprIdx++).createAggState().value();
                    } else if (columnDescription instanceof GlobalExpressionDescription) {
                        Object rowValue = parsedStatement.globalExpressions().get(globalExprIdx++).evaluate();
                        if (rowValue != null && rowValue instanceof BytesRef) {
                            rowValue = ((BytesRef)rowValue).utf8ToString();
                        }
                        result[0][colIdx++] = rowValue;
                    } else {
                        result[0][colIdx++] = null;
                    }
                }
                return result;
            } else {
                // final merge
                Iterator<GroupByRow> iter = rows.iterator();
                final GroupByRow resultRow = iter.next();
                while (iter.hasNext()) {
                    resultRow.merge(iter.next());
                }
                rows = new ArrayList<GroupByRow>(1) {{ add(resultRow); }};
            }
        }

        for (GroupByRow row : rows) {
            if (remainingOffset > 0) {
                remainingOffset--;
                continue;
            }
            currentRow++;
            for (int i = 0; i < result[currentRow].length; i++) {
                result[currentRow][i] = fieldExtractors[i].getValue(row);
            }
        }

        return result;
    }
}

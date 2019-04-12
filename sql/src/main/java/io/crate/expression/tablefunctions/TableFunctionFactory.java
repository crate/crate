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
import io.crate.data.ArrayBucket;
import io.crate.data.Bucket;
import io.crate.data.Input;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import org.elasticsearch.cluster.ClusterState;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class TableFunctionFactory {

    public static TableFunctionImplementation from(FunctionImplementation functionImplementation) {

        TableFunctionImplementation tableFunction;
        switch (functionImplementation.info().type()) {
            case TABLE:
                tableFunction = (TableFunctionImplementation) functionImplementation;
                break;
            case SCALAR:
                tableFunction = new ScalarTableFunctionImplementation<>((Scalar<?, ?>) functionImplementation);
                break;
            case WINDOW:
            case AGGREGATE:
                throw new UnsupportedOperationException(
                    String.format(
                        Locale.ENGLISH,
                        "Window or Aggregate function: '%s' is not allowed in function in FROM clause",
                        functionImplementation.info().ident().name()));
            default:
                throw new UnsupportedOperationException(
                    String.format(
                        Locale.ENGLISH,
                        "Unknown type function: '%s' is not allowed in function in FROM clause",
                        functionImplementation.info().ident().name()));
        }
        return tableFunction;
    }

    /**
     * Evaluates the {@link Scalar} function and emits scalar result as a 1x1 table
     */
    private static class ScalarTableFunctionImplementation<T> extends TableFunctionImplementation<T> {

        private final RelationName TABLE_IDENT = new RelationName("", "scalar_table");

        private final Scalar<?, T> functionImplementation;

        private ScalarTableFunctionImplementation(Scalar<?, T> functionImplementation) {
            this.functionImplementation = functionImplementation;
        }

        @Override
        public FunctionInfo info() {
            return functionImplementation.info();
        }

        @Override
        public Bucket evaluate(TransactionContext txnCtx, Input<T>[] args) {
            return new ArrayBucket(new Object[][] {new Object[] {functionImplementation.evaluate(txnCtx,args)}});
        }

        @Override
        public TableInfo createTableInfo() {
            String functionName = info().ident().name();
            ColumnIdent col = new ColumnIdent(functionName);
            Reference reference = new Reference(new ReferenceIdent(TABLE_IDENT, col),
                                                RowGranularity.DOC,
                                                info().returnType(),
                                                1);
            Map<ColumnIdent, Reference> referenceByColumn = Collections.singletonMap(col, reference);
            return new StaticTableInfo(TABLE_IDENT, referenceByColumn, List.of(reference), List.of()) {
                @Override
                public Routing getRouting(ClusterState state,
                                          RoutingProvider routingProvider,
                                          WhereClause whereClause,
                                          RoutingProvider.ShardSelection shardSelection,
                                          SessionContext sessionContext) {
                    return Routing.forTableOnSingleNode(TABLE_IDENT, state.getNodes().getLocalNodeId());
                }

                @Override
                public RowGranularity rowGranularity() {
                    return RowGranularity.DOC;
                }
            };
        }
    }
}

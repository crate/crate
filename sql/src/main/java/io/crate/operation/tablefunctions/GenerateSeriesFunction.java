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

import com.google.common.collect.ImmutableList;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Bucket;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.metadata.*;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;

import javax.annotation.Nullable;
import java.util.*;

public class GenerateSeriesFunction {

    private static final String NAME = "generate_series";
    private static final TableIdent TABLE_IDENT = new TableIdent(null, NAME);

    private static final Map<DataType, FunctionImplementation> implTwoArgs;
    private static final Map<DataType, FunctionImplementation> implThreeArgs;

    static {
        implTwoArgs = new HashMap<>();
        implThreeArgs = new HashMap<>();

        DataType type = DataTypes.INTEGER;

        implTwoArgs.put(type, new IntegerGenerateSeriesFunctionImplementation(new FunctionInfo(new FunctionIdent(NAME, ImmutableList.of(type, type)), type, FunctionInfo.Type.TABLE)));
        implThreeArgs.put(type, new IntegerGenerateSeriesFunctionImplementation(new FunctionInfo(new FunctionIdent(NAME, ImmutableList.of(type, type, type)), type, FunctionInfo.Type.TABLE)));

        type = DataTypes.LONG;

        implTwoArgs.put(type, new LongGenerateSeriesFunctionImplementation(new FunctionInfo(new FunctionIdent(NAME, ImmutableList.of(type, type)), type, FunctionInfo.Type.TABLE)));
        implThreeArgs.put(type, new LongGenerateSeriesFunctionImplementation(new FunctionInfo(new FunctionIdent(NAME, ImmutableList.of(type, type, type)), type, FunctionInfo.Type.TABLE)));

        type = DataTypes.FLOAT;

        implTwoArgs.put(type, new FloatGenerateSeriesFunctionImplementation(new FunctionInfo(new FunctionIdent(NAME, ImmutableList.of(type, type)), type, FunctionInfo.Type.TABLE)));
        implThreeArgs.put(type, new FloatGenerateSeriesFunctionImplementation(new FunctionInfo(new FunctionIdent(NAME, ImmutableList.of(type, type, type)), type, FunctionInfo.Type.TABLE)));

        type = DataTypes.DOUBLE;

        implTwoArgs.put(type, new DoubleGenerateSeriesFunctionImplementation(new FunctionInfo(new FunctionIdent(NAME, ImmutableList.of(type, type)), type, FunctionInfo.Type.TABLE)));
        implThreeArgs.put(type, new DoubleGenerateSeriesFunctionImplementation(new FunctionInfo(new FunctionIdent(NAME, ImmutableList.of(type, type, type)), type, FunctionInfo.Type.TABLE)));
    }

    public static void register(TableFunctionModule module){
        module.register(NAME, new BaseFunctionResolver(
            Signature.numArgs(2,3).and(Signature.withLenientVarArgs(Signature.ArgMatcher.NUMERIC))) {

            @Override
            public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
                Map<DataType, FunctionImplementation> impl = dataTypes.size() == 2 ? implTwoArgs : implThreeArgs;
                DataType first = dataTypes.get(0);
                DataType third = dataTypes.size() > 2 ? dataTypes.get(2) : DataTypes.INTEGER;

                DataType implType;
                final boolean hasDoubleOrLong = dataTypes.contains(DataTypes.DOUBLE) || dataTypes.contains(DataTypes.LONG);
                if(DataTypes.NUMERIC_WITH_DECIMAL.contains(first) || DataTypes.NUMERIC_WITH_DECIMAL.contains(third))
                    implType = hasDoubleOrLong ? DataTypes.DOUBLE : DataTypes.FLOAT;
                else
                    implType = hasDoubleOrLong ? DataTypes.LONG : DataTypes.INTEGER;
                return impl.get(implType);
            }
        });
    }

    static abstract class GenerateSeriesFunctionImplementation<T extends Number> implements TableFunctionImplementation {

        private final FunctionInfo info;
        private final Map<ColumnIdent, Reference> columnMap;
        private final Collection<Reference> columns;

        protected abstract T plus(T v1, T v2);
        protected abstract T minus(T v1, T v2);
        protected abstract T divide(T v1, T v2);

        public GenerateSeriesFunctionImplementation(FunctionInfo info) {
            this.info = info;

            ColumnIdent columnIdent = new ColumnIdent(NAME);
            Reference reference = new Reference(new ReferenceIdent(TABLE_IDENT, columnIdent), RowGranularity.DOC, info.returnType());
            columnMap = Collections.singletonMap(columnIdent, reference);
            columns = Collections.singletonList(reference);
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        @Override
        public Symbol normalizeSymbol(Function function, @Nullable TransactionContext transactionContext) {
            for(int i = 0; i < function.arguments().size(); i++) {
                function.arguments().set(i, ExpressionAnalyzer.castIfNeededOrFail(function.arguments().get(i), info.returnType()));
            }
            return function;
        }

        @Override
        public Bucket execute(Collection<? extends Input> arguments) {

            DataType<T> type = info.returnType();
            Iterator<? extends Input> i = arguments.iterator();

            T inclusiveStart = type.value(i.next().value());
            T inclusiveStop = type.value(i.next().value());
            T step = type.value(i.hasNext() ? i.next().value() : 1);

            final int numRows = divide(minus(inclusiveStop, inclusiveStart), step).intValue();

            return new Bucket() {
                final Object[] cells = new Object[1];
                final RowN row = new RowN(cells);

                @Override
                public int size() {
                    return numRows+1;
                }

                @Override
                public Iterator<Row> iterator() {
                    return new Iterator<Row>() {
                        T current = inclusiveStart;
                        int currentIdx = 0;

                        @Override
                        public boolean hasNext() {
                            return currentIdx < size();
                        }

                        @Override
                        public Row next() {
                            if (!hasNext()) {
                                throw new NoSuchElementException("No more rows");
                            }
                            cells[0] = current;
                            current = plus(current, step);
                            currentIdx++;
                            return row;
                        }
                    };
                }
            };
        }

        @Override
        public TableInfo createTableInfo(ClusterService clusterService) {
            return new StaticTableInfo(TABLE_IDENT, columnMap, columns, Collections.emptyList()) {
                @Override
                public RowGranularity rowGranularity() {
                    return RowGranularity.DOC;
                }

                @Override
                public Routing getRouting(WhereClause whereClause, @Nullable String preference, SessionContext sessionContext) {
                    return Routing.forTableOnSingleNode(TABLE_IDENT, clusterService.localNode().getId());
                }
            };
        }
    }

    static class IntegerGenerateSeriesFunctionImplementation extends GenerateSeriesFunctionImplementation<Integer> {

        public IntegerGenerateSeriesFunctionImplementation(FunctionInfo info) {
            super(info);
        }

        @Override
        protected Integer plus(Integer v1, Integer v2) {
            return v1 + v2;
        }

        @Override
        protected Integer minus(Integer v1, Integer v2) {
            return v1 - v2;
        }

        @Override
        protected Integer divide(Integer v1, Integer v2) {
            return v1 / v2;
        }
    }

    static class LongGenerateSeriesFunctionImplementation extends GenerateSeriesFunctionImplementation<Long> {

        public LongGenerateSeriesFunctionImplementation(FunctionInfo info) {
            super(info);
        }

        @Override
        protected Long plus(Long v1, Long v2) {
            return v1 + v2;
        }

        @Override
        protected Long minus(Long v1, Long v2) {
            return v1 - v2;
        }

        @Override
        protected Long divide(Long v1, Long v2) {
            return v1 / v2;
        }
    }

    static class FloatGenerateSeriesFunctionImplementation extends GenerateSeriesFunctionImplementation<Float> {

        public FloatGenerateSeriesFunctionImplementation(FunctionInfo info) {
            super(info);
        }

        @Override
        protected Float plus(Float v1, Float v2) {
            return v1 + v2;
        }

        @Override
        protected Float minus(Float v1, Float v2) {
            return v1 - v2;
        }

        @Override
        protected Float divide(Float v1, Float v2) {
            return v1 / v2;
        }
    }

    static class DoubleGenerateSeriesFunctionImplementation extends GenerateSeriesFunctionImplementation<Double> {

        public DoubleGenerateSeriesFunctionImplementation(FunctionInfo info) {
            super(info);
        }

        @Override
        protected Double plus(Double v1, Double v2) {
            return v1 + v2;
        }

        @Override
        protected Double minus(Double v1, Double v2) {
            return v1 - v2;
        }

        @Override
        protected Double divide(Double v1, Double v2) {
            return v1 / v2;
        }
    }
}

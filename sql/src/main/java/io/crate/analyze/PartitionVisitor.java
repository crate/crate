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

package io.crate.analyze;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.crate.PartitionName;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.*;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nullable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PartitionVisitor extends SymbolVisitor<PartitionVisitor.Context, Symbol> {

    private final EvaluatingNormalizer normalizer;

    public PartitionVisitor(EvaluatingNormalizer normalizer) {
        this.normalizer = normalizer;
    }

    public Context process(WhereClause whereClause, TableInfo tableInfo) {
        Context ctx = new Context(tableInfo.partitionedByColumns(), whereClause);
        if (tableInfo.isPartitioned() && whereClause.hasQuery()) {
            List<String> partitions = new LinkedList<>();
            Symbol processed = process(whereClause.query(), ctx);
            Symbol normalized = null;
            Symbol lastQuery = null;
            for (PartitionName partitionName : tableInfo.partitions()) {
                List<BytesRef> values = partitionName.values();
                List<ReferenceInfo> partitionedByColumns = tableInfo.partitionedByColumns();
                for (int i = 0; i < partitionedByColumns.size(); i++) {
                    ReferenceInfo info = partitionedByColumns.get(i);
                    ReferencePlaceHolder partitionSymbol = ctx.partitionSymbol(info);
                    if (partitionSymbol != null) {
                        partitionSymbol.setValue(values.get(i));
                    }
                }
                normalized = this.normalizer.normalize(processed);

                if (normalized != null && !new WhereClause(normalized).noMatch()) {
                    // we have a query
                    if (lastQuery != null && !lastQuery.equals(normalized)) {
                        // TODO: remove this check if we are able to run normal search queries without ESSearch
                        // Just now, we would have to execute 2 separate ESSearch tasks and merge results
                        // which is not supported right now and maybe never will be
                        throw new UnsupportedFeatureException("Using a partitioned column and a " +
                                "normal column inside an OR clause is not supported");
                    } else {
                         partitions.add(partitionName.stringValue());
                    }
                    lastQuery = normalized;
                }
            }
            ctx.partitions = partitions;
            ctx.whereClause = new WhereClause(Objects.firstNonNull(lastQuery,
                    Objects.firstNonNull(normalized, Literal.NULL)));

        } else if (whereClause.noMatch()) {
            ctx.partitions = ImmutableList.of();
        } else {
            ctx.partitions = Lists.transform(tableInfo.partitions(), new com.google.common.base.Function<PartitionName, String>() {
                @Nullable
                @Override
                public String apply(@Nullable PartitionName input) {
                    return input == null ? null : input.stringValue();
                }
            });
        }
        return ctx;
    }

    public static class Context {
        private final Map<ReferenceInfo, ReferencePlaceHolder> partitionValues;

        private List<String> partitions = ImmutableList.of();

        private WhereClause whereClause;

        public Context(List<ReferenceInfo> partitionedByColumns, WhereClause whereClause) {
            ImmutableMap.Builder<ReferenceInfo, ReferencePlaceHolder> builder = ImmutableMap.builder();

            for (ReferenceInfo info : partitionedByColumns) {
                builder.put(info, new ReferencePlaceHolder(info));
            }
            this.partitionValues = builder.build();
            this.whereClause = whereClause;
        }

        private @Nullable ReferencePlaceHolder partitionSymbol(ReferenceInfo info) {
            return partitionValues.get(info);
        }

        public List<String> partitions() {
            return partitions;
        }

        public WhereClause whereClause() {
            return whereClause;
        }
    }

    @Override
    public Symbol visitFunction(Function symbol, Context context) {
        int i = 0;
        for (Symbol arg : symbol.arguments()) {
            Symbol newArg = arg.accept(this, context);
            if (!arg.equals(newArg)) {
                symbol.setArgument(i, newArg);
            }
            i++;
        }
        return symbol;
    }

    @Override
    public Symbol visitReference(Reference symbol, Context context) {
        ReferencePlaceHolder collectSymbol = context.partitionSymbol(symbol.info());
        return collectSymbol == null ? symbol : collectSymbol;
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, Context context) {
        return symbol;
    }
}

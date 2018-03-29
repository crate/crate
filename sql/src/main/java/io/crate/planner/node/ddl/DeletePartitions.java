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

package io.crate.planner.node.ddl;

import com.google.common.annotations.VisibleForTesting;
import io.crate.analyze.SymbolEvaluator;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.collections.Lists2;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.metadata.Functions;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.IndicesOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class DeletePartitions implements Plan {

    private final RelationName relationName;
    private final List<List<Symbol>> partitions;

    public DeletePartitions(RelationName relationName, List<List<Symbol>> partitions) {
        this.relationName = relationName;
        this.partitions = partitions;
    }

    public List<List<Symbol>> partitions() {
        return partitions;
    }

    @Override
    public void execute(DependencyCarrier executor,
                        PlannerContext plannerContext,
                        RowConsumer consumer,
                        Row params,
                        Map<SelectSymbol, Object> valuesBySubQuery) {

        ArrayList<String> indexNames = getIndices(executor.functions(), params, valuesBySubQuery);
        DeleteIndexRequest request = new DeleteIndexRequest(indexNames.toArray(new String[0]));
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        executor.transportActionProvider().transportDeleteIndexAction()
            .execute(request, new OneRowActionListener<>(consumer, r -> Row1.ROW_COUNT_UNKNOWN));
    }

    @VisibleForTesting
    ArrayList<String> getIndices(Functions functions, Row parameters, Map<SelectSymbol, Object> subQueryValues) {
        ArrayList<String> indexNames = new ArrayList<>();
        for (List<Symbol> partitionValues : partitions) {
            Function<Symbol, BytesRef> symbolToBytesRef =
                s -> DataTypes.STRING.value(SymbolEvaluator.evaluate(functions, s, parameters, subQueryValues));
            List<BytesRef> values = Lists2.copyAndReplace(partitionValues, symbolToBytesRef);
            String indexName = IndexParts.toIndexName(relationName, PartitionName.encodeIdent(values));
            indexNames.add(indexName);
        }
        return indexNames;
    }
}

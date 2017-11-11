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

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.annotations.VisibleForTesting;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.symbol.Symbol;
import io.crate.collections.Lists2;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.executor.JobTask;
import io.crate.executor.transport.OneRowActionListener;
import io.crate.metadata.Functions;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import io.crate.planner.node.ddl.DeletePartitions;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.support.IndicesOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class DeletePartitionTask extends JobTask {

    static final Function<Object, Row> TO_UNKNOWN_COUNT_ROW = o -> new Row1(-1L);

    private final Functions functions;
    private final TransportDeleteIndexAction transport;
    private final List<List<Symbol>> partitions;
    private final TableIdent table;

    public DeletePartitionTask(DeletePartitions deletePartitions,
                               Functions functions,
                               TransportDeleteIndexAction transport) {
        super(deletePartitions.jobId());
        this.functions = functions;
        this.transport = transport;
        this.table = deletePartitions.tableIdent();
        this.partitions = deletePartitions.partitions();
    }

    @Override
    public void execute(RowConsumer consumer, Row parameters) {
        OneRowActionListener<DeleteIndexResponse> actionListener = new OneRowActionListener<>(consumer, TO_UNKNOWN_COUNT_ROW);
        ArrayList<String> indexNames = getIndices(parameters);
        DeleteIndexRequest request = new DeleteIndexRequest(indexNames.toArray(new String[0]));
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        transport.execute(request, actionListener);
    }

    @VisibleForTesting
    ArrayList<String> getIndices(Row parameters) {
        ArrayList<String> indexNames = new ArrayList<>();
        for (List<Symbol> partitionValues : partitions) {
            Function<Symbol, BytesRef> symbolToBytesRef =
                s -> DataTypes.STRING.value(SymbolEvaluator.evaluate(functions, s, parameters));
            List<BytesRef> values = Lists2.copyAndReplace(partitionValues, symbolToBytesRef);
            String indexName = IndexParts.toIndexName(table, PartitionName.encodeIdent(values));
            indexNames.add(indexName);
        }
        return indexNames;
    }
}

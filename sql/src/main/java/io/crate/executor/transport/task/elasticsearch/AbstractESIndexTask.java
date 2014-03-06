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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.Task;
import io.crate.operator.Input;
import io.crate.planner.node.dml.ESIndexNode;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.util.BytesRef;
import io.crate.Constants;
import io.crate.exceptions.CrateException;
import org.elasticsearch.action.index.IndexRequest;

import java.util.*;

public abstract class AbstractESIndexTask implements Task<Object[][]> {

    protected final SettableFuture<Object[][]> result;
    protected final List<ListenableFuture<Object[][]>> results;
    protected final ESIndexNode node;

    public AbstractESIndexTask(ESIndexNode node) {
        this.node = node;

        result = SettableFuture.create();
        results = Arrays.<ListenableFuture<Object[][]>>asList(result);
    }

    @Override
    public List<ListenableFuture<Object[][]>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<Object[][]>> result) {
        throw new UnsupportedOperationException();
    }

    protected IndexRequest buildIndexRequest(String index,
                                             List<Reference> columns,
                                             List<Symbol> values,
                                             int primaryKeyIdx) {
        IndexRequest request = new IndexRequest(index, Constants.DEFAULT_MAPPING_TYPE);
        request.create(true);

        Map<String, Object> sourceMap = new HashMap<>();
        Iterator<Symbol> valuesIt = values.iterator();

        //int primaryKeyIdx = -1;
        //if (node.hasPrimaryKey()) {
        //    primaryKeyIdx = node.primaryKeyIndices()[0];
        //}
        for (int i = 0, length=columns.size(); i<length; i++) {
            Reference column = columns.get(i);
            Symbol v = valuesIt.next();
            assert v instanceof Literal;
            try {
                Object value = ((Input)v).value();
                if (value instanceof BytesRef) {
                    value = ((BytesRef) value).utf8ToString();
                }
                sourceMap.put(
                        column.info().ident().columnIdent().name(),
                        value
                );
                if (i == primaryKeyIdx) {
                    request.id(value.toString());
                }
            } catch (ClassCastException e) {
                // symbol is no input
                throw new CrateException(String.format("invalid value '%s' in insert statement", v.toString()));
            }
        }
        request.source(sourceMap);
        return request;
    }
}

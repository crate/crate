/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operator.Input;
import io.crate.operator.reference.sys.shard.ShardTableNameExpression;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.util.BytesRef;
import org.cratedb.Constants;
import org.cratedb.DataType;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.common.inject.Inject;

public class DeleteFunction implements Scalar<Boolean> {

    public static final String NAME = "delete";

    public static final FunctionInfo INFO = new FunctionInfo(
            new FunctionIdent(NAME, ImmutableList.of(DataType.STRING, DataType.STRING)), DataType.BOOLEAN);

    private final TransportDeleteAction deleteAction;

    @Inject
    public DeleteFunction(TransportDeleteAction deleteAction) {
        this.deleteAction = deleteAction;
    }

    @Override
    public FunctionInfo info() {
        return INFO;
    }

    @Override
    public Boolean evaluate(Input<?>... args) {
        ShardTableNameExpression indexName = (ShardTableNameExpression)args[0];
        String id = ((BytesRef)args[1].value()).utf8ToString();
        Long version = (Long)args[2].value();

        DeleteRequest request = new DeleteRequest(indexName.value().utf8ToString(), Constants.DEFAULT_MAPPING_TYPE, id);
        request.version(version);
        DeleteResponse deleteResponse = deleteAction.execute(request).actionGet();
        return !deleteResponse.isNotFound();
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        return symbol;
    }
}

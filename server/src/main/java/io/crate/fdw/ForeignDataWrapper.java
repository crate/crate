/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.fdw;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.common.settings.Setting;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.expression.symbol.Symbol;
import io.crate.fdw.ServersMetadata.Server;
import io.crate.metadata.TransactionContext;
import io.crate.role.Role;

public interface ForeignDataWrapper {

    default List<Setting<?>> mandatoryServerOptions() {
        return List.of();
    }

    default List<Setting<?>> optionalTableOptions() {
        return List.of();
    }

    default List<Setting<?>> optionalUserOptions() {
        return List.of();
    }

    /**
     * Indicates if the query can be executed on the foreign server.
     *
     * If this returns `false` filtering must be done via dedicated filter operator
     * because the query parameter to
     * {@link #getIterator(Role, Server, ForeignTable, TransactionContext, List, Symbol)}
     * is ignored.
     **/
    boolean supportsQueryPushdown(Symbol query);

    CompletableFuture<BatchIterator<Row>> getIterator(Role user,
                                                      Server server,
                                                      ForeignTable foreignTable,
                                                      TransactionContext txnCtx,
                                                      List<Symbol> collect,
                                                      Symbol query);
}

// /*
//  * Licensed to Crate.io GmbH ("Crate") under one or more contributor
//  * license agreements.  See the NOTICE file distributed with this work for
//  * additional information regarding copyright ownership.  Crate licenses
//  * this file to you under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.  You may
//  * obtain a copy of the License at
//  *
//  *   http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//  * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
//  * License for the specific language governing permissions and limitations
//  * under the License.
//  *
//  * However, if you have executed another commercial license agreement
//  * with Crate these terms will supersede the license and you may use the
//  * software solely pursuant to the terms of the relevant commercial agreement.
//  */
//
// package io.crate.fdw;
//
// import io.crate.role.Role;
// import org.elasticsearch.cluster.metadata.RelationMetadata;
// import io.crate.data.BatchIterator;
// import io.crate.data.Row;
// import java.util.concurrent.CompletableFuture;
// import java.util.List;
// import io.crate.fdw.ServersMetadata.Server;
// import io.crate.metadata.TransactionContext;
//
// import io.crate.expression.symbol.Symbol;
//
// final class ParquetForeignDataWrapper implements ForeignDataWrapper {
//
//     @Override
//     public boolean supportsQueryPushdown(Symbol query) {
//         return true;
//     }
//
//     // @Override
//     // public CompletableFuture<BatchIterator<Row>> getIterator(Role currentUser,
//     //         Server server,
//     //         RelationMetadata.ForeignTable foreignTable,
//     //         TransactionContext txnCtx,
//     //         List<Symbol> collect,
//     //         Symbol query) {
//     //
//     //     // send the query to the ParquetBatchIterator
//     //     BatchIterator<Row> it = new ParquetBatchIterator();
//     //
//     //     // `collect`: projection pushdown
//     //     // `filter`: predicate pushdown
//     //     // `limit`: push down a limit to get the first n matching records
//     //
//     // }
// }

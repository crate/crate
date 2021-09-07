/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

/**
 * <p>
 *     The logical replication package contains all code related for replicating tables "logically" from a different
 *     (remote) cluster. It follow closely the logical replication specification of PostgreSQL in such a way that only
 *     tables exposed via publication's can be replicated by subscribing to a publication.
 * </p>
 *
 * <p>
 *     Internally, logical replication execution can be split into following main topics:
 * </p>
 * <ul>
 *     <li>Connection management to the remote cluster, handled on a high level by
 *     {@link io.crate.replication.logical.LogicalReplicationService}</li>
 *     <li>Requesting current available tables of a publication
 *     {@link io.crate.replication.logical.action.PublicationsStateAction}</li>
 *     <li>Restore all tables exposed by the publication including the current data
 *     {@link io.crate.replication.logical.repository}</li>
 *     <li>Track data, mapping and setting changes on the remote cluster and apply them locally
 *     {@link io.crate.replication.logical.ShardReplicationChangesTracker}</li>
 * </ul>
 */

package io.crate.replication.logical;

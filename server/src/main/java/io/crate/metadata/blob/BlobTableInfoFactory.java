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

package io.crate.metadata.blob;

import io.crate.metadata.RelationName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.ImplementedBy;

/**
 * Similar to {@link io.crate.metadata.doc.DocTableInfoFactory} this is a factory to create BlobTableInfos'
 *
 * The reason there is no shared interface with generics is that guice cannot bind different implementations based
 * on the generic
 */
@ImplementedBy(InternalBlobTableInfoFactory.class)
public interface BlobTableInfoFactory {

    BlobTableInfo create(RelationName ident, ClusterState clusterState);
}

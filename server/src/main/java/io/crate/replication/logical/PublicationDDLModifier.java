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

package io.crate.replication.logical;

import java.util.ArrayList;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;

import io.crate.metadata.RelationName;
import io.crate.metadata.cluster.DDLClusterStateModifier;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.PublicationsMetadata;

public class PublicationDDLModifier implements DDLClusterStateModifier {

    @Override
    public ClusterState onDropTable(ClusterState currentState, RelationName relationName) {
        PublicationsMetadata publications = currentState.metadata().custom(PublicationsMetadata.TYPE);
        if (publications == null) {
            return currentState;
        }
        PublicationsMetadata updated = PublicationsMetadata.newInstance(publications);
        boolean isUpdated = false;
        for (var entry : publications.publications().entrySet()) {
            Publication publication = entry.getValue();
            if (publication.isForAllTables() || publication.tables().contains(relationName) == false) {
                continue;
            }
            var tables = new ArrayList<>(publication.tables());
            tables.remove(relationName);
            updated.publications().put(entry.getKey(), new Publication(publication.owner(), false, tables));
            isUpdated = true;
        }
        if (isUpdated == false) {
            return currentState;
        }
        return ClusterState.builder(currentState)
            .metadata(Metadata.builder(currentState.metadata()).putCustom(PublicationsMetadata.TYPE, updated))
            .build();
    }
}

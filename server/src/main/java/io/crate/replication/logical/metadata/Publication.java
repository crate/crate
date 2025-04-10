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

package io.crate.replication.logical.metadata;

import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_INDEX_ROUTING_ACTIVE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

import io.crate.metadata.RelationName;
import io.crate.role.Permission;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.role.Securable;

public class Publication implements Writeable {

    private static final Logger LOGGER = LogManager.getLogger(Publication.class);
    private final String owner;
    private final boolean forAllTables;
    private final List<RelationName> tables;

    public Publication(String owner, boolean forAllTables, List<RelationName> tables) {
        assert !forAllTables || (forAllTables && tables.isEmpty()) : "If forAllTables is true, tables must be empty";
        this.owner = owner;
        this.forAllTables = forAllTables;
        this.tables = tables;
    }

    Publication(StreamInput in) throws IOException {
        owner = in.readString();
        forAllTables = in.readBoolean();
        int size = in.readVInt();
        tables = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            tables.add(RelationName.fromIndexName(in.readString()));
        }
    }

    public String owner() {
        return owner;
    }

    public boolean isForAllTables() {
        return forAllTables;
    }

    public List<RelationName> tables() {
        return tables;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(owner);
        out.writeBoolean(forAllTables);
        out.writeVInt(tables.size());
        for (var table : tables) {
            out.writeString(table.indexNameOrAlias());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Publication that = (Publication) o;
        return forAllTables == that.forAllTables && owner.equals(that.owner) && tables.equals(that.tables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(owner, tables, forAllTables);
    }

    @Override
    public String toString() {
        return "Publication{forAllTables=" + forAllTables + ", owner=" + owner + ", tables=" + tables + "}";
    }

    public Metadata.Builder resolveCurrentRelations(ClusterState state,
                                                    Roles roles,
                                                    Role publicationOwner,
                                                    Role subscriber,
                                                    String publicationName,
                                                    Metadata.Builder metadataBuilder) {
        Metadata metadata = state.metadata();
        Predicate<RelationName> relationFilter = relationName -> {
            if (!userCanPublish(roles, relationName, publicationOwner, publicationName)) {
                return false;
            }
            return subscriberCanRead(roles, relationName, subscriber, publicationName);
        };
        // skip indices where not all shards are active yet, restore will fail if primaries are not (yet) assigned
        Predicate<Index> indexFilter = index -> {
            var indexMetadata = metadata.index(index);
            if (indexMetadata != null) {
                var routingTable = state.routingTable().index(index);
                assert routingTable != null : "routingTable must not be null";
                return routingTable.allPrimaryShardsActive();

            }
            return false;
        };

        if (isForAllTables()) {
            for (var table : metadata.relations(org.elasticsearch.cluster.metadata.RelationMetadata.Table.class)) {
                if (relationFilter.test(table.name()) == false) {
                    continue;
                }
                addRelation(metadata, metadataBuilder, table, indexFilter);
            }
        } else {
            for (RelationName relationName : tables) {
                if (relationFilter.test(relationName) == false) {
                    continue;
                }
                org.elasticsearch.cluster.metadata.RelationMetadata.Table table = metadata.getRelation(relationName);
                if (table == null) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Table {} not found in metadata, skipping publication resolution for it.", relationName);
                    }
                    continue;
                }
                addRelation(metadata, metadataBuilder, table, indexFilter);
            }
        }

        return metadataBuilder;
    }

    private static void addRelation(Metadata currentMetadata,
                                    Metadata.Builder metadataBuilder,
                                    org.elasticsearch.cluster.metadata.RelationMetadata.Table table,
                                    Predicate<Index> indexFilter) {
        metadataBuilder.setRelation(table);
        for (IndexMetadata indexMetadata : currentMetadata.getIndices(table.name(), List.of(), true, im -> im)) {
            var publishedIndexMetadata = IndexMetadata.builder(indexMetadata);
            if (indexFilter.test(indexMetadata.getIndex()) == false) {
                publishedIndexMetadata.settings(
                    Settings.builder()
                        .put(indexMetadata.getSettings())
                        .put(REPLICATION_INDEX_ROUTING_ACTIVE.getKey(), false)
                );
            }
            metadataBuilder.put(publishedIndexMetadata);
        }
    }

    private static boolean subscriberCanRead(Roles roles, RelationName relationName, Role subscriber, String publicationName) {
        boolean canRead = roles.hasPrivilege(subscriber, Permission.DQL, Securable.TABLE, relationName.fqn());
        if (canRead == false) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("User {} subscribed to the publication {} doesn't have DQL privilege on the table {}, this table will not be replicated.",
                    subscriber.name(), publicationName, relationName.fqn());
            }
        }
        return canRead;
    }

    private static boolean userCanPublish(Roles roles, RelationName relationName, Role publicationOwner, String publicationName) {
        for (Permission permission : Permission.READ_WRITE_DEFINE) {
            // This check is triggered only on ALL TABLES case.
            // Required privileges correspond to those we check for the pre-defined tables case in AccessControlImpl.visitCreatePublication.

            // Schemas.DOC_SCHEMA_NAME is a dummy parameter since we are passing fqn as ident.
            if (!roles.hasPrivilege(publicationOwner, permission, Securable.TABLE, relationName.fqn())) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("User {} owning publication {} doesn't have {} privilege on the table {}, this table will not be replicated.",
                        publicationOwner.name(), publicationName, permission.name(), relationName.fqn());
                }
                return false;
            }
        }
        return true;
    }
}

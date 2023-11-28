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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexSettings;

import io.crate.metadata.IndexParts;
import io.crate.metadata.RelationName;
import io.crate.user.Privilege;
import io.crate.user.User;

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


    public Map<RelationName, RelationMetadata> resolveCurrentRelations(ClusterState state, User publicationOwner, User subscriber, String publicationName) {
        if (isForAllTables()) {
            Map<RelationName, RelationMetadata> relations = new HashMap<>();
            Metadata metadata = state.metadata();
            for (var cursor : metadata.templates().keys()) {
                String templateName = cursor.value;
                IndexParts indexParts = new IndexParts(templateName);
                RelationName relationName = indexParts.toRelationName();
                if (indexParts.isPartitioned()
                        && userCanPublish(relationName, publicationOwner, publicationName)
                        && subscriberCanRead(relationName, subscriber, publicationName)) {
                    relations.put(relationName, RelationMetadata.fromMetadata(relationName, metadata));
                }
            }
            for (var cursor : metadata.indices().values()) {
                var indexMetadata = cursor.value;
                var indexParts = new IndexParts(indexMetadata.getIndex().getName());
                if (indexParts.isPartitioned()) {
                    continue;
                }
                RelationName relationName = indexParts.toRelationName();
                boolean softDeletes = IndexSettings.INDEX_SOFT_DELETES_SETTING.get(indexMetadata.getSettings());
                if (softDeletes == false) {
                    LOGGER.warn(
                        "Table '{}' won't be replicated as the required table setting " +
                            "'soft_deletes.enabled' is set to: {}",
                        relationName,
                        softDeletes
                    );
                    continue;
                }
                if (userCanPublish(relationName, publicationOwner, publicationName) && subscriberCanRead(relationName, subscriber, publicationName)) {
                    relations.put(relationName, RelationMetadata.fromMetadata(relationName, metadata));
                }
            }
            return relations;
        } else {
            return tables.stream()
                .filter(relationName -> userCanPublish(relationName, publicationOwner, publicationName))
                .filter(relationName -> subscriberCanRead(relationName, subscriber, publicationName))
                .map(relationName -> RelationMetadata.fromMetadata(relationName, state.metadata()))
                .collect(Collectors.toMap(x -> x.name(), x -> x));
        }
    }

    private static boolean subscriberCanRead(RelationName relationName, User subscriber, String publicationName) {
        boolean canRead = subscriber.hasPrivilege(Privilege.Type.DQL, Privilege.Clazz.TABLE, relationName.fqn());
        if (canRead == false) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("User {} subscribed to the publication {} doesn't have DQL privilege on the table {}, this table will not be replicated.",
                    subscriber.name(), publicationName, relationName.fqn());
            }
        }
        return canRead;
    }

    private static boolean userCanPublish(RelationName relationName, User publicationOwner, String publicationName) {
        for (Privilege.Type type: Privilege.Type.READ_WRITE_DEFINE) {
            // This check is triggered only on ALL TABLES case.
            // Required privileges correspond to those we check for the pre-defined tables case in AccessControlImpl.visitCreatePublication.

            // Schemas.DOC_SCHEMA_NAME is a dummy parameter since we are passing fqn as ident.
            if (!publicationOwner.hasPrivilege(type, Privilege.Clazz.TABLE, relationName.fqn())) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("User {} owning publication {} doesn't have {} privilege on the table {}, this table will not be replicated.",
                        publicationOwner.name(), publicationName, type.name(), relationName.fqn());
                }
                return false;
            }
        }
        return true;
    }
}

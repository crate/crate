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

package io.crate.metadata.doc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;

import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.IndexParts;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.replication.logical.metadata.PublicationsMetadata;

@Singleton
public class DocTableInfoFactory {

    private static final Logger LOGGER = LogManager.getLogger(DocTableInfoFactory.class);

    private final NodeContext nodeCtx;

    @Inject
    public DocTableInfoFactory(NodeContext nodeCtx) {
        this.nodeCtx = nodeCtx;
    }

    public DocTableInfo create(RelationName relation, ClusterState state) {
        var metadata = state.metadata();
        PublicationsMetadata publicationsMetadata = metadata.custom(PublicationsMetadata.TYPE);

        DocIndexMetadata docIndexMetadata;
        String[] concreteIndices;
        String[] concreteOpenIndices;
        String templateName = PartitionName.templateName(relation.schema(), relation.name());
        if (metadata.templates().containsKey(templateName)) {
            docIndexMetadata = buildDocIndexMetadataFromTemplate(
                metadata,
                publicationsMetadata,
                relation,
                templateName
            );
            // We need all concrete indices, regardless of their state, for operations such as reopening.
            concreteIndices = IndexNameExpressionResolver.concreteIndexNames(
                metadata,
                IndicesOptions.lenientExpandOpen(),
                relation.indexNameOrAlias()
            );
            // We need all concrete open indices, as closed indices must not appear in the routing.
            concreteOpenIndices = IndexNameExpressionResolver.concreteIndexNames(
                metadata,
                IndicesOptions.fromOptions(true, true, true, false, IndicesOptions.strictExpandOpenAndForbidClosed()),
                relation.indexNameOrAlias()
            );
        } else {
            try {
                concreteIndices = IndexNameExpressionResolver.concreteIndexNames(
                    metadata, IndicesOptions.strictExpandOpen(), relation.indexNameOrAlias());
                concreteOpenIndices = concreteIndices;
                if (concreteIndices.length == 0) {
                    // no matching index found
                    throw new RelationUnknown(relation);
                }
                docIndexMetadata = buildDocIndexMetadata(metadata, publicationsMetadata, relation, concreteIndices[0]);
            } catch (IndexNotFoundException ex) {
                throw new RelationUnknown(relation.fqn(), ex);
            }
        }
        List<PartitionName> partitions = buildPartitions(
            relation,
            concreteIndices,
            docIndexMetadata
        );
        return new DocTableInfo(
            relation,
            docIndexMetadata.references(),
            docIndexMetadata.notNullColumns(),
            docIndexMetadata.indices(),
            docIndexMetadata.analyzers(),
            docIndexMetadata.pkConstraintName(),
            docIndexMetadata.primaryKey(),
            docIndexMetadata.checkConstraints(),
            docIndexMetadata.routingCol(),
            docIndexMetadata.hasAutoGeneratedPrimaryKey(),
            concreteIndices,
            concreteOpenIndices,
            docIndexMetadata.numberOfShards(), docIndexMetadata.numberOfReplicas(),
            docIndexMetadata.tableParameters(),
            docIndexMetadata.partitionedBy(),
            partitions,
            docIndexMetadata.columnPolicy(),
            docIndexMetadata.versionCreated(),
            docIndexMetadata.versionUpgraded(),
            docIndexMetadata.isClosed(),
            docIndexMetadata.supportedOperations());
    }

    private DocIndexMetadata buildDocIndexMetadata(Metadata metadata,
                                                   PublicationsMetadata publicationsMetadata,
                                                   RelationName relation,
                                                   String concreteIndex) {
        DocIndexMetadata docIndexMetadata;
        IndexMetadata indexMetadata = metadata.index(concreteIndex);
        try {
            docIndexMetadata = new DocIndexMetadata(
                nodeCtx,
                indexMetadata,
                relation,
                publicationsMetadata);
        } catch (Exception e) {
            throw new UnhandledServerException("Unable to build DocIndexMetadata", e);
        }
        try {
            return docIndexMetadata.build();
        } catch (Exception e) {
            try {
                LOGGER.error(
                    "Could not build DocIndexMetadata from: {}", indexMetadata.mapping().sourceAsMap());
            } catch (Exception ignored) {
            }
            throw e;
        }
    }

    private DocIndexMetadata buildDocIndexMetadataFromTemplate(Metadata metadata,
                                                               PublicationsMetadata publicationsMetadata,
                                                               RelationName relation,
                                                               String templateName) {
        IndexTemplateMetadata indexTemplateMetadata = metadata.templates().get(templateName);
        DocIndexMetadata docIndexMetadata;
        try {
            IndexMetadata.Builder builder = new IndexMetadata.Builder(relation.indexNameOrAlias());
            builder.putMapping(indexTemplateMetadata.mapping().toString());

            Settings.Builder settingsBuilder = Settings.builder()
                .put(indexTemplateMetadata.settings());
            if (indexTemplateMetadata.settings().get(IndexMetadata.SETTING_VERSION_CREATED) == null) {
                settingsBuilder.put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);
            }

            Settings settings = settingsBuilder.build();
            builder.settings(settings);
            builder.numberOfShards(settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5));
            builder.numberOfReplicas(settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
            docIndexMetadata = new DocIndexMetadata(nodeCtx, builder.build(), relation, publicationsMetadata);
        } catch (IOException e) {
            throw new UnhandledServerException("Unable to build DocIndexMetadata from template", e);
        }
        return docIndexMetadata.build();
    }

    private static List<PartitionName> buildPartitions(RelationName relation,
                                                       String[] concreteIndices,
                                                       DocIndexMetadata md) {
        if (md.partitionedBy().isEmpty()) {
            return List.of();
        }
        List<PartitionName> partitions = new ArrayList<>();
        for (String indexName : concreteIndices) {
            if (IndexParts.isPartitioned(indexName)) {
                try {
                    PartitionName partitionName = PartitionName.fromIndexOrTemplate(indexName);
                    assert partitionName.relationName().equals(relation) : "ident must equal partitionName";
                    partitions.add(partitionName);
                } catch (IllegalArgumentException e) {
                    // ignore
                    LOGGER.warn(String.format(Locale.ENGLISH, "Cannot build partition %s of index %s", indexName, relation.indexNameOrAlias()));
                }
            }
        }
        return partitions;
    }
}

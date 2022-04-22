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

import io.crate.Constants;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.IndexParts;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

public class DocTableInfoBuilder {

    private final RelationName ident;
    private final NodeContext nodeCtx;
    private final Metadata metadata;
    private String[] concreteIndices;
    private String[] concreteOpenIndices;
    private static final Logger LOGGER = LogManager.getLogger(DocTableInfoBuilder.class);

    public DocTableInfoBuilder(NodeContext nodeCtx,
                               RelationName ident,
                               ClusterState state) {
        this.nodeCtx = nodeCtx;
        this.ident = ident;
        this.metadata = state.metadata();
    }

    private DocIndexMetadata docIndexMetadata() {
        DocIndexMetadata docIndexMetadata;
        String templateName = PartitionName.templateName(ident.schema(), ident.name());
        if (metadata.getTemplates().containsKey(templateName)) {
            docIndexMetadata = buildDocIndexMetadataFromTemplate(ident.indexNameOrAlias(), templateName);
            // We need all concrete indices, regardless of their state, for operations such as reopening.
            concreteIndices = IndexNameExpressionResolver.concreteIndexNames(
                metadata,
                IndicesOptions.lenientExpandOpen(),
                ident.indexNameOrAlias()
            );
            // We need all concrete open indices, as closed indices must not appear in the routing.
            concreteOpenIndices = IndexNameExpressionResolver.concreteIndexNames(
                metadata,
                IndicesOptions.fromOptions(true, true, true, false, IndicesOptions.strictExpandOpenAndForbidClosed()),
                ident.indexNameOrAlias()
            );
        } else {
            try {
                concreteIndices = IndexNameExpressionResolver.concreteIndexNames(
                    metadata, IndicesOptions.strictExpandOpen(), ident.indexNameOrAlias());
                concreteOpenIndices = concreteIndices;
                if (concreteIndices.length == 0) {
                    // no matching index found
                    throw new RelationUnknown(ident);
                }
                docIndexMetadata = buildDocIndexMetadata(concreteIndices[0]);
            } catch (IndexNotFoundException ex) {
                throw new RelationUnknown(ident.fqn(), ex);
            }
        }
        return docIndexMetadata;
    }

    private DocIndexMetadata buildDocIndexMetadata(String indexName) {
        DocIndexMetadata docIndexMetadata;
        IndexMetadata indexMetadata = metadata.index(indexName);
        try {
            docIndexMetadata = new DocIndexMetadata(nodeCtx, indexMetadata, ident);
        } catch (IOException e) {
            throw new UnhandledServerException("Unable to build DocIndexMetadata", e);
        }
        try {
            return docIndexMetadata.build();
        } catch (Exception e) {
            try {
                LOGGER.error(
                    "Could not build DocIndexMetadata from: {}", indexMetadata.mapping().getSourceAsMap());
            } catch (Exception ignored) {
            }
            throw e;
        }
    }

    private DocIndexMetadata buildDocIndexMetadataFromTemplate(String index, String templateName) {
        IndexTemplateMetadata indexTemplateMetadata = metadata.getTemplates().get(templateName);
        DocIndexMetadata docIndexMetadata;
        try {
            IndexMetadata.Builder builder = new IndexMetadata.Builder(index);
            builder.putMapping(Constants.DEFAULT_MAPPING_TYPE,
                indexTemplateMetadata.getMappings().get(Constants.DEFAULT_MAPPING_TYPE).toString());

            Settings.Builder settingsBuilder = Settings.builder()
                .put(indexTemplateMetadata.settings())
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);

            Settings settings = settingsBuilder.build();
            builder.settings(settings);
            builder.numberOfShards(settings.getAsInt(SETTING_NUMBER_OF_SHARDS, 5));
            builder.numberOfReplicas(settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 1));
            docIndexMetadata = new DocIndexMetadata(nodeCtx, builder.build(), ident);
        } catch (IOException e) {
            throw new UnhandledServerException("Unable to build DocIndexMetadata from template", e);
        }
        return docIndexMetadata.build();
    }

    private List<PartitionName> buildPartitions(DocIndexMetadata md) {
        List<PartitionName> partitions = new ArrayList<>();
        if (md.partitionedBy().size() > 0) {
            for (String indexName : concreteIndices) {
                if (IndexParts.isPartitioned(indexName)) {
                    try {
                        PartitionName partitionName = PartitionName.fromIndexOrTemplate(indexName);
                        assert partitionName.relationName().equals(ident) : "ident must equal partitionName";
                        partitions.add(partitionName);
                    } catch (IllegalArgumentException e) {
                        // ignore
                        LOGGER.warn(String.format(Locale.ENGLISH, "Cannot build partition %s of index %s", indexName, ident.indexNameOrAlias()));
                    }
                }
            }
        }
        return partitions;
    }

    public DocTableInfo build() {
        DocIndexMetadata md = docIndexMetadata();
        List<PartitionName> partitions = buildPartitions(md);

        return new DocTableInfo(
            ident,
            md.columns(),
            md.partitionedByColumns(),
            md.generatedColumnReferences(),
            md.notNullColumns(),
            md.indices(),
            md.references(),
            md.analyzers(),
            md.primaryKey(),
            md.checkConstraints(),
            md.routingCol(),
            md.hasAutoGeneratedPrimaryKey(),
            concreteIndices,
            concreteOpenIndices,
            md.numberOfShards(), md.numberOfReplicas(),
            md.tableParameters(),
            md.partitionedBy(),
            partitions,
            md.columnPolicy(),
            md.versionCreated(),
            md.versionUpgraded(),
            md.isClosed(),
            md.supportedOperations());
    }
}

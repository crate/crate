/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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
import io.crate.PartitionName;
import io.crate.exceptions.CrateException;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.TableIdent;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexMissingException;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;

public class DocTableInfoBuilder {

    private final TableIdent ident;
    private final MetaData metaData;
    private final boolean checkAliasSchema;
    private final ClusterService clusterService;
    private String[] concreteIndices;

    public DocTableInfoBuilder(TableIdent ident, ClusterService clusterService, boolean checkAliasSchema) {
        this.clusterService = clusterService;
        this.metaData = clusterService.state().metaData();
        this.ident = ident;
        this.checkAliasSchema = checkAliasSchema;
    }

    public DocIndexMetaData docIndexMetaData() {
        DocIndexMetaData docIndexMetaData;
        try {
            concreteIndices = metaData.concreteIndices(new String[]{ident.name()}, IndicesOptions.strict());
            docIndexMetaData = buildDocIndexMetaData(concreteIndices[0]);
        } catch (IndexMissingException ex) {
            String templateName = PartitionName.templateName(ident.name());
            if (!metaData.getTemplates().containsKey(templateName)) {
                throw new TableUnknownException(ident.name(), ex);
            }
            docIndexMetaData = buildDocIndexMetaDataFromTemplate(ident.name(), templateName);
            concreteIndices = new String[]{ident.name()};
        }
        if (concreteIndices.length == 1 || !checkAliasSchema) {
            return docIndexMetaData;
        }
        for (int i = 1; i < concreteIndices.length; i++) {
            docIndexMetaData = docIndexMetaData.merge(buildDocIndexMetaData(concreteIndices[i]));
        }
        return docIndexMetaData;
    }

    private DocIndexMetaData buildDocIndexMetaData(String index) {
        DocIndexMetaData docIndexMetaData;
        try {
            docIndexMetaData = new DocIndexMetaData(metaData.index(index), ident);
        } catch (IOException e) {
            throw new CrateException("Unable to build DocIndexMetaData", e);
        }
        return docIndexMetaData.build();
    }

    private DocIndexMetaData buildDocIndexMetaDataFromTemplate(String index, String templateName) {
        IndexTemplateMetaData indexTemplateMetaData = metaData.getTemplates().get(templateName);
        DocIndexMetaData docIndexMetaData;
        try {
            IndexMetaData.Builder builder = new IndexMetaData.Builder(index);
            builder.putMapping(Constants.DEFAULT_MAPPING_TYPE,
                    indexTemplateMetaData.getMappings().get(Constants.DEFAULT_MAPPING_TYPE).toString());
            Settings settings = indexTemplateMetaData.settings();
            builder.settings(settings);
            // default values
            builder.numberOfShards(settings.getAsInt(SETTING_NUMBER_OF_SHARDS, 5));
            builder.numberOfReplicas(settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 1));
            docIndexMetaData = new DocIndexMetaData(builder.build(), ident);
        } catch (IOException e) {
            throw new CrateException("Unable to build DocIndexMetaData from template", e);
        }
        return docIndexMetaData.build();
    }

    public DocTableInfo build() {
        DocIndexMetaData md = docIndexMetaData();
        return new DocTableInfo(ident, md.columns(), md.references(), md.primaryKey(), md.routingCol(),
                md.isAlias(), md.hasAutoGeneratedPrimaryKey(),
                concreteIndices, clusterService, md.numberOfShards(), md.numberOfReplicas());
    }

}

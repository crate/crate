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

import io.crate.metadata.TableIdent;
import org.cratedb.sql.CrateException;
import org.cratedb.sql.TableUnknownException;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.indices.IndexMissingException;

import java.io.IOException;

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
            concreteIndices = metaData.concreteIndices(new String[]{ident.name()}, IgnoreIndices.NONE, true);
        } catch (IndexMissingException ex) {
            throw new TableUnknownException(ident.name(), ex);
        }
        docIndexMetaData = buildDocIndexMetaData(concreteIndices[0]);
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

    public DocTableInfo build() {
        DocIndexMetaData md = docIndexMetaData();
        return new DocTableInfo(ident, md.columns(), md.references(), md.primaryKey(), md.routingCol(),
                md.isAlias(),
                concreteIndices, clusterService);
    }

}

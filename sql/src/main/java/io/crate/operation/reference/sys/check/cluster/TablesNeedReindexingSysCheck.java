/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.reference.sys.check.cluster;

import io.crate.metadata.PartitionName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.reference.sys.check.AbstractSysCheck;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Singleton
public class TablesNeedReindexingSysCheck extends AbstractSysCheck {

    public static final int ID = 3;
    public static final String DESCRIPTION =
        "The following tables must be re-indexed to be able to be used in future releases of CrateDB: ";

    private final ClusterService clusterService;
    private final Schemas schemas;
    private volatile Collection<String> tablesNeedReindexing;

    @Inject
    public TablesNeedReindexingSysCheck(ClusterService clusterService, Schemas schemas) {
        super(ID, DESCRIPTION, Severity.MEDIUM);
        this.clusterService = clusterService;
        this.schemas = schemas;
    }

    @Override
    public BytesRef description() {
        String linkedDescriptionBuilder = DESCRIPTION + tablesNeedReindexing + ' ' + LINK_PATTERN + ID;
        return new BytesRef(linkedDescriptionBuilder);
    }

    @Override
    public boolean validate() {
        List<String> newTablesNeedReindexing = new ArrayList<>();
        for (SchemaInfo schemaInfo : schemas) {
            if (schemaInfo instanceof DocSchemaInfo) {
                for (TableInfo tableInfo : schemaInfo) {
                    if (((DocTableInfo) tableInfo).isPartitioned()) {
                        // check each partition, if one needs reindexing, complete table will be marked
                        for (PartitionName partitionName : ((DocTableInfo) tableInfo).partitions()) {
                            IndexMetaData indexMetaData = clusterService.state().metaData().index(partitionName.asIndexName());
                            assert indexMetaData != null :
                                "could not get metadata for partition " + partitionName.asIndexName();
                            if (checkIndexMetaData(tableInfo, indexMetaData, newTablesNeedReindexing)) {
                                break;
                            }
                        }
                    } else {
                        IndexMetaData metaData = ((DocTableInfo) tableInfo).metadata();
                        checkIndexMetaData(tableInfo, metaData, newTablesNeedReindexing);
                    }
                }
            }
        }
        Collections.sort(newTablesNeedReindexing);
        tablesNeedReindexing = newTablesNeedReindexing;
        return newTablesNeedReindexing.isEmpty();
    }

    private boolean checkIndexMetaData(TableInfo tableInfo, IndexMetaData metaData, List<String> newTablesNeedReindexing) {
        if (IndexMetaDataChecks.checkReindexIsRequired(metaData)) {
            newTablesNeedReindexing.add(tableInfo.ident().fqn());
            return true;
        }
        return false;
    }
}

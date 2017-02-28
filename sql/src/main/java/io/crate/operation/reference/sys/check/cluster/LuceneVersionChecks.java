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
import org.apache.lucene.util.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;

import java.text.ParseException;
import java.util.Collection;
import java.util.HashSet;

final class LuceneVersionChecks {

    static boolean checkUpgradeRequired(String versionStr) {
        if (versionStr == null || versionStr.isEmpty()) {
            return false;
        }

        try {
            Version version = Version.parse(versionStr);
            return !version.onOrAfter(Version.LATEST);
        } catch (ParseException e) {
            return false;
        }
    }

    static boolean checkReindexRequired(IndexMetaData indexMetaData) {
        return indexMetaData != null &&
               !indexMetaData.getCreationVersion().luceneVersion.onOrAfter(Version.LUCENE_4_10_0);
    }

    static Collection<String> tablesNeedReindexing(Schemas schemas, MetaData clusterIndexMetaData) {
        Collection<String> tablesNeedReindexing = new HashSet<>();
        for (SchemaInfo schemaInfo : schemas) {
            if (schemaInfo instanceof DocSchemaInfo) {
                for (TableInfo tableInfo : schemaInfo) {
                    if (((DocTableInfo) tableInfo).isPartitioned()) {
                        // check each partition, if one needs reindexing, complete table will be marked
                        for (PartitionName partitionName : ((DocTableInfo) tableInfo).partitions()) {
                            IndexMetaData indexMetaData = clusterIndexMetaData.index(partitionName.asIndexName());
                            assert indexMetaData != null :
                                "could not get metadata for partition " + partitionName.asIndexName();
                            if (checkIndexMetaData(tableInfo, indexMetaData, tablesNeedReindexing)) {
                                break;
                            }
                        }
                    } else {
                        IndexMetaData metaData = ((DocTableInfo) tableInfo).metaData();
                        checkIndexMetaData(tableInfo, metaData, tablesNeedReindexing);
                    }
                }
            }
        }
        return tablesNeedReindexing;
    }

    private static boolean checkIndexMetaData(TableInfo tableInfo, IndexMetaData metaData,
                                              Collection<String> tablesNeedReindexing) {
        if (LuceneVersionChecks.checkReindexRequired(metaData)) {
            tablesNeedReindexing.add(tableInfo.ident().fqn());
            return true;
        }
        return false;
    }
}

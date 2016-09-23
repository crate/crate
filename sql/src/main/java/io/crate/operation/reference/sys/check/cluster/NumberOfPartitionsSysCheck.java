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

import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.reference.sys.check.AbstractSysCheck;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

@Singleton
public class NumberOfPartitionsSysCheck extends AbstractSysCheck {

    private static final int ID = 2;
    private static final String DESCRIPTION =
        "The total number of partitions of one or more partitioned tables should" +
        " not be greater than 1000. A large amount of shards can significantly reduce performance.";

    private static final int PARTITIONS_THRESHOLD = 1000;
    private final ReferenceInfos referenceInfos;

    @Inject
    public NumberOfPartitionsSysCheck(ReferenceInfos referenceInfos) {
        super(ID, DESCRIPTION, Severity.MEDIUM);
        this.referenceInfos = referenceInfos;
    }

    @Override
    public boolean validate() {
        for (SchemaInfo schemaInfo : referenceInfos) {
            if (schemaInfo instanceof DocSchemaInfo && !validateDocTablesPartitioning(schemaInfo)) {
                return false;
            }
        }
        return true;
    }

    boolean validateDocTablesPartitioning(SchemaInfo schemaInfo) {
        for (TableInfo tableInfo : schemaInfo) {
            DocTableInfo docTableInfo = (DocTableInfo) tableInfo;
            if (docTableInfo.isPartitioned() && docTableInfo.partitions().size() > PARTITIONS_THRESHOLD) {
                return false;
            }
        }
        return true;
    }

}

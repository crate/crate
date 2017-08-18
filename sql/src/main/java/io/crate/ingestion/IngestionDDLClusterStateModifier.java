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

package io.crate.ingestion;

import io.crate.metadata.TableIdent;
import io.crate.metadata.cluster.DDLClusterStateModifier;
import io.crate.metadata.rule.ingest.IngestRulesMetaData;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;

public class IngestionDDLClusterStateModifier implements DDLClusterStateModifier {

    @Override
    public ClusterState onRenameTable(ClusterState currentState,
                                      TableIdent sourceTableIdent,
                                      TableIdent targetTableIdent,
                                      boolean isPartitionedTable) {
        MetaData currentMetaData = currentState.metaData();
        MetaData.Builder mdBuilder = MetaData.builder(currentMetaData);
        if (transferIngestionRules(mdBuilder, sourceTableIdent, targetTableIdent)) {
            return ClusterState.builder(currentState).metaData(mdBuilder).build();
        }
        return currentState;
    }

    private static boolean transferIngestionRules(MetaData.Builder mdBuilder,
                                                  TableIdent sourceTableIdent,
                                                  TableIdent targetTableIdent) {
        IngestRulesMetaData oldMetaData = (IngestRulesMetaData) mdBuilder.getCustom(IngestRulesMetaData.TYPE);
        if (oldMetaData == null) {
            return false;
        }

        // create a new instance of the metadata if rules were changed, to guarantee the cluster changed action.
        IngestRulesMetaData newMetaData = IngestRulesMetaData.maybeCopyAndReplaceTargetTableIdents(
            oldMetaData, sourceTableIdent.fqn(), targetTableIdent.fqn());

        if (newMetaData != null) {
            mdBuilder.putCustom(IngestRulesMetaData.TYPE, newMetaData);
            return true;
        }
        return false;
    }
}

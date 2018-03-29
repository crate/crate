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

import io.crate.metadata.RelationName;
import io.crate.metadata.cluster.DDLClusterStateModifier;
import io.crate.metadata.rule.ingest.IngestRulesMetaData;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;

public class IngestionDDLClusterStateModifier implements DDLClusterStateModifier {

    @Override
    public ClusterState onRenameTable(ClusterState currentState,
                                      RelationName sourceRelationName,
                                      RelationName targetRelationName,
                                      boolean isPartitionedTable) {
        MetaData currentMetaData = currentState.metaData();
        MetaData.Builder mdBuilder = MetaData.builder(currentMetaData);
        if (transferIngestionRules(mdBuilder, sourceRelationName, targetRelationName)) {
            return ClusterState.builder(currentState).metaData(mdBuilder).build();
        }
        return currentState;
    }

    private static boolean transferIngestionRules(MetaData.Builder mdBuilder,
                                                  RelationName sourceRelationName,
                                                  RelationName targetRelationName) {
        IngestRulesMetaData oldMetaData = (IngestRulesMetaData) mdBuilder.getCustom(IngestRulesMetaData.TYPE);
        if (oldMetaData == null) {
            return false;
        }

        // a new instance of the metadata is created if any rules are transferred from source to target (this guarantees
        // a cluster change event is triggered)
        IngestRulesMetaData newMetaData = IngestRulesMetaData.maybeCopyAndReplaceTargetTableIdents(
            oldMetaData, sourceRelationName.fqn(), targetRelationName.fqn());

        if (newMetaData != null) {
            mdBuilder.putCustom(IngestRulesMetaData.TYPE, newMetaData);
            return true;
        }
        return false;
    }
}

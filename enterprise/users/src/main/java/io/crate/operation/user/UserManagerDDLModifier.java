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

package io.crate.operation.user;

import io.crate.metadata.TableIdent;
import io.crate.metadata.UsersPrivilegesMetaData;
import io.crate.metadata.cluster.DDLClusterStateModifier;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;

public class UserManagerDDLModifier implements DDLClusterStateModifier {

    @Override
    public ClusterState onDropTable(ClusterState currentState, TableIdent tableIdent) {
        MetaData currentMetaData = currentState.metaData();
        MetaData.Builder mdBuilder = MetaData.builder(currentMetaData);

        if (dropPrivileges(mdBuilder, tableIdent) == false) {
            // if nothing is affected, don't modify the state and just return the given currentState
            return currentState;
        }

        return ClusterState.builder(currentState).metaData(mdBuilder).build();
    }

    private boolean dropPrivileges(MetaData.Builder mdBuilder, TableIdent tableIdent) {
        // create a new instance of the metadata, to guarantee the cluster changed action.
        UsersPrivilegesMetaData newMetaData = UsersPrivilegesMetaData.copyOf(
            (UsersPrivilegesMetaData) mdBuilder.getCustom(UsersPrivilegesMetaData.TYPE));

        long affectedRows = newMetaData.dropTablePrivileges(tableIdent.fqn());
        mdBuilder.putCustom(UsersPrivilegesMetaData.TYPE, newMetaData);
        return affectedRows > 0L;
    }
}

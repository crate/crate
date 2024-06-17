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

package io.crate.metadata.information;

import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.STRING;

import io.crate.execution.engine.collect.files.SqlFeatureContext;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;


public class InformationSqlFeaturesTableInfo {

    public static final String NAME = "sql_features";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    public static SystemTable<SqlFeatureContext> INSTANCE = SystemTable.<SqlFeatureContext>builder(IDENT)
        .add("feature_id", STRING, SqlFeatureContext::getFeatureId)
        .add("feature_name", STRING, SqlFeatureContext::getFeatureName)
        .add("sub_feature_id", STRING, SqlFeatureContext::getSubFeatureId)
        .add("sub_feature_name", STRING, SqlFeatureContext::getSubFeatureName)
        .add("is_supported", BOOLEAN, SqlFeatureContext::isSupported)
        .add("is_verified_by", STRING, SqlFeatureContext::getIsVerifiedBy)
        .add("comments", STRING, SqlFeatureContext::getComments)
        .setPrimaryKeys(
            new ColumnIdent("feature_id"),
            new ColumnIdent("feature_name"),
            new ColumnIdent("sub_feature_id"),
            new ColumnIdent("sub_feature_name"),
            new ColumnIdent("is_supported"),
            new ColumnIdent("is_verified_by")
        )
        .build();
}

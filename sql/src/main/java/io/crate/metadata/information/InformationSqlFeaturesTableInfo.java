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

package io.crate.metadata.information;

import io.crate.execution.engine.collect.files.SqlFeatureContext;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;

import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.STRING;


public class InformationSqlFeaturesTableInfo extends InformationTableInfo<SqlFeatureContext> {

    public static final String NAME = "sql_features";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    private static ColumnRegistrar<SqlFeatureContext> columnRegistrar() {
        return new ColumnRegistrar<SqlFeatureContext>(IDENT, RowGranularity.DOC)
            .register("feature_id", STRING, () -> forFunction(SqlFeatureContext::getFeatureId))
            .register("feature_name", STRING, () -> forFunction(SqlFeatureContext::getFeatureName))
            .register("sub_feature_id", STRING, () -> forFunction(SqlFeatureContext::getSubFeatureId))
            .register("sub_feature_name", STRING, () -> forFunction(SqlFeatureContext::getSubFeatureName))
            .register("is_supported", BOOLEAN, () -> forFunction(SqlFeatureContext::isSupported))
            .register("is_verified_by", STRING, () -> forFunction(SqlFeatureContext::getIsVerifiedBy))
            .register("comments", STRING, () -> forFunction(SqlFeatureContext::getComments));
    }

    static Map<ColumnIdent, RowCollectExpressionFactory<SqlFeatureContext>> expressions() {
        return columnRegistrar().expressions();
    }

    InformationSqlFeaturesTableInfo() {
        super(IDENT, columnRegistrar(), "feature_id", "feature_name", "sub_feature_id", "sub_feature_name",
              "is_supported", "is_verified_by");
    }
}

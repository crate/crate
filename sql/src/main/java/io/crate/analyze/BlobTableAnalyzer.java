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

package io.crate.analyze;

import com.google.common.base.Preconditions;
import io.crate.analyze.expressions.ExpressionToNumberVisitor;
import io.crate.analyze.expressions.ExpressionToObjectVisitor;
import io.crate.core.NumberOfReplicas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.Table;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public abstract class BlobTableAnalyzer<TypeAnalysis extends AnalyzedStatement>
        extends AbstractStatementAnalyzer<Void, TypeAnalysis> {

    protected static TableIdent tableToIdent(Table table) {
        List<String> tableNameParts = table.getName().getParts();
        Preconditions.checkArgument(tableNameParts.size() < 3, "Invalid tableName \"%s\"", table.getName());

        if (tableNameParts.size() == 2) {
            Preconditions.checkArgument(tableNameParts.get(0).equalsIgnoreCase(BlobSchemaInfo.NAME),
                    "The Schema \"%s\" isn't valid in a [CREATE | ALTER] BLOB TABLE clause",
                    tableNameParts.get(0));

            return new TableIdent(tableNameParts.get(0), tableNameParts.get(1));
        }
        assert tableNameParts.size() == 1;
        return new TableIdent(BlobSchemaInfo.NAME, tableNameParts.get(0));
    }

    protected static NumberOfReplicas extractNumberOfReplicas(
            GenericProperties genericProperties,
            Object[] parameters) {
        Map<String,Expression> properties = genericProperties.properties();
        Expression number_of_replicas = properties.remove("number_of_replicas");

        NumberOfReplicas numberOfReplicas = null;
        if (number_of_replicas != null) {
            try {
                Integer numReplicas = ExpressionToNumberVisitor.convert(number_of_replicas, parameters).intValue();
                numberOfReplicas = new NumberOfReplicas(numReplicas);
            } catch (IllegalArgumentException e) {
                String numReplicas = ExpressionToObjectVisitor.convert(number_of_replicas, parameters).toString();
                numberOfReplicas = new NumberOfReplicas(numReplicas);
            }
        }

        if (properties.size() > 0) {
            throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Invalid properties \"%s\" passed to [ALTER | CREATE] BLOB TABLE statement",
                            properties.keySet()));
        }
        return numberOfReplicas;
    }
}

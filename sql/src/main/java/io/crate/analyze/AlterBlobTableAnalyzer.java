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
import io.crate.sql.tree.AlterBlobTable;
import io.crate.sql.tree.GenericProperties;

import java.util.List;

public class AlterBlobTableAnalyzer extends BlobTableAnalyzer<AlterBlobTableAnalysis> {

    @Override
    public Void visitAlterBlobTable(AlterBlobTable node, AlterBlobTableAnalysis context) {
        context.table(tableToIdent(node.table()));


        if (node.genericProperties().isPresent()) {
            updateProperties(context, node.genericProperties().get());
        } else {
            resetPropertiesValidation(context, node.resetProperties());
        }

        return null;
    }

    private void resetPropertiesValidation(AlterBlobTableAnalysis context, List<String> properties) {
        Preconditions.checkArgument(properties.size() == 1, "Blob tables currently have only 1 property");
        String propertyName = properties.get(1);
        Preconditions.checkArgument(propertyName.equalsIgnoreCase("number_of_replicas"),
                "the only supported parameter on blob tables it \"number_of_replicas\"");
    }

    private void updateProperties(AlterBlobTableAnalysis context, GenericProperties genericProperties) {
        context.numberOfReplicas(extractNumberOfReplicas(genericProperties, context.parameters()));
    }
}

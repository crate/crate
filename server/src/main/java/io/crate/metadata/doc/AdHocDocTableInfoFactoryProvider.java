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

package io.crate.metadata.doc;

import org.elasticsearch.cluster.metadata.Metadata;

import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;

/**
 * Creates {@link DocTableInfoFactory} instances based on the given metadata.
 * Metadata can come from a foreign cluster, so table validation must use UDFs from that metadata
 * instead of the local UDF registry.
 */
public class AdHocDocTableInfoFactoryProvider {

    private final NodeContext nodeContext;
    private final UserDefinedFunctionService userDefinedFunctionService;

    public AdHocDocTableInfoFactoryProvider(NodeContext nodeContext,
                                            UserDefinedFunctionService userDefinedFunctionService) {
        this.nodeContext = nodeContext;
        this.userDefinedFunctionService = userDefinedFunctionService;
    }

    public DocTableInfoFactory forMetadata(Metadata metadata) {
        Functions functions = nodeContext.functions().copyOfBuiltIns();
        functions.setUDFs(userDefinedFunctionService.buildUDFResolvers(metadata));
        return new DocTableInfoFactory(
            new NodeContext(
                functions,
                nodeContext.roles(),
                _ -> nodeContext.schemas(),
                nodeContext.tableStats()
            )
        );
    }
}

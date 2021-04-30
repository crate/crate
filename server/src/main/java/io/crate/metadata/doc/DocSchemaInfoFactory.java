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

import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.metadata.NodeContext;
import io.crate.metadata.view.ViewInfoFactory;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

@Singleton
public class DocSchemaInfoFactory {

    private final DocTableInfoFactory docTableInfoFactory;
    private final ViewInfoFactory viewInfoFactory;
    private final NodeContext nodeCtx;
    private final UserDefinedFunctionService udfService;

    @Inject
    public DocSchemaInfoFactory(DocTableInfoFactory docTableInfoFactory,
                                ViewInfoFactory viewInfoFactory,
                                NodeContext nodeCtx,
                                UserDefinedFunctionService udfService) {
        this.docTableInfoFactory = docTableInfoFactory;
        this.viewInfoFactory = viewInfoFactory;
        this.nodeCtx = nodeCtx;
        this.udfService = udfService;
    }

    public DocSchemaInfo create(String schemaName, ClusterService clusterService) {
        return new DocSchemaInfo(schemaName, clusterService, nodeCtx, udfService, viewInfoFactory, docTableInfoFactory);
    }
}

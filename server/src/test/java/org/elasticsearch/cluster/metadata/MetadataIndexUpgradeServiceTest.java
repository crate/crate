/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.elasticsearch.cluster.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.junit.Test;

import io.crate.expression.udf.UdfUnitTest;
import io.crate.expression.udf.UserDefinedFunctionMetadata;
import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.SearchPath;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class MetadataIndexUpgradeServiceTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_upgradeIndexMetadata_ensure_UDFs_are_loaded_before_checkMappingsCompatibility_is_called() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService);
        e.udfService().registerLanguage(UdfUnitTest.DUMMY_LANG);
        var metadataIndexUpgradeService = new MetadataIndexUpgradeService(
            e.nodeCtx,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            e.udfService()
        );
        metadataIndexUpgradeService.upgradeIndexMetadata(
            IndexMetadata.builder("test")
                .settings(settings(Version.V_5_7_0))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build(),
            IndexTemplateMetadata.builder("test")
                .patterns(List.of("*"))
                .putMapping("{\"default\": {}}")
                .build(),
            Version.V_5_7_0,
            UserDefinedFunctionsMetadata.of(new UserDefinedFunctionMetadata(
                "custom",
                "foo",
                List.of(),
                DataTypes.INTEGER,
                "dummy",
                "def foo(): return 1"
            )));
        FunctionImplementation functionImplementation = e.nodeCtx.functions().get(
            "custom",
            "foo",
            List.of(),
            SearchPath.pathWithPGCatalogAndDoc()
        );
        assertThat(functionImplementation).isNotNull();
    }
}

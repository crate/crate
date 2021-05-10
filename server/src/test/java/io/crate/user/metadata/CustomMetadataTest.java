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

package io.crate.user.metadata;

import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.expression.udf.UserDefinedFunctionsMetadataTest;
import io.crate.metadata.view.ViewsMetadata;
import io.crate.metadata.view.ViewsMetadataTest;
import io.crate.plugin.SQLPlugin;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class CustomMetadataTest {

    private NamedXContentRegistry getNamedXContentRegistry() {
        List<NamedXContentRegistry.Entry> registry = new ArrayList<>();
        registry.addAll(new SQLPlugin(Settings.EMPTY).getNamedXContent());
        registry.addAll(ClusterModule.getNamedXWriteables());
        return new NamedXContentRegistry(registry);
    }

    @Test
    public void testAllMetaDataXContentRoundtrip() throws IOException {
        Metadata metadata = Metadata.builder()
            .putCustom(UsersMetadata.TYPE,
                new UsersMetadata(UserDefinitions.DUMMY_USERS))
            .putCustom(UserDefinedFunctionsMetadata.TYPE,
                UserDefinedFunctionsMetadataTest.DUMMY_UDF_METADATA)
            .putCustom(UsersPrivilegesMetadata.TYPE,
                UsersPrivilegesMetadataTest.createMetadata())
            .putCustom(ViewsMetadata.TYPE,
                ViewsMetadataTest.createMetadata())
            .generateClusterUuidIfNeeded()
            .version(1L)
            .build();

        String xContent = xContentFromMetadata(metadata);

        XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
            getNamedXContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            xContent
        );

        Metadata restoredMetadata = Metadata.FORMAT.fromXContent(parser);
        boolean isEqual = Metadata.isGlobalStateEquals(restoredMetadata, metadata);
        if (!isEqual) {
            assertEquals("meta-data must be equal", xContent, xContentFromMetadata(restoredMetadata));
        }
        assertTrue(isEqual);
        assertThat(parser.currentToken(), is(XContentParser.Token.END_OBJECT));
    }

    private String xContentFromMetadata(Metadata metadata) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        Metadata.FORMAT.toXContent(builder, metadata);
        builder.endObject();
        return Strings.toString(builder);
    }

}

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

package io.crate.metadata;

import io.crate.expression.udf.UserDefinedFunctionsMetaData;
import io.crate.expression.udf.UserDefinedFunctionsMetaDataTest;
import io.crate.metadata.view.ViewsMetaData;
import io.crate.metadata.view.ViewsMetaDataTest;
import io.crate.plugin.SQLPlugin;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class CustomMetaDataTest {

    private NamedXContentRegistry getNamedXContentRegistry() {
        List<NamedXContentRegistry.Entry> registry = new ArrayList<>();
        registry.addAll(new SQLPlugin(Settings.EMPTY).getNamedXContent());
        registry.addAll(ClusterModule.getNamedXWriteables());
        return new NamedXContentRegistry(registry);
    }

    @Test
    public void testAllMetaDataXContentRoundtrip() throws IOException {
        MetaData metaData = MetaData.builder()
            .putCustom(UsersMetaData.TYPE,
                new UsersMetaData(UserDefinitions.DUMMY_USERS))
            .putCustom(UserDefinedFunctionsMetaData.TYPE,
                UserDefinedFunctionsMetaDataTest.DUMMY_UDF_META_DATA)
            .putCustom(UsersPrivilegesMetaData.TYPE,
                UsersPrivilegesMetaDataTest.createMetaData())
            .putCustom(ViewsMetaData.TYPE,
                ViewsMetaDataTest.createMetaData())
            .generateClusterUuidIfNeeded()
            .version(1L)
            .build();

        String xContent = xContentFromMetaData(metaData);

        XContentParser parser = XContentHelper.createParser(getNamedXContentRegistry(),
            new BytesArray(xContent.getBytes()),
            XContentType.JSON);

        MetaData restoredMetaData = MetaData.FORMAT.fromXContent(parser);
        boolean isEqual = MetaData.isGlobalStateEquals(restoredMetaData, metaData);
        if (!isEqual) {
            assertEquals("meta-data must be equal", xContent, xContentFromMetaData(restoredMetaData));
        }
        assertTrue(isEqual);
        assertThat(parser.currentToken(), is(XContentParser.Token.END_OBJECT));
    }

    private String xContentFromMetaData(MetaData metaData) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        MetaData.FORMAT.toXContent(builder, metaData);
        builder.endObject();
        return builder.string();
    }

}

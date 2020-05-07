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

package io.crate.analyze;

import io.crate.analyze.repositories.RepositoryParamValidator;
import io.crate.analyze.repositories.RepositorySettingsModule;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.GenericProperty;
import io.crate.sql.tree.StringLiteral;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class RepositoryParamValidatorTest extends CrateUnitTest {

    private RepositoryParamValidator validator;

    @Before
    public void initValidator() {
        validator = new ModulesBuilder()
            .add(new RepositorySettingsModule()).createInjector().getInstance(RepositoryParamValidator.class);
    }

    @Test
    public void testValidate() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid repository type \"invalid_type\"");
        validator.validate("invalid_type", GenericProperties.empty(), Settings.EMPTY);
    }

    @Test
    public void testRequiredTypeIsMissing() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "The following required parameters are missing to create a repository of type \"fs\": [location]");
        validator.validate("fs", GenericProperties.empty(), Settings.EMPTY);
    }

    @Test
    public void testValidateHdfsDynamicConfParam() throws Exception {
        GenericProperties<Expression> genericProperties = new GenericProperties<>();
        genericProperties.add(new GenericProperty<>("path", new StringLiteral("/data")));
        genericProperties.add(new GenericProperty<>("conf.foobar", new StringLiteral("bar")));
        validator.validate("hdfs", genericProperties, toSettings(genericProperties));
    }

    @Test
    public void testValidateHdfsSecurityPrincipal() throws Exception {
        GenericProperties<Expression> genericProperties = new GenericProperties<>();
        genericProperties.add(new GenericProperty<>("uri", new StringLiteral("hdfs://ha-name:8020")));
        genericProperties.add(new GenericProperty<>("security.principal", new StringLiteral("myuserid@REALM.DOMAIN")));
        genericProperties.add(new GenericProperty<>("path", new StringLiteral("/user/name/data")));
        genericProperties.add(new GenericProperty<>("conf.foobar", new StringLiteral("bar")));
        validator.validate("hdfs", genericProperties, toSettings(genericProperties));
    }

    @Test
    public void testValidateS3ConfigParams() {
        GenericProperties<Expression> genericProperties = new GenericProperties<>();
        genericProperties.add(new GenericProperty<>("access_key", new StringLiteral("foobar")));
        genericProperties.add(new GenericProperty<>("base_path", new StringLiteral("/data")));
        genericProperties.add(new GenericProperty<>("bucket", new StringLiteral("myBucket")));
        genericProperties.add(new GenericProperty<>("buffer_size", new StringLiteral("5mb")));
        genericProperties.add(new GenericProperty<>("canned_acl", new StringLiteral("cannedACL")));
        genericProperties.add(new GenericProperty<>("chunk_size", new StringLiteral("4g")));
        genericProperties.add(new GenericProperty<>("compress", new StringLiteral("true")));
        genericProperties.add(new GenericProperty<>("endpoint", new StringLiteral("myEndpoint")));
        genericProperties.add(new GenericProperty<>("max_retries", new StringLiteral("8")));
        genericProperties.add(new GenericProperty<>("protocol", new StringLiteral("http")));
        genericProperties.add(new GenericProperty<>("secret_key", new StringLiteral("thisIsASecretKey")));
        genericProperties.add(new GenericProperty<>("server_side_encryption", new StringLiteral("false")));
        validator.validate(
            "s3",
            genericProperties,
            toSettings(genericProperties));
    }

    private static Settings toSettings(GenericProperties<Expression> genericProperties) {
        Settings.Builder builder = Settings.builder();
        for (Map.Entry<String, Expression> property : genericProperties.properties().entrySet()) {
            builder.put(property.getKey(), property.getValue().toString());
        }
        return builder.build();
    }
}

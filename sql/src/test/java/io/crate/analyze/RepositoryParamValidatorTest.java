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
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.GenericProperty;
import io.crate.sql.tree.StringLiteral;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.hamcrest.Matchers.is;

public class RepositoryParamValidatorTest extends CrateUnitTest {

    private RepositoryParamValidator validator;

    @Before
    public void initValidator() throws Exception {
        validator = new ModulesBuilder()
            .add(new RepositorySettingsModule()).createInjector().getInstance(RepositoryParamValidator.class);
    }

    @Test
    public void testValidate() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid repository type \"invalid_type\"");
        validator.convertAndValidate("invalid_type", GenericProperties.EMPTY, ParameterContext.EMPTY);
    }

    @Test
    public void testRequiredTypeIsMissing() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "The following required parameters are missing to create a repository of type \"fs\": [location]");
        validator.convertAndValidate("fs", GenericProperties.EMPTY, ParameterContext.EMPTY);
    }

    @Test
    public void testInvalidSetting() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("setting 'yay' not supported");
        GenericProperties genericProperties = new GenericProperties();
        genericProperties.add(new GenericProperty("location", new StringLiteral("foo")));
        genericProperties.add(new GenericProperty("yay", new StringLiteral("invalid")));
        validator.convertAndValidate("fs", genericProperties, ParameterContext.EMPTY);
    }

    @Test
    public void testHdfsDynamicConfParam() throws Exception {
        GenericProperties genericProperties = new GenericProperties();
        genericProperties.add(new GenericProperty("path", new StringLiteral("/data")));
        genericProperties.add(new GenericProperty("conf.foobar", new StringLiteral("bar")));
        Settings settings = validator.convertAndValidate("hdfs", genericProperties, ParameterContext.EMPTY);
        assertThat(settings.get("conf.foobar"), is("bar"));
    }

    @Test
    public void testS3ConfigParams() throws Exception {
        GenericProperties genericProperties = new GenericProperties();
        genericProperties.add(new GenericProperty("access_key", new StringLiteral("foobar")));
        genericProperties.add(new GenericProperty("base_path", new StringLiteral("/data")));
        genericProperties.add(new GenericProperty("bucket", new StringLiteral("myBucket")));
        genericProperties.add(new GenericProperty("buffer_size", new StringLiteral("10k")));
        genericProperties.add(new GenericProperty("canned_acl", new StringLiteral("cannedACL")));
        genericProperties.add(new GenericProperty("chunk_size", new StringLiteral("4g")));
        genericProperties.add(new GenericProperty("compress", new StringLiteral("true")));
        genericProperties.add(new GenericProperty("concurrent_streams", new StringLiteral("12")));
        genericProperties.add(new GenericProperty("endpoint", new StringLiteral("myEndpoint")));
        genericProperties.add(new GenericProperty("max_retries", new StringLiteral("8")));
        genericProperties.add(new GenericProperty("protocol", new StringLiteral("myProtocol")));
        genericProperties.add(new GenericProperty("region", new StringLiteral("Europe-1")));
        genericProperties.add(new GenericProperty("secret_key", new StringLiteral("thisIsASecretKey")));
        genericProperties.add(new GenericProperty("server_side_encryption", new StringLiteral("false")));
        Settings settings = validator.convertAndValidate("s3", genericProperties, ParameterContext.EMPTY);
        assertThat(settings.get("access_key"), is("foobar"));
        assertThat(settings.get("base_path"), is("/data"));
        assertThat(settings.get("bucket"), is("myBucket"));
        assertThat(settings.get("buffer_size"), is("10240b"));
        assertThat(settings.get("canned_acl"), is("cannedACL"));
        assertThat(settings.get("chunk_size"), is("4294967296b"));
        assertThat(settings.get("compress"), is("true"));
        assertThat(settings.get("concurrent_streams"), is("12"));
        assertThat(settings.get("endpoint"), is("myEndpoint"));
        assertThat(settings.get("max_retries"), is("8"));
        assertThat(settings.get("protocol"), is("myProtocol"));
        assertThat(settings.get("region"), is("Europe-1"));
        assertThat(settings.get("secret_key"), is("thisIsASecretKey"));
        assertThat(settings.get("server_side_encryption"), is("false"));
    }
}

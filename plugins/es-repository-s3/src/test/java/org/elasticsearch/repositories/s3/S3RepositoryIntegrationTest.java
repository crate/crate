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

package org.elasticsearch.repositories.s3;

import io.crate.integrationtests.SQLIntegrationTestCase;
import org.elasticsearch.plugins.Plugin;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThrowsMatches;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.hamcrest.Matchers.startsWith;

public class S3RepositoryIntegrationTest extends SQLIntegrationTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(S3RepositoryPlugin.class);
        return plugins;
    }

    @Test
    public void test_unable_to_create_s3_repository() throws Throwable {
        assertThrowsMatches(() -> execute(
            "create repository test123 type s3 with (bucket='bucket', endpoint='https://s3.region.amazonaws.com', " +
            "protocol='https', access_key='access',secret_key='secret', base_path='test123')"),
                     isSQLError(
                         startsWith("[test123] Unable to verify the repository, [test123] is not accessible on master " +
                              "node: SdkClientException 'Unable to execute HTTP request: bucket.s3.region.amazonaws.com"),
                         INTERNAL_ERROR,
                         INTERNAL_SERVER_ERROR,
                         5000)
        );
    }
}

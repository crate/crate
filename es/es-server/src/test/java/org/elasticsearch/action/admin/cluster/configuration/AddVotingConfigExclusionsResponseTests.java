/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.cluster.configuration;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class AddVotingConfigExclusionsResponseTests extends ESTestCase {
    public void testSerialization() throws IOException {
        final AddVotingConfigExclusionsResponse originalRequest = new AddVotingConfigExclusionsResponse();
        copyStreamable(originalRequest, writableRegistry(), AddVotingConfigExclusionsResponse::new);
        // there are no fields so we're just checking that this doesn't throw anything
    }
}

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

package io.crate.metadata;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class DefaultTemplateServiceTest {

    @Test
    public void testAddDefaultTemplate() throws Exception {
        ClusterState state = ClusterState.builder(new ClusterName("foo")).build();

        ClusterState newState = DefaultTemplateService.addDefaultTemplate(state);
        assertThat(newState.getMetadata().templates().containsKey(DefaultTemplateService.TEMPLATE_NAME), is(true));

        // verify that it doesn't fail it the template already exists
        newState = DefaultTemplateService.addDefaultTemplate(newState);
        assertThat(newState.getMetadata().templates().containsKey(DefaultTemplateService.TEMPLATE_NAME), is(true));
    }
}

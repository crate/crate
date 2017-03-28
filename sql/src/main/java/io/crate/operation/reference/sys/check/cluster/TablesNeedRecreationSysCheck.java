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

package io.crate.operation.reference.sys.check.cluster;

import io.crate.operation.reference.sys.check.AbstractSysCheck;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.Collection;

@Singleton
public class TablesNeedRecreationSysCheck extends AbstractSysCheck {

    public static final int ID = 4;
    public static final String DESCRIPTION =
        "The following tables need to be recreated for compatibility with future versions of CrateDB: ";

    private final ClusterService clusterService;
    private volatile Collection<String> tablesNeedRecreation;

    @Inject
    public TablesNeedRecreationSysCheck(ClusterService clusterService) {
        super(ID, DESCRIPTION, Severity.MEDIUM);
        this.clusterService = clusterService;
    }

    @Override
    public BytesRef description() {
        String linkedDescriptionBuilder = DESCRIPTION + tablesNeedRecreation + ' ' + LINK_PATTERN + ID;
        return new BytesRef(linkedDescriptionBuilder);
    }

    @Override
    public boolean validate() {
        tablesNeedRecreation = LuceneVersionChecks.tablesNeedRecreation(clusterService.state().metaData());
        return tablesNeedRecreation.isEmpty();
    }
}

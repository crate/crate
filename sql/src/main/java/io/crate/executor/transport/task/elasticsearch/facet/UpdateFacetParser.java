/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport.task.elasticsearch.facet;

import com.google.common.base.Optional;
import io.crate.exceptions.SQLParseException;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.FacetParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;


/**
 *
 */
public class UpdateFacetParser extends AbstractComponent implements FacetParser {

    private final TransportUpdateAction updateAction;

    @Inject
    public UpdateFacetParser(
            Settings settings,
            TransportUpdateAction updateAction) {
        super(settings);
        InternalUpdateFacet.registerStreams();
        this.updateAction = updateAction;
    }

    @Override
    public String[] types() {
        return new String[]{
                UpdateFacet.TYPE
        };
    }

    @Override
    public FacetExecutor.Mode defaultMainMode() {
        return FacetExecutor.Mode.COLLECTOR;
    }

    @Override
    public FacetExecutor.Mode defaultGlobalMode() {
        return FacetExecutor.Mode.COLLECTOR;
    }

    @SuppressWarnings("unchecked")
    @Override
    public FacetExecutor parse(String facetName, XContentParser parser,
            SearchContext searchContext) throws IOException {
        Map<String, Object> payload = parser.mapOrderedAndClose();
        if (!payload.containsKey("doc")) {
            throw new SQLParseException("update doc missing");
        }
        Map<String, Object> doc;
        Optional<Long> version;
        try {
            doc = (Map<String, Object>)payload.get("doc");
            Number versionNumber = (Number)payload.get("version");
            if (versionNumber == null) {
                version = Optional.absent();
            } else {
                version = Optional.of(versionNumber.longValue());
            }
        } catch (ClassCastException e) {
            throw new SQLParseException("invalid update doc");
        }
        return new UpdateFacetExecutor(doc, version, searchContext, updateAction);
    }
}

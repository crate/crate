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

package org.elasticsearch.gateway;

import com.google.common.collect.Maps;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public class UpcraterMetaStateService {

    private static final ESLogger LOGGER = Loggers.getLogger(UpcraterMetaStateService.class);

    private final XContentType format;
    private final ToXContent.Params formatParams;
    private final MetaDataStateFormat<IndexMetaData> indexStateFormat;

    public UpcraterMetaStateService(Settings settings) {
        this.format = XContentType.fromRestContentType(settings.get(MetaStateService.FORMAT_SETTING, "smile"));
        if (this.format == XContentType.SMILE) {
            Map<String, String> params = Maps.newHashMap();
            params.put("binary", "true");
            formatParams = new ToXContent.MapParams(params);
        } else {
            formatParams = ToXContent.EMPTY_PARAMS;
        }
        indexStateFormat = MetaStateService.indexStateFormat(format, formatParams);
    }

    /**
     * Writes a new {@link IndexMetaData} to the given index path.
     */
    public void writeIndexState(IndexMetaData indexMetaData, Path indexPath) throws Exception {
        try {
            indexStateFormat.write(indexMetaData, indexMetaData.getVersion(), indexPath);
        } catch (Throwable ex) {
            LOGGER.warn("[{}]: failed to write index state to [{}]", ex, indexMetaData.getIndex(), indexPath);
            throw new IOException("failed to write state for [" + indexMetaData.getIndex() + "] to [" + indexPath + "]", ex);
        }
    }
}

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

package org.elasticsearch.index.mapper.object;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MetadataFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Locale;

public class ObjectMapperProxy {

    final ObjectMapper objectMapper;

    public ObjectMapperProxy(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * This is a copy of {@link ObjectMapper#toXContent(XContentBuilder, ToXContent.Params, ToXContent)}
     *
     * but without the first "builder.startObject(simpleName())" and the last builder.endObject()
     */
    public void toXContent(XContentBuilder builder, ToXContent.Params params, ToXContent custom) throws IOException {
        if (objectMapper.nested().isNested()) {
            builder.field("type", ObjectMapper.NESTED_CONTENT_TYPE);
            if (objectMapper.nested().isIncludeInParent()) {
                builder.field("include_in_parent", true);
            }
            if (objectMapper.nested().isIncludeInRoot()) {
                builder.field("include_in_root", true);
            }
        } else if (Iterators.size(objectMapper.iterator()) == 0 && custom == null) { // only write the object content type if there are no properties, otherwise, it is automatically detected
            builder.field("type", ObjectMapper.CONTENT_TYPE);
        }
        if (objectMapper.dynamic() != null) {
            builder.field("dynamic", objectMapper.dynamic().name().toLowerCase(Locale.ROOT));
        }
        if (objectMapper.isEnabled() != ObjectMapper.Defaults.ENABLED) {
            builder.field("enabled", objectMapper.isEnabled());
        }
        if (objectMapper.pathType() != ObjectMapper.Defaults.PATH_TYPE) {
            builder.field("path", objectMapper.pathType().name().toLowerCase(Locale.ROOT));
        }
        // TODO: FIX ME! Can we ommit this?
        /*if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        }*/

        if (custom != null) {
            custom.toXContent(builder, params);
        }

        objectMapper.doXContent(builder, params);

        // sort the mappers so we get consistent serialization format
        ArrayList<Mapper> sortedMappers = Lists.newArrayList(objectMapper.iterator());
        Collections.sort(sortedMappers, new Comparator<Mapper>() {
            @Override
            public int compare(Mapper o1, Mapper o2) {
                return o1.name().compareTo(o2.name());
            }
        });

        int count = 0;
        for (Mapper mapper : sortedMappers) {
            if (!(mapper instanceof MetadataFieldMapper)) {
                if (count++ == 0) {
                    builder.startObject("properties");
                }
                mapper.toXContent(builder, params);
            }
        }
        if (count > 0) {
            builder.endObject();
        }
    }
}

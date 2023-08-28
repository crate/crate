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

package io.crate.metadata.view;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.jetbrains.annotations.Nullable;

import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.RelationName;
import io.crate.metadata.SearchPath;

public class ViewsMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String TYPE = "views";
    private final Map<String, ViewMetadata> viewByName;

    ViewsMetadata(Map<String, ViewMetadata> viewByName) {
        this.viewByName = viewByName;
    }

    public ViewsMetadata(StreamInput in) throws IOException {
        int numViews = in.readVInt();
        viewByName = new HashMap<>(numViews);
        for (int i = 0; i < numViews; i++) {
            viewByName.put(in.readString(), ViewMetadata.of(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(viewByName.size());
        for (Map.Entry<String, ViewMetadata> view : viewByName.entrySet()) {
            out.writeString(view.getKey());
            view.getValue().writeTo(out);
        }
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_3_0_1;
    }


    /*
     * ViewsMetadata XContent has the following structure:
     *
     * <pre>
     *     {
     *       "views": {
     *         "docs.my_view": {
     *           "stmt": "select x, y from t1 where z = 'a'",
     *           "owner": "user_a"
     *         }
     *       }
     *     }
     * </pre>
     *
     * <ul>
     *     <li>"docs.my_view" is the full qualified name of the view</li>
     *     <li>value of "stmt" is the analyzed SELECT statement</li>
     *     <li>value of "owner" is the name of the user who created the view</li>
     * </ul>
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TYPE);
        for (Map.Entry<String, ViewMetadata> entry : viewByName.entrySet()) {
            ViewMetadata view = entry.getValue();
            builder.startObject(entry.getKey());
            {
                builder.field("stmt", view.stmt());
                builder.field("owner", view.owner());
                builder.startArray("searchpath");
                for (String schema : view.searchPath().showPath()) {
                    builder.value(schema);
                }
                builder.endArray();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static ViewsMetadata fromXContent(XContentParser parser) throws IOException {
        Map<String, ViewMetadata> views = new HashMap<>();

        if (parser.nextToken() == XContentParser.Token.FIELD_NAME && parser.currentName().equals(TYPE)) {
            if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                    String viewName = parser.currentName();
                    if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                        String stmt = null;
                        String owner = null;
                        SearchPath searchPath = null;
                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            if ("stmt".equals(parser.currentName())) {
                                parser.nextToken();
                                stmt = parser.text();
                            }
                            if ("owner".equals(parser.currentName())) {
                                parser.nextToken();
                                owner = parser.textOrNull();
                            }
                            if ("searchpath".equals(parser.currentName())) {
                                List<String> paths = new ArrayList<>();
                                parser.nextToken();
                                while (parser.nextToken() != Token.END_ARRAY) {
                                    paths.add(parser.text());
                                }
                                searchPath = SearchPath.createSearchPathFrom(paths.toArray(String[]::new));
                            }
                        }
                        if (stmt == null) {
                            throw new ElasticsearchParseException("failed to parse views, expected field 'stmt' in object");
                        }
                        ViewMetadata viewMetadata = new ViewMetadata(
                            stmt,
                            owner,
                            searchPath == null ? SearchPath.pathWithPGCatalogAndDoc() : searchPath
                        );
                        views.put(viewName, viewMetadata);
                    }
                }
            }
            if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                // each custom metadata is packed inside an object.
                // each custom must move the parser to the end otherwise possible following customs won't be read
                throw new ElasticsearchParseException("failed to parse views, expected an object token at the end");
            }
        }
        return new ViewsMetadata(views);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ViewsMetadata that = (ViewsMetadata) o;
        return viewByName.equals(that.viewByName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(viewByName);
    }

    public boolean contains(RelationName relationName) {
        return viewByName.containsKey(relationName.fqn());
    }

    public Iterable<String> names() {
        return viewByName.keySet();
    }

    /**
     * @param searchPath
     * @return A copy of the ViewsMetadata with the new view added (or replaced in case it already existed)
     */
    public static ViewsMetadata addOrReplace(@Nullable ViewsMetadata prevViews,
                                             RelationName name,
                                             String query,
                                             @Nullable String owner,
                                             SearchPath searchPath) {
        HashMap<String, ViewMetadata> queryByName;
        if (prevViews == null) {
            queryByName = new HashMap<>();
        } else {
            queryByName = new HashMap<>(prevViews.viewByName);
        }
        queryByName.put(name.fqn(), new ViewMetadata(query, owner, searchPath));
        return new ViewsMetadata(queryByName);
    }

    public RemoveResult remove(List<RelationName> names) {
        HashMap<String, ViewMetadata> updatedQueryByName = new HashMap<>(this.viewByName);
        ArrayList<RelationName> missing = new ArrayList<>(names.size());
        for (RelationName name : names) {
            ViewMetadata removed = updatedQueryByName.remove(name.fqn());
            if (removed == null) {
                missing.add(name);
            }
        }
        return new RemoveResult(new ViewsMetadata(updatedQueryByName), missing);
    }

    /**
     * @throws RelationUnknown if source view doesn't exist.
     */
    public ViewsMetadata rename(RelationName source, RelationName target) {
        HashMap<String, ViewMetadata> newViewByName = new HashMap<>(viewByName);
        ViewMetadata removed = newViewByName.remove(source.fqn());
        if (removed == null) {
            throw new RelationUnknown(source);
        }
        newViewByName.put(target.fqn(), removed);
        return new ViewsMetadata(newViewByName);
    }

    @Nullable
    public ViewMetadata getView(RelationName name) {
        return viewByName.get(name.fqn());
    }

    public class RemoveResult {
        private final ViewsMetadata updatedViews;
        private final List<RelationName> missing;

        RemoveResult(ViewsMetadata updatedViews, List<RelationName> missing) {
            this.updatedViews = updatedViews;
            this.missing = missing;
        }

        public ViewsMetadata updatedViews() {
            return updatedViews;
        }

        public List<RelationName> missing() {
            return missing;
        }
    }
}

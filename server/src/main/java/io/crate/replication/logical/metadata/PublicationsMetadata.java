/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.replication.logical.metadata;

import io.crate.metadata.RelationName;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PublicationsMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String TYPE = "publications";

    public static PublicationsMetadata newInstance(@Nullable PublicationsMetadata instance) {
        if (instance == null) {
            return new PublicationsMetadata();
        }
        return new PublicationsMetadata(new HashMap<>(instance.publicationByName));
    }

    private final Map<String, Publication> publicationByName;

    public PublicationsMetadata(Map<String, Publication> publicationByName) {
        this.publicationByName = publicationByName;
    }

    public PublicationsMetadata(StreamInput in) throws IOException {
        int numPubs = in.readVInt();
        publicationByName = new HashMap<>(numPubs);
        for (int i = 0; i < numPubs; i++) {
            publicationByName.put(in.readString(), new Publication(in));
        }
    }

    private PublicationsMetadata() {
        this(new HashMap<>());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(publicationByName.size());
        for (var entry : publicationByName.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
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
        return Version.V_4_7_0;
    }

    /*
     * PublicationsMetadata XContent has the following structure:
     *
     * <pre>
     *     {
     *       "publications": {
     *         "my_pub1": {
     *           "owner": "user1",
     *           "tables": [ "table1", "table2" ]
     *         }
     *       }
     *     }
     * </pre>
     *
     * <ul>
     *     <li>"my_pub1" is the full qualified name of the publication</li>
     *     <li>value of "owner" is the name of the user who created the publication</li>
     *     <li>value of "tables" contains a list of all tables which are part of this publication</li>
     * </ul>
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TYPE);
        for (var entry : publicationByName.entrySet()) {
            var publication = entry.getValue();
            builder.startObject(entry.getKey());
            {
                builder.field("owner", publication.owner());
                builder.startArray("tables");
                for (var table : publication.tables()) {
                    builder.value(table.indexNameOrAlias());
                }
                builder.endArray();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static PublicationsMetadata fromXContent(XContentParser parser) throws IOException {
        Map<String, Publication> publications = new HashMap<>();

        if (parser.nextToken() == XContentParser.Token.FIELD_NAME && parser.currentName().equals(TYPE)) {
            if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                    String name = parser.currentName();
                    if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                        String owner = null;
                        var tables = new ArrayList<RelationName>();
                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            if ("owner".equals(parser.currentName())) {
                                parser.nextToken();
                                owner = parser.textOrNull();
                            }
                            if ("tables".equals(parser.currentName())) {
                                parser.nextToken();
                                while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                    tables.add(RelationName.fromIndexName(parser.text()));
                                }
                            }
                        }
                        if (owner == null) {
                            throw new ElasticsearchParseException("failed to parse publication, expected field 'owner' in object");
                        }
                        publications.put(name, new Publication(owner, tables));
                    }
                }
            }
            if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                // each custom metadata is packed inside an object.
                // each custom must move the parser to the end otherwise possible following customs won't be read
                throw new ElasticsearchParseException("failed to parse publications, expected an object token at the end");
            }
        }
        return new PublicationsMetadata(publications);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PublicationsMetadata that = (PublicationsMetadata) o;
        return publicationByName.equals(that.publicationByName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(publicationByName);
    }

    public Map<String, Publication> publications() {
        return publicationByName;
    }
}

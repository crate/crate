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

package io.crate.metadata;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.crate.core.StringUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ColumnIdent implements Comparable<ColumnIdent>, Streamable {

    public static final Function<ColumnIdent, String> GET_FQN_NAME_FUNCTION = new com.google.common.base.Function<ColumnIdent, String>() {
        @Nullable
        @Override
        public String apply(@Nullable ColumnIdent input) {
            if (input != null) {
                return input.fqn();
            }
            return null;
        }
    };

    private String name;
    private List<String> path;

    public ColumnIdent() {
    }

    public ColumnIdent(String name) {
        this.name = name;
        this.path = ImmutableList.of();
    }

    public ColumnIdent(String name, String childName) {
        this(name, ImmutableList.of(childName));
    }

    public ColumnIdent(String name, @Nullable List<String> path) {
        this(name);
        this.path = Objects.firstNonNull(path, ImmutableList.<String>of());
    }

    public static ColumnIdent fromPath(@Nullable String path) {
        if (path == null) {
            return null;
        }
        List<String> parts = StringUtils.PATH_SPLITTER.splitToList(path);
        if (parts.size() > 1) {
            return new ColumnIdent(parts.get(0), parts.subList(1, parts.size()));
        } else {
            return new ColumnIdent(parts.get(0));
        }
    }

    public static ColumnIdent getChild(ColumnIdent parent, String name) {
        List<String> childPath = ImmutableList.<String>builder().addAll(parent.path).add(name).build();
        return new ColumnIdent(parent.name, childPath);
    }

    /**
     * checks whether this ColumnIdent is a child of <code>parentIdent</code>
     * @param parentIdent the ident to check for parenthood
     * @return true if <code>parentIdent</code> is parentIdent of this, false otherwise.
     */
    public boolean isChildOf(ColumnIdent parentIdent) {
        boolean result = false;
        ColumnIdent parent = getParent();
        while (parent != null) {
            if (parent.equals(parentIdent)) {
                result = true;
                break;
            }
            parent = parent.getParent();
        }
        return result;
    }

    /**
     * person['addresses']['street'] --> person['addresses']
     * <p>
     * person --> null
     */
    public ColumnIdent getParent() {
        if (isColumn()) {
            return null;
        }

        if (path.size() > 1) {
            return new ColumnIdent(name(), path.subList(0, path.size() - 1));
        }
        return new ColumnIdent(name());
    }

    /**
     * returns true if this is a system column
     */
    public boolean isSystemColumn(){
        return name.startsWith("_");
    }

    /**
     * person['addresses']['street'] --> person
     * <p>
     * person --> person
     */
    public ColumnIdent getRoot() {
        if (isColumn()) {
            return this;
        }
        return new ColumnIdent(name());
    }

    public String name() {
        return name;
    }

    public String fqn() {
        if (isColumn()) {
            return name;
        }
        return StringUtils.PATH_JOINER.join(name, StringUtils.PATH_JOINER.join(path));
    }

    public String sqlFqn() {
        return StringUtils.dottedToSqlPath(fqn());
    }

    public List<String> path() {
        return path;
    }

    public boolean isColumn() {
        return path.isEmpty();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ColumnIdent o = (ColumnIdent) obj;
        return Objects.equal(name, o.name) &&
                Objects.equal(path, o.path);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, path);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("path", path)
                .toString();
    }

    @Override
    public int compareTo(ColumnIdent o) {
        return ComparisonChain.start()
                .compare(name, o.name)
                .compare(path, o.path, Ordering.<String>natural().lexicographical())
                .result();
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        int numParts = in.readVInt();
        if (numParts > 0) {
            path = new ArrayList<>(numParts);
            for (int i = 0; i < numParts; i++) {
                path.add(in.readString());
            }
        } else {
            path = ImmutableList.of();
        }

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVInt(path.size());
        for (String s : path) {
            out.writeString(s);
        }
    }
}

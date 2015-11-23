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
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.crate.core.StringUtils;
import io.crate.sql.Identifiers;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ColumnIdent implements Path, Comparable<ColumnIdent>, Streamable {

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

    private static final Ordering<Iterable<String>> ordering = Ordering.<String>natural().lexicographical();

    private String name;
    private List<String> path;

    private ColumnIdent() { }

    public static ColumnIdent fromStream(StreamInput in) throws IOException {
        ColumnIdent columnIdent = new ColumnIdent();
        columnIdent.readFrom(in);
        return columnIdent;
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
        this.path = MoreObjects.firstNonNull(path, ImmutableList.<String>of());
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
        if (parent.isColumn()) {
            return new ColumnIdent(parent.name, name);
        }
        List<String> childPath = ImmutableList.<String>builder().addAll(parent.path).add(name).build();
        return new ColumnIdent(parent.name, childPath);
    }

    /**
     * checks whether this ColumnIdent is a child of <code>parentIdent</code>
     * @param parentIdent the ident to check for parenthood
     * @return true if <code>parentIdent</code> is parentIdent of this, false otherwise.
     */
    public boolean isChildOf(ColumnIdent parentIdent) {
        if (!name.equals(parentIdent.name)) return false;
        if (path.size() > parentIdent.path.size()) {
            Iterator<String> parentIt = parentIdent.path.iterator();
            Iterator<String> it = path.iterator();
            while (parentIt.hasNext()) {
                if (!parentIt.next().equals(it.next())) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * person['addresses']['street'] --&gt; person['addresses']
     * <p>
     * person --&gt; null
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
     * creates a new columnIdent which just consists of the path of the given columnIdent
     * e.g.
     * <pre>foo['x']['y']</pre>
     * becomes
     * <pre> x['y']</pre>
     *
     * If the columnIdent doesn't have a path the return value is null
     */
    @Nullable
    public ColumnIdent shiftRight() {
        if (path.isEmpty()) {
            return null;
        }
        ColumnIdent newCi;
        if (path.size() > 1) {
            newCi = new ColumnIdent(path.get(0), path.subList(1, path.size()));
        } else {
            newCi = new ColumnIdent(path.get(0));
        }
        return newCi;
    }

    /**
     * returns true if this is a system column
     */
    public boolean isSystemColumn(){
        return name.startsWith("_");
    }

    /**
     * person['addresses']['street'] --&gt; person
     * <p>
     * person --&gt; person
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

    @Override
    public String outputName() {
        return sqlFqn();
    }

    public String sqlFqn() {
        StringBuilder sb = new StringBuilder(Identifiers.quoteIfNeeded(name));
        for (String s : path) {
            sb.append("['");
            sb.append(s);
            sb.append("']");
        }
        return sb.toString();
    }

    public List<String> path() {
        return path;
    }

    /**
     * @return true if this is a top level column, otherwise false
     */
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
        int result = name.hashCode();
        result = 31 * result + (path != null ? path.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return sqlFqn();
    }

    @Override
    public int compareTo(ColumnIdent o) {
        return ComparisonChain.start()
                .compare(name, o.name)
                .compare(path, o.path, ordering)
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

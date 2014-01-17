package io.crate.metadata;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import io.crate.sql.tree.Table;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.List;

public class TableIdent implements Comparable<TableIdent>, Streamable {

    private String schema;
    private String name;

    public static TableIdent of(Table tableNode) {
        List<String> parts = tableNode.getName().getParts();
        Preconditions.checkArgument(parts.size() < 3,
                "Table with more then 2 QualifiedName parts is not supported. only <schema>.<tableName> works.");

        if (parts.size() == 2) {
            return new TableIdent(parts.get(0), parts.get(1));
        }
        return new TableIdent(null, parts.get(1));
    }

    public TableIdent() {

    }

    public TableIdent(String schema, String name) {
        Preconditions.checkNotNull(name);
        this.schema = schema;
        this.name = name;
    }

    public String schema() {
        // return Optional<String> ?
        return schema;
    }

    public String name() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        TableIdent o = (TableIdent) obj;
        return Objects.equal(schema, o.schema) &&
                Objects.equal(name, o.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(schema, name);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("schema", schema)
                .add("name", name)
                .toString();
    }

    @Override
    public int compareTo(TableIdent o) {
        return ComparisonChain.start()
                .compare(schema, o.schema)
                .compare(name, o.name)
                .result();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        schema = in.readString();
        name = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(schema);
        out.writeString(name);
    }
}

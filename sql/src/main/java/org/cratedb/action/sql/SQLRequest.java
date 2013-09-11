package org.cratedb.action.sql;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class SQLRequest extends ActionRequest<SQLRequest> {


    private String stmt;
    private Object[] args;

    public SQLRequest(String stmt, Object[] args) {
        this.stmt = stmt;
        this.args = args;
    }

    public SQLRequest(String stmt) {
        this.stmt = stmt;
        this.args = new Object[0];
    }

    public SQLRequest() {
    }

    public String stmt() {
        return stmt;
    }

    public Object[] args() {
        return args;
    }

    public void args(Object[] args) {
        this.args = args;
    }

    public SQLRequest stmt(String stmt){
        this.stmt = stmt;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        stmt = in.readString();
        int length = in.readVInt();
        args = new Object[length];
        for (int i = 0; i < length; i++) {
            args[i] = in.readGenericValue();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(stmt);
        out.writeVInt(args.length);
        for (int i = 0; i < args.length; i++) {
            out.writeGenericValue(args[i]);
        }
    }

}

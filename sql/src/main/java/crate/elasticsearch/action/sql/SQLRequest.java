package crate.elasticsearch.action.sql;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class SQLRequest extends ActionRequest<SQLRequest> {


    private String stmt;

    public SQLRequest(String stmt) {
        this.stmt = stmt;
    }

    public SQLRequest() {
    }

    public String stmt() {
        return stmt;
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
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(stmt);
    }

}

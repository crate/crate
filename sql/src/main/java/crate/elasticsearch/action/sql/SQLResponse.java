package crate.elasticsearch.action.sql;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;

import java.io.IOException;
import java.util.Arrays;

public class SQLResponse extends ActionResponse implements ToXContent {

    static final class Fields {
        static final XContentBuilderString COLS = new XContentBuilderString("cols");
        static final XContentBuilderString ROWS = new XContentBuilderString("rows");
    }

    private Object[][] rows;
    private String[] cols;

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.array(Fields.COLS, cols);
        builder.startArray(Fields.ROWS);
        for (int i = 0; i < rows.length; i++) {
            builder.startArray();
            for (int j = 0; j < cols.length; j++) {
                builder.value(rows[i][j]);
            }
            builder.endArray();
        }
        builder.endArray();
        return builder;
    }

    public String[] cols(){
        return cols;
    }

    public void cols(String[] cols){
        this.cols = cols;
    }

    public Object[][] rows(){
        return rows;
    }

    public void rows(Object[][] rows) {
        this.rows = rows;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        cols = in.readStringArray();
        int numRows = in.readInt();
        rows = new Object[numRows][cols.length];
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < cols.length; j++) {
                rows[i][j] = in.readGenericValue();
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(cols);
        out.writeInt(rows.length);
        for (int i = 0; i < rows.length ; i++) {
            for (int j = 0; j < cols.length; j++) {
                out.writeGenericValue(rows[i][j]);
            }
        }
    }

    @Override
    public String toString() {
        return "SQLResponse{" +
                "cols=" + ((cols!=null) ? Arrays.toString(cols): null) +
                ", rows=" + ((rows!=null) ? rows.length: -1)  +
                '}';
    }
}

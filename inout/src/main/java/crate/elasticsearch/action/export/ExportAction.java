package crate.elasticsearch.action.export;

import crate.elasticsearch.client.action.export.ExportRequestBuilder;
import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;


/**
 *
 */
public class ExportAction extends Action<ExportRequest, ExportResponse, ExportRequestBuilder> {

    public static final ExportAction INSTANCE = new ExportAction();
    public static final String NAME = "el-crate-export";

    private ExportAction() {
        super(NAME);
    }

    @Override
    public ExportResponse newResponse() {
        return new ExportResponse();
    }

    @Override
    public ExportRequestBuilder newRequestBuilder(Client client) {
        return new ExportRequestBuilder(client);
    }

}

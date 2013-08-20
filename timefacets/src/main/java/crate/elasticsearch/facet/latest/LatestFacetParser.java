package crate.elasticsearch.facet.latest;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.FacetParser;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

public class LatestFacetParser extends AbstractComponent implements FacetParser {

    @Inject
    public LatestFacetParser(Settings settings) {
        super(settings);
    }

    @Override
    public String[] types() {
        return new String[] { InternalLatestFacet.TYPE };
    }

    @Override
    public FacetExecutor.Mode defaultMainMode() {
        return FacetExecutor.Mode.COLLECTOR;
    }

    @Override
    public FacetExecutor.Mode defaultGlobalMode() {
        return FacetExecutor.Mode.COLLECTOR;
    }

    @Override
    public FacetExecutor parse(String facetName, XContentParser parser,
            SearchContext context) throws IOException {
        String keyFieldName = null;
        String valueFieldName = null;
        String tsFieldName = null;
        int size = 10;
        int start = 0;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
            } else if (token.isValue()) {
                if ("key_field".equals(currentFieldName)) {
                    keyFieldName = parser.text();
                } else if ("value_field".equals(currentFieldName)) {
                    valueFieldName = parser.text();
                } else if ("ts_field".equals(currentFieldName)) {
                    tsFieldName = parser.text();
                } else if ("size".equals(currentFieldName)) {
                    size = parser.intValue();
                } else if ("start".equals(currentFieldName)) {
                    start = parser.intValue();
                }
            }
        }
        FieldMapper keyFieldMapper = context.mapperService().smartNameFieldMapper(
                keyFieldName);
        if ((keyFieldMapper != null)
                && !(keyFieldMapper.fieldDataType().getType().equals(LatestFacetExecutor.keyDataType.getType()))) {
            throw new FacetPhaseExecutionException(facetName,
                    "key field must be of type long but is "
                            + keyFieldMapper.fieldDataType().getType());
        }

        FieldMapper tsFieldMapper = context.mapperService().smartNameFieldMapper(tsFieldName);
        if ((tsFieldMapper != null)
                && !(tsFieldMapper.fieldDataType().getType().equals(LatestFacetExecutor.tsDataType.getType()))) {
            throw new FacetPhaseExecutionException(facetName,
                    "ts field must be of type long but is "
                            + tsFieldMapper.fieldDataType().getType());
        }

        FieldMapper valueFieldMapper = context.mapperService().smartNameFieldMapper(valueFieldName);
        if (valueFieldMapper.fieldDataType().getType().equals("int") || valueFieldMapper.fieldDataType().getType().equals("long")) {
            IndexNumericFieldData valueFieldData = context.fieldData().getForField(valueFieldMapper);
            IndexNumericFieldData keyFieldData = context.fieldData().getForField(keyFieldMapper);
            IndexNumericFieldData tsFieldData = context.fieldData().getForField(tsFieldMapper);

            return new LatestFacetExecutor(keyFieldData, valueFieldData, tsFieldData, size, start);
        } else {
            throw new FacetPhaseExecutionException(facetName, "value field  is not of type int or long");
        }


    }

}

package crate.elasticsearch.action.import_.parser;

import org.elasticsearch.common.xcontent.XContentParser;

import crate.elasticsearch.action.import_.ImportContext;

public interface ImportParseElement {

    void parse(XContentParser parser, ImportContext context) throws Exception;

}

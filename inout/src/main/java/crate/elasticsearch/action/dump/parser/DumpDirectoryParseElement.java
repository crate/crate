package crate.elasticsearch.action.dump.parser;

import crate.elasticsearch.action.export.ExportContext;
import crate.elasticsearch.action.import_.ImportContext;
import crate.elasticsearch.action.import_.parser.ImportParseElement;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

import java.io.File;


/**
 * Parser element class to parse a given 'directory' option to the _dump endpoint
 */
public class DumpDirectoryParseElement implements SearchParseElement {


    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token.isValue()) {
            setOutPutFile((ExportContext) context, parser.text());
        }
    }

    /**
     * Set the constant filename_pattern prefixed with a target directory as output_file to the context
     *
     * @param context
     * @param directory
     */
    public void setOutPutFile(ExportContext context, String directory) {
        File dir = new File(directory);
        File file = new File(dir, DumpParser.FILENAME_PATTERN);
        context.outputFile(file.getPath());
    }

}

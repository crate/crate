package io.crate.expression.scalar.regex;

import static io.crate.expression.RegexpFlags.isGlobal;
import static io.crate.expression.RegexpFlags.parseFlags;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.util.automaton.RegExp;

import io.crate.data.Input;
import io.crate.expression.RegexpFlags;
import java.util.regex.PatternSyntaxException;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.symbol.Literal;
import io.crate.lucene.match.CrateRegexQuery;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

public class RegexpMatchScalar extends Scalar<List<String>, String> {
    private Pattern pattern;
    public static final String NAME = "regexp_match";

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING_ARRAY.getTypeSignature()
            ),
            RegexpMatchScalar::new
        );

        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING_ARRAY.getTypeSignature()
            ),
            RegexpMatchScalar::new
        );
    }

    public RegexpMatchScalar(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    public RegexpMatchScalar(Signature signature, BoundSignature boundSignature, String patternString, int flags) throws PatternSyntaxException {
        super(signature, boundSignature);
        compilePattern(patternString, flags);
    }

    private void compilePattern(String patternString, int flags) throws PatternSyntaxException {
        this.pattern = Pattern.compile(patternString, flags);
    }


    @Override
    public List<String> evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<String>[] args) throws IllegalArgumentException{
        assert args.length == 2 || args.length == 3 : "number of args must be 2 or 3";
        String source = args[0].value();
        if (source == null) {
            return null;
        }

        String pattern = args[1].value();
        if (pattern == null) {
            return null;
        }

        String flags = null;

        if (args.length == 3) {
            flags = args[2].value();
            if (!isGlobal(flags)) {
                pattern = Pattern.compile(pattern, parseFlags(flags)).pattern();
            } else{
                throw new IllegalArgumentException("Unsupported flag: 'g'. Global flag is not supported.");
            }
        }

        List<String> matches = new ArrayList<>();
        java.util.regex.Matcher matcher = java.util.regex.Pattern.compile(pattern).matcher(source);
        while (matcher.find()) {
            matches.add(matcher.group());
        }
        return matches;
    }

    @Override
    public Query toQuery(Reference ref, Literal<?> literal) throws IllegalArgumentException {
        String pattern = (String) literal.value();
        int flags = 0;

        // Check if the pattern contains flags
        String flagsStr = null;
        int flagSeparatorIndex = pattern.lastIndexOf('/');
        if (flagSeparatorIndex != -1 && flagSeparatorIndex < pattern.length() - 1) {
            flagsStr = pattern.substring(flagSeparatorIndex + 1);
            pattern = pattern.substring(0, flagSeparatorIndex);
            flags = parseFlags(flagsStr);
        }

        Term term = new Term(ref.storageIdent(), pattern);
        if (RegexpFlags.isPcrePattern(pattern)) {
            if (isGlobal(flagsStr)) {
                throw new IllegalArgumentException("Unsupported flag: 'g'. Global flag is not supported.");
            } else {
                return new CrateRegexQuery(term, flags);
            }
        } else {
            return new ConstantScoreQuery(new RegexpQuery(term, RegExp.ALL));
        }
    }

}

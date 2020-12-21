.. _sql-analyzer:

==================
Fulltext analyzers
==================

.. rubric:: Table of contents

.. contents::
   :local:

.. _analyzer-overview:

Overview
========

Analyzers are used for creating fulltext-indexes. They take the content of a
field and split it into tokens, which are then searched. Analyzers filter,
reorder and/or transform the content of a field before it becomes the final
stream of tokens.

An analyzer consists of one tokenizer, zero or more token-filters, and zero or
more char-filters.

When a field-content is analyzed to become a stream of tokens, the char-filter
is applied at first. It is used to filter some special chars from the stream of
characters that make up the content.

Tokenizers take a possibly filtered stream of characters and split it into a
stream of tokens.

Token-filters can add tokens, delete tokens or transform them.

With these elements in place, analyzer provide fine-grained control over
building a token stream used for fulltext search. For example you can use
language specific analyzers, tokenizers and token-filters to get proper search
results for data provided in a certain language.

Below the builtin analyzers, tokenizers, token-filters and char-filters are
listed. They can be used as is or can be extended.

.. SEEALSO::

  :ref:`fulltext-indices` for examples showing how to create tables which make
  use of analyzers.

  :ref:`create_custom_analyzer` for an example showing how to create a custom
  analyzer.

  :ref:`ref-create-analyzer` for the syntax reference.

.. _builtin-analyzer:

Built-in analyzers
==================

.. _standard-analyzer:

``standard``
------------

``type='standard'``

An analyzer of type standard is built using the :ref:`standard-tokenizer`
Tokenizer with the :ref:`standard-tokenfilter` Token Filter,
:ref:`lowercase-tokenfilter` Token Filter, and :ref:`stop-tokenfilter` Token
Filter.

Lowercase all Tokens, uses *NO* stopwords and excludes tokens longer than 255
characters. This analyzer uses unicode text segmentation, which is defined by
`UAX#29`_.

For example, the standard analyzer converts the sentence

::

    The quick brown fox jumps Over the lAzY DOG.

into the following tokens

::

    quick, brown, fox, jumps, lazy, dog

.. rubric:: Parameters

stopwords
    A list of stopwords to initialize the :ref:`stop-tokenfilter` filter with.
    Defaults to the english stop words.

max_token_length
    The maximum token length. If a token exceeds this length it is split in
    max_token_length chunks. Defaults to ``255``.

.. _default-analyzer:

``default``
-----------

``type='default'``

This is the same as the `standard-analyzer`_ analyzer.

.. _simple-analyzer:

``simple``
----------

``type='simple'``

Uses the :ref:`lowercase-tokenizer` tokenizer.

.. _plain-analyzer:

``plain``
----------

``type='plain'``

The plain analyzer is an alias for the keyword_ analyzer and cannot be extended.
You must extend the keyword_ analyzer instead.

.. _whitespace-analyzer:

``whitespace``
--------------

``type='whitespace'``

Uses a :ref:`whitespace-tokenizer` tokenizer

.. _stop-analyzer:

``stop``
--------

``type='stop'``

Uses a :ref:`lowercase-tokenizer` tokenizer, with :ref:`stop-tokenfilter` Token
Filter.

.. rubric:: Parameters

stopwords
    A list of stopwords to initialize the :ref:'stop-tokenfilter` filter with.
    Defaults to the english stop words.

stopwords_path
    A path (either relative to config location, or absolute) to a stopwords
    file configuration.

.. _keyword-analyzer:

``keyword``
-----------

``type='keyword'``

Creates one single token from the field-contents.

.. _pattern-analyzer:

``pattern``
-----------

``type='pattern'``

An analyzer of type pattern that can flexibly separate text into terms via a
regular expression.

.. rubric:: Parameters

lowercase
    Should terms be lowercased or not. Defaults to true.

pattern
    The regular expression pattern, defaults to \W+.

flags
    The regular expression flags.

.. NOTE::

   The regular expression should match the token separators, not the tokens
   themselves.

Flags should be pipe-separated, eg ``CASE_INSENSITIVE|COMMENTS``. Check `Java
Pattern API`_ for more details about flags options.

.. _language-analyzer:

``language``
------------

``type='<language-name>'``

The following types are supported:

``arabic``, ``armenian``, ``basque``, ``brazilian``, ``bengali``,
``bulgarian``, ``catalan``, ``chinese``, ``cjk``, ``czech``, ``danish``,
``dutch``, ``english``, ``finnish``, ``french``, ``galician``, ``german``,
``greek``, ``hindi``, ``hungarian``, ``indonesian``, ``italian``,  ``latvian``,
``lithuanian``, ``norwegian``, ``persian``, ``portuguese``, ``romanian``,
``russian``, ``sorani``, ``spanish``, ``swedish``, ``turkish``, ``thai``.

.. rubric:: Parameters

stopwords
    A list of stopwords to initialize the stop filter with. Defaults to the
    english stop words.

stopwords_path
    A path (either relative to config location, or absolute) to a stopwords
    file configuration.

stem_exclusion
    The stem_exclusion parameter allows you to specify an array of lowercase words
    that should not be stemmed. The following analyzers support setting
    stem_exclusion:
    ``arabic``, ``armenian``, ``basque``, ``brazilian``, ``bengali``,
    ``bulgarian``, ``catalan``, ``czech``, ``danish``, ``dutch``, ``english``,
    ``finnish``, ``french``, ``galician``, ``german``, ``hindi``, ``hungarian``,
    ``indonesian``, ``italian``, ``latvian``, ``lithuanian``, ``norwegian``,
    ``portuguese``, ``romanian``, ``russian``, ``spanish``, ``swedish``,
    ``turkish``.

.. _snowball-analyzer:

``snowball``
------------

``type='snowball'``

Uses the :ref:`standard-tokenizer` tokenizer, with :ref:`standard-tokenfilter`
filter, :ref:`lowercase-tokenfilter` filter, :ref:`stop-tokenfilter` filter,
and :ref:`snowball-tokenfilter` filter.

.. rubric:: Parameters

stopwords
    A list of stopwords to initialize the stop filter with. Defaults to the
    english stop words.

language
    See the language-parameter of :ref:`snowball-tokenfilter`.

.. _fingerprint-analyzer:

``fingerprint``
---------------

``type='fingerprint'``

The fingerprint analyzer implements a fingerprinting algorithm which is used by
the OpenRefine project to assist in clustering. Input text is lowercased,
normalized to remove extended characters, sorted, deduplicated and concatenated
into a single token. If a stopword list is configured, stop words will also be
removed. It uses the :ref:`standard-tokenizer` tokenizer and the following
filters: :ref:`lowercase-tokenfilter`, :ref:`asciifolding-tokenfilter`,
:ref:`fingerprint-tokenfilter` and ref:`stop-tokenfilter`.

.. rubric:: Parameters

separator
    The character to use to concatenate the terms. Defaults to a space.

max_output_size
    The maximum token size to emit, tokens larger than this size will be
    discarded. Defaults to ``255``.

stopwords
    A pre-defined stop words list like _english_ or an array containing a list
    of stop words. Defaults to ``\_none_``.

stopwords_path
    The path to a file containing stop words.

.. _builtin-tokenizer:

Built-in tokenizers
===================

.. _standard-tokenizer:

Standard tokenizer
------------------

``type='standard'``

The tokenizer of type ``standard`` is providing a grammar based tokenizer,
which is a good tokenizer for most European language documents. The tokenizer
implements the Unicode Text Segmentation algorithm, as specified in Unicode
Standard Annex #29.

.. rubric:: Parameters

max_token_length
    The maximum token length. If a token exceeds this length it is split in
    max_token_length chunks. Defaults to ``255``.

.. _classic-tokenizer:

Classic tokenizer
-----------------

``type='classic'``

The ``classic`` tokenizer is a grammar based tokenizer that is good for English
language documents. This tokenizer has heuristics for special treatment of
acronyms, company names, email addresses, and internet host names. However,
these rules don't always work, and the tokenizer doesn't work well for most
languages other than English.

.. rubric:: Parameters

max_token_length
    The maximum token length. If a token exceeds this length it is split in
    max_token_length chunks. Defaults to ``255``.

.. _thai-tokenizer:

Thai tokenizer
--------------

``type='thai'``

The ``thai`` tokenizer splits Thai text correctly, treats all other languages
like the `standard-tokenizer`_ does.

.. _letter-tokenizer:

Letter tokenizer
----------------

``type='letter'``

The ``letter`` tokenzier splits text at non-letters.

.. _lowercase-tokenizer:

Lowercase tokenizer
-------------------

``type='lowercase'``

The ``lowercase`` tokenizer performs the function of :ref:`letter-tokenizer`
and :ref:`lowercase-tokenfilter` together. It divides text at non-letters and
converts them to lower case.

.. _whitespace-tokenizer:

Whitespace tokenizer
--------------------

``type='whitespace'``

The ``whitespace`` tokenizer splits text at whitespace.

.. rubric:: Parameters

max_token_length
    The maximum token length. If a token exceeds this length it is split in
    max_token_length chunks. Defaults to ``255``.

.. _uaxemailurl-tokenizer:

UAX URL email tokenizer
-----------------------

``type='uax_url_email'``

The ``uax_url_email`` tokenizer behaves like the :ref:`standard-tokenizer`, but
tokenizes emails and urls as single tokens.

.. rubric:: Parameters

max_token_length
    The maximum token length. If a token exceeds this length it is split in
    max_token_length chunks. Defaults to ``255``.

.. _ngram-tokenizer:

N-gram tokenizer
----------------

``type='ngram'``

.. rubric:: Parameters

min_gram
    Minimum length of characters in a gram. default: 1.

max_gram
    Maximum length of characters in a gram. default: 2.

token_chars
    Characters classes to keep in the tokens, will split on characters that
    don't belong to any of these classes. default: [] (Keep all characters).

    **Classes:** letter, digit, whitespace, punctuation, symbol

.. _edgengram-tokenizer:

Edge n-gram tokenizer
---------------------

``type='edge_ngram'``

The ``edge_ngram`` tokenizer is very similar to :ref:`ngram-tokenizer` but only
keeps n-grams which start at the beginning of a token.

.. rubric:: Parameters

min_gram
    Minimum length of characters in a gram. default: 1

max_gram
    Maximum length of characters in a gram. default: 2

token_chars
    Characters classes to keep in the tokens, will split on characters that
    don't belong to any of these classes. default: [] (Keep all characters).

    **Classes:** letter, digit, whitespace, punctuation, symbol

.. _keyword-tokenizer:

Keyword tokenizer
-----------------

``type='keyword'``

The ``keyworkd`` tokenizer emits the entire input as a single token.

.. rubric:: Parameters

buffer_size
    The term buffer size. Defaults to ``256``.

.. _pattern-tokenizer:

Pattern tokenizer
-----------------

``type='pattern'``

The ``pattern`` tokenizer separates text into terms via a regular expression.

.. rubric:: Parameters

pattern
    The regular expression pattern, defaults to \\W+.

flags
    The regular expression flags.

group
    Which group to extract into tokens. Defaults to -1 (split).

.. NOTE::

   The regular expression should match the token separators, not the tokens
   themselves.

Flags should be pipe-separated, eg ``CASE_INSENSITIVE|COMMENTS``. Check `Java
Pattern API`_ for more details about flags options.

.. _simple_pattern-tokenizer:

Simple pattern tokenizer
------------------------

``type='simple_pattern'``

Similar to the ``pattern`` tokenizer, this tokenizer uses a regular expression
to split matching text into terms, however with a limited, more restrictive
subset of expressions. This is in general faster than the normal ``pattern``
tokenizer, but does not support splitting on pattern.

.. rubric:: Parameters

pattern
    A `Lucene regular expression`_, defaults to empty string.

.. _simple_pattern_split-tokenizer:

Simple pattern split tokenizer
------------------------------

``type='simple_patten_split'``

The ``simple_pattern_split`` tokenizer operates with the same restricted subset
of regular expressions as the ``simple_pattern`` tokenizer, but it splits the
input on the pattern, rather than the matching pattern.

.. rubric:: Parameters

pattern
    A `Lucene regular expression`_, defaults to empty string.

.. _pathhierarchy-tokenizer:

Path hierarchy tokenizer
------------------------

``type='path_hierarchy'``

Takes something like this::

    /something/something/else

And produces tokens::

    /something
    /something/something
    /something/something/else

.. rubric:: Parameters

delimiter
    The character delimiter to use, defaults to /.

replacement
    An optional replacement character to use. Defaults to the delimiter.

buffer_size
    The buffer size to use, defaults to 1024.

reverse
    Generates tokens in reverse order, defaults to false.

skip
    Controls initial tokens to skip, defaults to 0.

.. _analyzers_char_group:

Char group tokenizer
--------------------

``type=char_group``

Breaks text into terms whenever it encounters a character that is part of a
predefined set.

.. rubric:: Parameters

tokenize_on_chars
    A list containing characters to tokenize on.


.. _builtin-token-filter:

Built-in token filters
======================

.. _standard-tokenfilter:

``standard``
------------

``type='standard'``

Normalizes tokens extracted with the :ref:`standard-tokenizer` tokenizer.

.. _classic-tokenfilter:

``classic``
-----------

``type='classic'``

Does optional post-processing of terms that are generated by the classic
tokenizer. It removes the english possessive from the end of words, and it
removes dots from acronyms.

.. _apostrophe-tokenfilter:

``apostrophe``
--------------

``type='apostrophe'``

Strips all characters after an apostrophe, and the apostrophe itself.

.. _asciifolding-tokenfilter:

``asciifolding``
----------------

``type='asciifolding'``

Converts alphabetic, numeric, and symbolic Unicode characters which are not in
the first 127 ASCII characters (the "Basic Latin" Unicode block) into their
ASCII equivalents, if one exists.

.. _length-tokenfilter:

``length``
----------

``type='length'``

Removes words that are too long or too short for the stream.

.. rubric:: Parameters

min
    The minimum number. Defaults to 0.

max
    The maximum number. Defaults to Integer.MAX_VALUE.

.. _lowercase-tokenfilter:

``lowercase``
-------------

``type='lowercase'``

Normalizes token text to lower case.

.. rubric:: Parameters

language
    For options, see :ref:`language-analyzer` Analyzer.

.. _ngram-tokenfilter:

``ngram``
---------

``type='ngram'``

.. rubric:: Parameters

min_gram
    Defaults to 1.

max_gram
    Defaults to 2.

.. _edgengram-tokenfilter:

``edge_ngram``
--------------

``type='edge_ngram'``

.. rubric:: Parameters

min_gram
    Defaults to 1.

max_gram
    Defaults to 2.

side
    Either front or back. Defaults to front.

.. _porterstem-tokenfilter:

``porter_stem``
---------------

``type='porter_stem'``

Transforms the token stream as per the Porter stemming algorithm.

.. NOTE::

    The input to the stemming filter must already be in lower case, so you will
    need to use Lower Case Token Filter or Lower Case tokenizer farther down
    the tokenizer chain in order for this to work properly! For example, when
    using custom analyzer, make sure the lowercase filter comes before the
    porterStem filter in the list of filters.

.. _shingle-tokenfilter:

``shingle``
-----------

``type='shingle'``

Constructs shingles (token n-grams), combinations of tokens as a single token,
from a token stream.

.. rubric:: Parameters

max_shingle_size
    The maximum shingle size. Defaults to 2.

min_shingle_sizes
    The minimum shingle size. Defaults to 2.

output_unigrams
    If true the output will contain the input tokens (unigrams) as well as the
    shingles. Defaults to true.

output_unigrams_if_no_shingles
    If output_unigrams is false the output will contain the input tokens
    (unigrams) if no shingles are available. Note if output_unigrams is set to
    true this setting has no effect. Defaults to false.

token_separator
    The string to use when joining adjacent tokens to form a shingle. Defaults
    to " ".

.. _stop-tokenfilter:

``stop``
--------

``type='stop'``

Removes stop words from token streams.

.. rubric:: Parameters

stopwords
    A list of stop words to use. Defaults to english stop words.

stopwords_path
    A path (either relative to config location, or absolute) to a stopwords
    file configuration. Each stop word should be in its own "line" (separated
    by a line break). The file must be UTF-8 encoded.

ignore_case
    Set to true to lower case all words first. Defaults to false.

remove_trailing
    Set to false in order to not ignore the last term of a search if it is a
    stop word. Defaults to true

.. _worddelimiter-tokenfilter:

``word_delimiter``
------------------

``type='word_delimiter'``

Splits words into subwords and performs optional transformations on subword
groups.

.. rubric:: Parameters

generate_word_parts
    If true causes parts of words to be generated: "PowerShot" ⇒ "Power"
    "Shot". Defaults to true.

generate_number_parts
    If true causes number subwords to be generated: "500-42" ⇒ "500" "42".
    Defaults to true.

catenate_words
    If true causes maximum runs of word parts to be catenated: "wi-fi" ⇒
    "wifi". Defaults to false.

catenate_numbers
    If true causes maximum runs of number parts to be catenated: "500-42" ⇒
    "50042". Defaults to false.

catenate_all
    If true causes all subword parts to be catenated: "wi-fi-4000" ⇒
    "wifi4000". Defaults to false.

split_on_case_change
    If true causes "PowerShot" to be two tokens; ("Power-Shot" remains two
    parts regards). Defaults to true.

preserve_original
    If true includes original words in subwords: "500-42" ⇒ "500-42" "500"
    "42". Defaults to false.

split_on_numerics
    If true causes "j2se" to be three tokens; "j" "2" "se". Defaults to true.

stem_english_possessive
    If true causes trailing "'s" to be removed for each subword: "O'Neil's" ⇒
    "O", "Neil". Defaults to true.

protected_words
    A list of words protected from being delimiter.

protected_words_path
    A relative or absolute path to a file configured with protected words (one
    on each line). If relative, automatically resolves to ``config/`` based
    location if exists.

type_table
    A custom type mapping table

.. _stemmer-tokenfilter:

``stemmer``
-----------

``type='stemmer'``

A filter that stems words (similar to :ref:`snowball-tokenfilter`, but with
more options).

.. rubric:: Parameters

language/name
    arabic, armenian, basque, brazilian, bulgarian, catalan, czech, danish,
    dutch, english, finnish, french, german, german2, greek, hungarian,
    italian, kp, kstem, lovins, latvian, norwegian, minimal_norwegian, porter,
    portuguese, romanian, russian, spanish, swedish, turkish, minimal_english,
    possessive_english, light_finnish, light_french, minimal_french,
    light_german, minimal_german, hindi, light_hungarian, indonesian,
    light_italian, light_portuguese, minimal_portuguese, portuguese,
    light_russian, light_spanish, light_swedish.

.. _keywordmarker-tokenfilter:

``keyword_marker``
------------------

``type='keyword_marker'``

Protects words from being modified by stemmers. Must be placed before any
stemming filters.

.. rubric:: Parameters

keywords
    A list of words to use.

keywords_path
    A path (either relative to config location, or absolute) to a list of
    words.

ignore_case
    Set to true to lower case all words first. Defaults to false.

.. _kstem-tokenfilter:

``kstem``
---------

``type='kstem'``

High performance filter for english.

All terms must already be lowercased (use :ref:`lowercase-tokenfilter` filter)
for this filter to work correctly.

.. _snowball-tokenfilter:

``snowball``
------------

``type='snowball'``

A filter that stems words using a Snowball-generated stemmer.

.. rubric:: Parameters

language
    Possible values: Armenian, Basque, Catalan, Danish, Dutch, English,
    Finnish, French, German, German2, Hungarian, Italian, Kp, Lovins,
    Norwegian, Porter, Portuguese, Romanian, Russian, Spanish, Swedish,
    Turkish.

.. _synonym-tokenfilter:

``synonym``
-----------

``type='synonym'``

Allows to easily handle synonyms during the analysis process. Synonyms are
configured using a configuration file.

.. rubric:: Parameters

synonyms_path
    Path to synonyms configuration file

ignore_case
    Defaults to ``false``

expand
    Defaults to ``true``

.. _compoundword-tokenfilter:

``*_decompounder``
------------------

``type='dictionary_decompounder'`` or ``type='hyphenation_decompounder'``

Decomposes compound words.

.. rubric:: Parameters

word_list
    A list of words to use.

word_list_path
    A path (either relative to config location, or absolute) to a list of
    words.

min_word_size
    Minimum word size(Integer). Defaults to 5.

min_subword_size
    Minimum subword size(Integer). Defaults to 2.

max_subword_size
    Maximum subword size(Integer). Defaults to 15.

only_longest_match
    Only matching the longest(Boolean). Defaults to false

.. _reverse-tokenfilter:

``reverse``
-----------

``type='reverse'``

Reverses each token.

.. _elision-tokenfilter:

``elision``
-----------

``type='elision'``

Removes elisions.

.. rubric:: Parameters

articles
    A set of stop words articles, for example ``['j', 'l']`` for content like
    ``J'aime l'odeur.``

.. _truncate-tokenfilter:

``truncate``
------------

``type='truncate'``

Truncates tokens to a specific length.

.. rubric:: Parameters

length
    Number of characters to truncate to. default 10

.. _unique-tokenfilter:

``unique``
----------

``type='unique'``

Used to only index unique tokens during analysis. By default it is applied on
all the token stream.

.. rubric:: Parameters

only_on_same_position
    If set to true, it will only remove duplicate tokens on the same position.

.. _patterncapture-tokenfilter:

``pattern_capture``
-------------------

``type='pattern_capture'``

Emits a token for every capture group in the regular expression

.. rubric:: Parameters

preserve_original
    If set to true (the default) then it would also emit the original token

.. _patternreplace-tokenfilter:

``pattern_replace``
-------------------

``type='pattern_replace'``

Handle string replacements based on a regular expression.

.. rubric:: Parameters

pattern
    Regular expression whose matches will be replaced.

replacement
    The replacement, can reference the original text with ``$1``-like (the
    first matched group) references.

.. _trim-tokenfilter:

``trim``
--------

``type='trim'``

Trims the whitespace surrounding a token.

.. _limittokencount-tokenfilter:

``limit``
---------

``type='limit'``

Limits the number of tokens that are indexed per document and field.

.. rubric:: Parameters

max_token_count
    The maximum number of tokens that should be indexed per document and field.
    The default is 1

consume_all_tokens
    If set to true the filter exhaust the stream even if max_token_count tokens
    have been consumed already. The default is false.

.. _hunspell-tokenfilter:

``hunspell``
------------

``type='hunspell'``

Basic support for Hunspell stemming. Hunspell dictionaries will be picked up
from the dedicated directory ``<path.conf>/hunspell``. Each dictionary is
expected to have its own directory named after its associated locale
(language). This dictionary directory is expected to hold both the \*.aff and
\*.dic files (all of which will automatically be picked up).

.. rubric:: Parameters

ignore_case
    If true, dictionary matching will be case insensitive (defaults to false)

strict_affix_parsing
    Determines whether errors while reading a affix rules file will cause
    exception or simply be ignored (defaults to true)

locale
    A locale for this filter. If this is unset, the lang or language are used
    instead - so one of these has to be set.

dictionary
    The name of a dictionary contained in ``<path.conf>/hunspell``.

dedup
    If only unique terms should be returned, this needs to be set to true.
    Defaults to true.

recursion_level
    Configures the recursion level a stemmer can go into. Defaults to 2. Some
    languages (for example czech) give better results when set to 1 or 0, so
    you should test it out.

.. _commongrams-tokenfilter:

``common_grams``
----------------

``type='common_grams'``

Generates bigrams for frequently occuring terms. Single terms are still
indexed. It can be used as an alternative to the :ref:`stop-tokenfilter` Token
filter when we don't want to completely ignore common terms.

.. rubric:: Parameters

common_words
    A list of common words to use.

common_words_path
    A path (either relative to config location, or absolute) to a list of
    common words. Each word should be in its own "line" (separated by a line
    break). The file must be UTF-8 encoded.

ignore_case
    If true, common words matching will be case insensitive (defaults to
    false).

query_mode
    Generates bigrams then removes common words and single terms followed by a
    common word (defaults to false).

.. NOTE::

    Either ``common_words`` or ``common_words_path`` must be given.

.. _normalization-tokenfilter:

``*_normalization``
-------------------

``type='<language>_normalization'``

Normalizes special characters of several languages.

Available languages:

* arabic
* bengali
* german
* hindi
* indic
* persian
* scandinavian
* serbian
* sorani

.. _scandinavian-folding-tokenfilter:

``scandinavian_folding``
------------------------

``type='scandinavian_folding'``

*Folds* scandinavian characters like ``ø`` to ``o`` or ``å`` to ``a``.

Though this might result in different words, it is easier to match different
scandinavian languages using this folding algorithm.

.. _delimited_payload-tokenfilter:

``delimited_payload``
---------------------

``type='delimited_payload'``

Split tokens up by delimiter (default ``|``) into the real token being indexed
and the payload stored additionally into the index. For example
``Trillian|65535`` will be indexed as ``Trillian`` with ``65535`` as payload.

.. rubric:: Parameters

encoding
    How the payload should be interpreted, possible values are ``real`` for
    float values, ``integer`` for integer values and ``identity`` for keeping the
    payload as byte array (string).

delimiter
    The string used to separate the token and its payload.

.. _keep-tokenfilter:

``keep``
--------

``type='keep'``

Only keep tokens defined within the settings of this filter ``keep_words`` and
variations.

All other tokens will be filtered. This filter works like an inverse
`stop-tokenfilter`_ filter.

.. rubric:: Parameters

keep_words
    A list of words to keep and index as tokens.

keep_words_path
    A path (either relative to config location, or absolute) to a list of words
    to keep and index.

    Each word should be in its own "line" (separated by a line break). The file
    must be UTF-8 encoded.

.. _stemmer_override-tokenfilter:

``stemmer_override``
--------------------

``type='stemmer_override'``

Override any previous stemmer that recognizes keywords with a custom mapping,
defined by ``rules`` or ``rules_path``. One of these settings has to be set.

.. rubric:: Parameters

rules
    A list of rules for overriding, in the form of ``[<source>=><replacement>]
    e.g. "foo=>bar"``

rules_path
    A path to a file with one rule per line, like above.

.. _cjk_bigram-tokenfilter:

``cjk_bigram``
--------------

``type='cjk_bigram'``

Handle Chinese, Japanese and Korean (CJK) bigrams.

.. rubric:: Parameters

output_bigrams
    Boolean flag to enable a combined unigram+bigram approach.

    Default is ``false``, so single CJK characters that do not form a bigram
    are passed as unigrams.

    All non CJK characters are output unmodified.

ignored_scripts
    Scripts to ignore. possible values: ``han``, ``hiragana``, ``katakana``,
    ``hangul``

.. cjk_width-tokenfilter:

``cjk_width``
-------------

``type='cjk_width'``

A filter that normalizes CJK.

.. language_stem-tokenfilter:

``*_stem``
----------

| ``type='arabic_stem'`` or
| ``type='brazilian_stem'`` or
| ``type='czech_stem'`` or
| ``type='dutch_stem'`` or
| ``type='french_stem'`` or
| ``type='german_stem'`` or
| ``type='russian_stem'``

A group of filters that applies language specific stemmers to the token stream.
To prevent terms from being stemmed put a `keywordmarker-tokenfilter`_ before
this filter into the ``token_filter`` chain.

``decimal_digit``
-----------------

A token filter that folds unicode digits to ``0-9``

.. _analyzers_remove_duplicates:

``remove_duplicates``
---------------------

A token filter that drops identical tokens at the same position.

.. _phonetic-tokenfilter:

``phonetic``
--------------

A token filter which converts tokens to their phonetic representation using
Soundex, Metaphone, and a variety of other algorithms.

.. rubric:: Parameters

encoder
    Which phonetic encoder to use. Accepts ``metaphone`` (default),
    ``double_metaphone``, ``soundex``, ``refined_soundex``, ``caverphone1``,
    ``caverphone2``, ``cologne``, ``nysiis``, ``koelnerphonetik``,
    ``haasephonetik``, ``beider_morse``, ``daitch_mokotoff``.

replace
    Whether or not the original token should be replaced by the phonetic
    token. Accepts ``true`` (default) and ``false``. Not supported by
    ``beider_morse`` encoding.

.. Note::

   Be aware that ``replace: false`` can lead to unexpected behavior since the
   original and the phonetically analyzed version are both kept at the same
   token position. Some queries handle these stacked tokens in special ways. For
   example, the :ref:`fuzzy match query <match_option_fuzziness>` does not apply
   fuzziness to stacked synonym tokens. This can lead to issues that are
   difficult to diagnose and reason about. For this reason, it is often
   beneficial to use separate fields for analysis with and without phonetic
   filtering. That way searches can be run against both fields with differing
   boosts and trade-offs (e.g. only run a fuzzy match query on the original text
   field, but not on the phonetic version).

``double_metaphone``
~~~~~~~~~~~~~~~~~~~~

If the ``double_metaphone`` encoder is used, then this additional parameter is supported:

.. rubric:: Parameters

max_code_len
    The maximum length of the emitted metaphone token. Defaults to ``4``.

``beider_morse``
~~~~~~~~~~~~~~~~

If the ``beider_morse`` encoder is used, then these additional parameters are supported:

.. rubric:: Parameters

rule_type
    Whether matching should be ``exact`` or ``approx`` (default).

name_type
    Whether names are ``ashkenazi``, ``sephardic``, or ``generic`` (default).

languageset
    An array of languages to check. If not specified, then the language will be
    guessed. Accepts: ``any``, ``common``, ``cyrillic``, ``english``,
    ``french``, ``german``, ``hebrew``, ``hungarian``, ``polish``, ``romanian``,
    ``russian``, ``spanish``.

.. _builtin-char-filter:

Built-in char filter
====================

.. _mapping-charfilter:

``mapping``
-----------

``type='mapping'``

.. rubric:: Parameters

mappings
    A list of mappings as strings of the form ``[<source>=><replacement>] e.g.
    "ph=>f"``

mappings_path
    A path to a file with one mapping per line, like above.

.. _htmlstrip-charfilter:

``html_strip``
--------------

``type='html_strip'``

Strips out HTML elements from an analyzed text.

.. _patternreplace-charfilter:

``pattern_replace``
-------------------

``type='pattern_replace'``

Manipulates the characters in a string before analysis with a regex.

.. rubric:: Parameters

pattern
    Regex whose matches will be replaced

replacement
    Replacement string, can reference replaced text by ``$1`` like references
    (first matched element)

.. _keeptypes-tokenfilter:

``keep_types``
--------------

``type='keep_types'``

Keeps only the tokens with a token type contained in a predefined set.

.. rubric:: Parameters

types
    A list of token types to keep.

.. _minhash-tokenfilter:

``min_hash``
------------

``type='min_hash'``

Hashes each token of the token stream and divides the resulting hashes into
buckets, keeping the lowest-valued hashes per bucket. It then returns these
hashes as tokens.

.. rubric:: Parameters

hash_count
    The number of hashes to hash the token stream with. Defaults to ``1``.

bucket_count
    The number of buckets to divide the minhashes into. Defaults to ``512``.

hash_set_size
    The number of minhashes to keep per bucket. Defaults to ``1``.

with_rotation
    Whether or not to fill empty buckets with the value of the first non-empty
    bucket to its circular right. Only takes effect if hash_set_size is equal
    to one. Defaults to ``true`` if bucket_count is greater than ``1``, else
    ``false``.

.. _fingerprint-tokenfilter:

``fingerprint``
---------------

``type='fingerprint'``

 Emits a single token which is useful for fingerprinting a body of text, and/or
 providing a token that can be clustered on. It does this by sorting the
 tokens, deduplicating and then concatenating them back into a single token.

.. rubric:: Parameters

separator
    Separator which is used for concatenating the tokens. Defaults to a space.

max_output_size
    If the concatenated fingerprint grows larger than max_output_size, the
    token filter will exit and will not emit a token. Defaults to ``255``.

.. _Java Pattern Api: https://download.oracle.com/javase/6/docs/api/java/util/regex/Pattern.html#field_summary
.. _`UAX#29`: https://www.unicode.org/reports/tr29/
.. _Lucene regular expression: https://lucene.apache.org/core/7_0_1/core/org/apache/lucene/util/automaton/RegExp.html

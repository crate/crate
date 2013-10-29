CREATE ANALYZER a5 EXTENDS a4 WITH (
    key='value',
    "another_key"=123,
    CHAR_FILTERS WITH (
        html_strip
    ),
    TOKENIZER tok WITH (
        tokkey='tokvalue'
    )
)
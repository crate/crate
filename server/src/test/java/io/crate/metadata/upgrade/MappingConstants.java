/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.metadata.upgrade;

public class MappingConstants {

    /*
    5.3 mapping for the table created with statement

    create table novels (
        title string,
        description string,
        author object as (
            name string, birthday timestamp with time zone
        ),
        index title_desc_fulltext using fulltext(title, description),
        index nested_col_fulltext using fulltext(title, author[ 'name' ]) with(analyzer = 'stop')
    )

    Has a column with multiple copy_to entries (column is part of many indices, in the new mapping has to be referenced in different sources)
    Has a sub-column with copy_to, sources should be able to reference deep columns (must be FQN).
    */
    static final String FULLTEXT_MAPPING_5_3 = """
        {
          "default": {
            "dynamic": "strict",
            "_meta": {
              "indices": {
                "nested_col_fulltext": {},
                "title_desc_fulltext": {}
              }
            },
            "properties": {
              "author": {
                "position": 3,
                "dynamic": "true",
                "properties": {
                  "birthday": {
                    "type": "date",
                    "position": 5,
                    "format": "epoch_millis||strict_date_optional_time"
                  },
                  "name": {
                    "type": "keyword",
                    "position": 4,
                    "copy_to": [
                      "nested_col_fulltext"
                    ]
                  }
                }
              },
              "description": {
                "type": "keyword",
                "position": 2,
                "copy_to": [
                  "title_desc_fulltext"
                ]
              },
              "nested_col_fulltext": {
                "type": "text",
                "position": 7,
                "analyzer": "stop"
              },
              "title": {
                "type": "keyword",
                "position": 1,
                "copy_to": [
                  "nested_col_fulltext",
                  "title_desc_fulltext"
                ]
              },
              "title_desc_fulltext": {
                "type": "text",
                "position": 6,
                "analyzer": "standard"
              }
            }
          }
        }
        """;

    static final String FULLTEXT_MAPPING_EXPECTED_IN_5_4 =
        "{\"default\":{" +
            "\"dynamic\":\"strict\"," +
            "\"_meta\":{" +
            "\"indices\":{\"nested_col_fulltext\":{},\"title_desc_fulltext\":{}}}" +
            ",\"properties\":{" +
                "\"author\":{\"position\":3,\"dynamic\":\"true\",\"properties\":{" +
                    "\"birthday\":{\"type\":\"date\",\"position\":5,\"format\":\"epoch_millis||strict_date_optional_time\"}," +
                    "\"name\":{\"type\":\"keyword\",\"position\":4}}}," +
                "\"description\":{\"type\":\"keyword\",\"position\":2}," +
                "\"nested_col_fulltext\":{\"type\":\"text\",\"position\":7,\"analyzer\":\"stop\",\"sources\":[\"author.name\",\"title\"]}," +
                "\"title\":{\"type\":\"keyword\",\"position\":1}," +
                "\"title_desc_fulltext\":{\"type\":\"text\",\"position\":6,\"analyzer\":\"standard\",\"sources\":[\"description\",\"title\"]}}}}";

    // Obtained by executing test_copy_deep_nested_object_to_partitioned_table_results_in_dynamic_mapping_updates manually.
    static final String DEEP_NESTED_MAPPING =
        """
            {
              "default": {
                "_meta": {
                  "partitioned_by": [
                    [
                      "p",
                      "integer"
                    ]
                  ]
                },
                "dynamic": "true",
                "properties": {
                  "p": {
                    "index": false,
                    "position": 2,
                    "type": "integer"
                  },
                  "tb": {
                    "type": "array",
                    "inner": {
                      "dynamic": "true",
                      "position": 1,
                      "type": "object",
                      "properties": {
                        "t1": {
                          "type": "array",
                          "inner": {
                            "position": 3,
                            "properties": {
                              "t6": {
                                "type": "array",
                                "inner": {
                                  "position": 6,
                                  "type": "long"
                                }
                              },
                              "t3": {
                                "position": 5,
                                "properties": {
                                  "t4": {
                                    "position": 7,
                                    "properties": {
                                      "t5": {
                                        "position": 8,
                                        "type": "long"
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        },
                        "t2": {
                          "position": 4,
                          "type": "object"
                        }
                      }
                    }
                  },
                  "o": {
                    "position": 9,
                    "properties": {
                      "a": {
                        "position": 10,
                        "properties": {
                          "b": {
                            "position": 12,
                            "type": "long"
                          }
                        }
                      },
                      "b": {
                        "position": 11,
                        "type": "long"
                      }
                    }
                  }
                }
              }
            }
            """;
}

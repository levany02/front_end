from elasticsearch import Elasticsearch, helpers
from pymongo import MongoClient
import sys

INDEX_NAME = sys.argv[2]
database = sys.argv[1]

if __name__ == '__main__':
    mongo = MongoClient()
    data = list(mongo[database].data.find({}, {"jobId": 1, "title": 1, "location": 1, "category": 1, "skill": 1, "salary": 1, "level": 1, "_id": 0, "image": 1}))
    setting = {
        "settings": {
            "number_of_shards": 1,
            "index": {
                "max_ngram_diff": 8
            },
            "similarity": {
                "scripted_count": {
                    "type": "scripted",
                    "script": {
                        "source": "return query.boost * 1"
                    }
                }
            },
            "analysis": {
                "filter": {
                    "shingle": {
                        "type": "shingle",
                        "min_shingle_size": 2,
                        "max_shingle_size": 3
                    },
                    "synonym_filter": {
                        "type": "synonym",
                        "lenient": True,
                        "synonyms": [
                            "Artificial Intelligent => AI, Artificial Intelligent",
                            "AI => AI, Artificial Intelligent"
                        ]
                    },
                    "custom_stop_words_filter": {
                        "type": "stop",
                        "ignore_case": True,
                        "stopwords": ['phim', 'the', 'Phim', 'The', 'kenh']
                    }
                },
                "analyzer": {
                    "edge_analyzer": {
                        "tokenizer": "edge_tokenizer"
                    },
                    "edge_analyzer_space": {
                        "tokenizer": "edge_tokenizer_space",
                        "filter": ["custom_stop_words_filter", "remove_duplicates"]
                    }
                    ,
                    "standard_analyzer": {
                        "tokenizer": "standard",
                        "filter": ["synonym_filter", "remove_duplicates"]
                    },
                    "ngam_analyzer": {
                        "tokenizer": "ngram_tokenizer",
                        "filter": ["synonym_filter", "remove_duplicates"]
                    },
                    "ngam_analyzer_completion": {
                        "tokenizer": "ngram_tokenizer_completion",
                        "filter": ["synonym_filter", "remove_duplicates"]
                    },
                    "custom_simple": {
                        "tokenizer": "standard",
                        "filter": ["custom_stop_words_filter", "remove_duplicates"]
                    },
                    "trigram": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "shingle"]
                    },
                    "reverse": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "reverse"]
                    }
                },
                "tokenizer": {
                    "edge_tokenizer_space": {
                        "type": "edge_ngram",
                        "min_gram": 2,
                        "max_gram": 50,
                        "token_chars": [
                            "letter",
                            "digit",
                            "whitespace"
                        ]
                    },
                    "edge_tokenizer": {
                        "type": "edge_ngram",
                        "min_gram": 2,
                        "max_gram": 7,
                        "token_chars": [
                            "letter",
                            "digit"
                        ]
                    },
                    "ngram_tokenizer": {
                        "type": "ngram",
                        "min_gram": 2,
                        "max_gram": 5,
                        "token_chars": [
                            "letter",
                            "digit",
                            "whitespace"
                        ]
                    },
                    "ngram_tokenizer_completion": {
                        "type": "ngram",
                        "min_gram": 4,
                        "max_gram": 6,
                        "token_chars": [
                            "letter",
                            "digit",
                            "whitespace"
                        ]
                    },
                    "lowercase_custom": {
                        "type": "lowercase",
                        "token_chars": [
                            "digit"
                        ]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "title": {
                    "type": "text",
                    "boost": 5,
                    "analyzer": "standard_analyzer",
                    "similarity": "scripted_count"
                },
                "category": {
                    "type": "text",
                    "boost": 2,
                    "analyzer": "standard_analyzer",
                    "similarity": "scripted_count"
                }
                ,
                "location": {
                    "type": "text",
                    "boost": 1,
                    "analyzer": "standard_analyzer",
                    "similarity": "scripted_count"
                },
                "salary": {
                    "type": "keyword",
                },
                "skill": {
                    "type": "text",
                    "boost": 1,
                    "analyzer": "standard_analyzer",
                    "similarity": "scripted_count"
                },
                "level": {
                    "type": "text",
                    "boost": 1,
                    "analyzer": "standard_analyzer",
                    "similarity": "scripted_count"
                }
            }
        }
    }
    es = Elasticsearch()
    res = es.indices.create(index=INDEX_NAME, body=setting, ignore=400)
    print(res)
    actions = []
    for idx, _source in enumerate(data):
        actions.append(
            {
                    "_index": INDEX_NAME,
                    '_op_type': 'index',
                    "_type": "_doc",
                    "_id": idx,
                    "_source": _source
                }
        )

    if len(actions) > 0:
        helpers.bulk(es, actions)

    print('All Finished')
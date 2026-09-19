from datetime import datetime
import uuid


def detail_line_chart_pipeline(node_id: uuid.UUID, start_time: datetime, unit_time: str):
    return [{
        "$match": {
            "node.id": str(node_id),
            "timestamp": {
                "$gte": start_time
            }
        }
    },
    {
        "$project": {
            "bucketTime": {
                "$dateTrunc": {
                    "date": "$timestamp",
                    "unit": unit_time
                }
            },
            "countsArray": {
                "$objectToArray": "$counts"
            }
        }
    },
    {
        "$unwind": "$countsArray"
    },
    {
        "$group": {
            "_id": {
                "bucketTime": "$bucketTime",
                "key": "$countsArray.k"
            },
            "avgValue": {
                "$avg": "$countsArray.v"
            }
        }
    },
    {
        "$sort": {
            "_id.bucketTime": 1
        }
    },
    {
        "$project": {
            "key": "$_id.key",
            "point": [
                "$_id.bucketTime",
                {
                    "$round": [
                        "$avgValue",
                        2
                    ]
                }
            ]
        }
    },
    {
        "$group": {
            "_id": "$key",
            "data": {
                "$push": "$point"
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "name": "$_id",
            "type": "line",
            "showSymbol": False,
            "data": 1
        }
    }]

def summary_bar_chart_pipeline(node_id: uuid.UUID, start_time: datetime):
    return [{
        "$match": {
            "node.id": str(node_id),
            "timestamp": {
                "$gte": start_time
            }
        }
    },
    {
        "$project": {
            "countsArray": {
                "$objectToArray": "$counts"
            }
        }
    },
    {
        "$unwind": "$countsArray"
    },
    {
        "$group": {
            "_id": "$countsArray.k",
            "minVal": {
                "$min": "$countsArray.v"
            },
            "avgVal": {
                "$avg": "$countsArray.v"
            },
            "maxVal": {
                "$max": "$countsArray.v"
            }
        }
    },
    {
        "$sort": {
            "_id": 1
        }
    },
    {
        "$group": {
            "_id": None,
            "categories": {
                "$push": "$_id"
            },
            "minList": {
                "$push": "$minVal"
            },
            "avgList": {
                "$push": {
                    "$round": [
                        "$avgVal",
                        2
                    ]
                }
            },
            "maxList": {
                "$push": "$maxVal"
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "xAxis": {
                "type": "category",
                "data": "$categories"
            },
            "yAxis": {
                "type": "value"
            },
            "legend": {
                "data": [
                    "min",
                    "average",
                    "max"
                ]
            },
            "series": [
                {
                    "name": "min",
                    "type": "bar",
                    "data": "$minList"
                },
                {
                    "name": "average",
                    "type": "bar",
                    "data": "$avgList"
                },
                {
                    "name": "max",
                    "type": "bar",
                    "data": "$maxList"
                }
            ]
        }
    }]

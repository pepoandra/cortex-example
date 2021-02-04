import pandas as pd
from redis import StrictRedis
import os
import time
import json

_WEEK_IN_SECONDS = 60 * 60 * 24 * 7
_REDIS_PIPE_SIZE = 1000
_REDIS_LPUSH_BLOCK_SIZE = 1000
try:
    redis = StrictRedis(host='ml-bg-localdev.zzesue.ng.0001.use2.cache.amazonaws.com',
                                     port=6379,
                                     encoding="utf-8",
                                     decode_responses=True)
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, 'hits_dataset.csv')
    data = pd.read_csv(
        filename,
        index_col=0,
        encoding="utf-8",
    )


    start_insert_time = time.time()
    grouped = data.groupby(by='memberID').apply(lambda x: x.to_dict(orient='records')).to_dict()

    pipe = redis.pipeline()
    # write _REDIS_PIPE_SIZE users in a row
    index = 0
    for key, value in grouped.items():
        for v in value:
            del v['memberID']
        pipe.set(f'scores:u:{key}', json.dumps(value), ex=_WEEK_IN_SECONDS)
        index += 1
        if index > 0 and index % _REDIS_PIPE_SIZE == 0:
            pipe.execute()
            pipe = redis.pipeline()

    # flush the pipe
    pipe.execute()


except Exception as ex:
    print(ex)

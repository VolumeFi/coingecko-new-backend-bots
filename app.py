import json
import logging
import threading

import bottle
import sentry_sdk
from bottle import request

import cache_db
import gecko
import momentum_scanner_intraday

sentry_sdk.init(
    dsn="https://955ac0a74d244e2c914767a351d4d069@o1200162.ingest.sentry.io/4505082653573120",
    traces_sample_rate=1.0,
)


@bottle.get("/get_high_returns")
def get_high_returns():
    with cache_db.connect():
        df = momentum_scanner_intraday.get_new_listing()
    df.dropna(how="all", axis=1, inplace=True)
    df.fillna(0, inplace=True)
    return json.dumps(df.to_dict(), sort_keys=True)


def main():
    logging.basicConfig(level=logging.INFO)
    cache_db.init()
    gecko.init()
    threading.Thread(target=cache_db.warm_cache_loop).start()
    bottle.run(host="localhost", port=8765)


if __name__ == "__main__":
    main()

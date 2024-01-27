#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import logging

from fastapi import FastAPI

from fbpcs.smart_agent import handler


logging.basicConfig(level=logging.INFO)

app = FastAPI()

app.include_router(handler.router)


def main() -> None:
    # uvicorn.run(app, host="0.0.0.0", port=8000)
    from multiprocessing import Process

    import uvicorn

    server = Process(
        target=uvicorn.run,
        args=(app,),
        kwargs={
            "host": "0.0.0.0",
            "port": 8000,
        },
    )
    logging.info("Starting server...")
    server.start()

    import time

    for i in range(10):
        logging.info(f"Sleeping 1 sec - {i}")
        time.sleep(1)

    logging.info("Terminating server...")
    server.terminate()
    server.join()


if __name__ == "__main__":
    main()  # pragma: no cover

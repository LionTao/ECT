import asyncio
import json
import time

import numpy as np
from dapr.actor import ActorProxy, ActorId
from rtree import index

from scale import DemoActorInterface

WINDOW = 20
minute = 1
step = 4 * minute


# 100 single 158s

async def single(idx, query, l):
    proxy = ActorProxy.create('DemoActor', ActorId(str(idx)), DemoActorInterface)
    first = await proxy.invoke_method("Init",
                                      json.dumps({"a": query[:WINDOW].tolist(), "b": l[:WINDOW].tolist()}).encode(
                                          "utf-8"))
    second = await proxy.invoke_method("Move", json.dumps(
        {"a_remove": 4, "b_remove": 4, "a_add": query[WINDOW:WINDOW + step].tolist(),
         "b_add": l[WINDOW:WINDOW + step].tolist()}).encode(
        "utf-8"))
    # await proxy.invoke_method("Reset")
    second = await proxy.invoke_method("Move", json.dumps(
        {"a_remove": 4, "b_remove": 4, "a_add": query[WINDOW + step:WINDOW + step * 2].tolist(),
         "b_add": l[WINDOW + step:WINDOW + step * 2].tolist()}).encode(
        "utf-8"))
    # await proxy.invoke_method("Reset")
    second = await proxy.invoke_method("Move", json.dumps(
        {"a_remove": 4, "b_remove": 4, "a_add": query[WINDOW + step * 2:WINDOW + step * 3].tolist(),
         "b_add": l[WINDOW + step * 2:WINDOW + step * 3].tolist()}).encode(
        "utf-8"))
    # await proxy.invoke_method("Reset")
    second = await proxy.invoke_method("Move", json.dumps(
        {"a_remove": 4, "b_remove": 4, "a_add": query[WINDOW + step * 3:WINDOW + step * 4].tolist(),
         "b_add": l[WINDOW + step * 3:WINDOW + step * 4].tolist()}).encode(
        "utf-8"))


async def main(data):
    # Create proxy client
    tasks = []

    for i in range(0, 3901, 100):
        for idx, l in enumerate(data[i:i + 100]):
            # Python 3.7+. Use ensure_future for older versions.
            task = asyncio.create_task(single(idx, data[0], l))
            tasks.append(task)

        await asyncio.gather(*tasks)
    # for idx, l in :

    # print(first.decode("utf-8"), second.decode("utf-8"))


if __name__ == '__main__':
    with open("/home/liontao/work/stream-distance/notebooks/preprocess/tdrive-dita-long-small.txt", 'r') as f:
        data = map(str.strip, f.readlines())
        data = list(filter(lambda x: len(x) > 40,
                      [np.asarray([[float(c) for c in p.split(",")] for p in i.split(";")]) for i in data]))

    print(len(data), flush=True)
    # tree = index.Index(interleaved=False)
    # for idx, l in enumerate(data):
    #     maxX, maxY = np.max(l, axis=0)
    #     minX, minY = np.min(l, axis=0)
    #     tree.insert(idx, (minX, maxX, minY, maxY))

    # maxX, maxY = np.max(data[0], axis=0)
    # minX, minY = np.min(data[0], axis=0)
    t1 = time.perf_counter_ns()
    asyncio.run(main(data))
    print((time.perf_counter_ns() - t1) / 1e9, flush=True)

import asyncio
import datetime
import time


def blocking_io(v: int):
    print(f"task started ({v})")
    time.sleep(v)
    # await asyncio.sleep(v)
    print(f"hello, from coroutine ({v})")


async def blocking_io_async(v: int):
    blocking_io(v)


# async def main():
#     task1 = asyncio.create_task(blocking_io(4))
#     print("task1 created")
#     task2 = asyncio.create_task(blocking_io(2))
#     print("task2 created")
#     print(datetime.datetime.now())
#     await asyncio.sleep(10)
#     # await task1
#     print(datetime.datetime.now())
#     # await task2
#     print(datetime.datetime.now())

#     # asyncio.gather(task1, task2)


async def main():
    loop = asyncio.get_event_loop()

    print(datetime.datetime.now())
    fut1 = loop.run_in_executor(None, blocking_io, 5)
    print(datetime.datetime.now())
    fut2 = loop.run_in_executor(None, blocking_io, 3)
    print(datetime.datetime.now())

    await asyncio.gather(fut1, fut2)
    print(datetime.datetime.now())


asyncio.run(main())

import asyncio
import datetime
import random
import time
import typing
from concurrent.futures import ThreadPoolExecutor

import boto3

s3 = boto3.client("s3")


CopyAllArg: typing.TypeAlias = list[tuple[str, str, str, str]]


def copy_object(bucket_from: str, key_from: str, bucket_to: str, key_to: str):
    # time.sleep(1)
    s3.copy_object(
        Bucket=bucket_to,
        Key=key_to,
        CopySource={"Bucket": bucket_from, "Key": key_from},
    )
    print("copied", f"s3://{bucket_from}/{key_from}", " -> ", f"s3://{bucket_to}/{key_to}")


def copy_and_maybe_fail(bucket_from: str, key_from: str, bucket_to: str, key_to: str):
    if random.random() > 0.5:
        raise Exception("!!!!")
    time.sleep(1)


def copy_all(from_to_pairs: CopyAllArg):
    """(1) 一つずつ順番にコピーするやり方"""
    for bucket_from, key_from, bukcet_to, key_to in from_to_pairs:
        copy_object(bucket_from, key_from, bukcet_to, key_to)


def copy_all_concurrent_using_submit(from_to_pairs: CopyAllArg):
    """並行にファイルをコピーするやり方(Executor.submit)"""
    with ThreadPoolExecutor() as executor:
        futures = []
        for pair in from_to_pairs:
            future = executor.submit(copy_object, *pair)
            futures.append(future)

        # for future in futures:
        #     try:
        #         future.result()
        #         print("ok")
        #     except Exception as e:
        #         print(e, file=sys.stdout)
        #         print("failed")


def _copy_object(
    pair: tuple[str, str, str, str]
) -> tuple[tuple[str, str, str, str], typing.Optional[Exception]]:
    try:
        copy_object(*pair)
        return pair, None
    except Exception as e:
        return pair, e


def copy_all_concurrent_using_map(from_to_pairs: CopyAllArg):
    """並行にファイルをコピーするやり方(Executor.map)"""

    with ThreadPoolExecutor() as executor:
        executor.map(_copy_object, from_to_pairs)
        # results = executor.map(_copy_object, from_to_pairs)

    # for result in results:
    #     pair, err = result
    #     match err:
    #         case Exception():
    #             print(err, file=sys.stdout)
    #             # print(pair, "failed")
    #         case _:
    #             ...
    #             # print(pair, "ok")


async def copy_all_async(from_to_pairs: CopyAllArg):
    """並行にファイルをコピーするやり方(asyncio.to_thread)"""
    async with asyncio.TaskGroup() as tg:
        for pair in from_to_pairs:
            co = asyncio.to_thread(_copy_object, pair)
            tg.create_task(co)


async def copy_all_run_in_executor(from_to_pairs: CopyAllArg):
    """並行にファイルをコピーするやり方(asyncio.run_in_executor)"""
    default_loop = asyncio.get_event_loop()
    futures = []
    for pair in from_to_pairs:
        futures.append(default_loop.run_in_executor(None, copy_object, *pair))

    asyncio.gather(*futures)


def main():
    bucket_name = "stepfunctions-copy-multiple-objects-demo"

    # fmt: off
    from_to_pairs: list[tuple] = []
    for index in range(500):
        from_to_pairs.append((bucket_name, f"demo/data_{index}.dat", bucket_name, f"dest/data_{index}.dat")) # noqa

    # from_to_pairs = [
    #     (bucket_name, "demo/data_0.dat", bucket_name, "dest/data_0.dat"),
    #     (bucket_name, "demo/data_1.dat", bucket_name, "dest/data_1.dat"),
    #     (bucket_name, "demo/data_2.dat", bucket_name, "dest/data_2.dat"),
    #     (bucket_name, "demo/data_3.dat", bucket_name, "dest/data_3.dat"),
    #     (bucket_name, "demo/data_4.dat", bucket_name, "dest/data_4.dat"),
    #     (bucket_name, "demo/data_5.dat", bucket_name, "dest/data_5.dat"),
    #     (bucket_name, "demo/data_6.dat", bucket_name, "dest/data_6.dat"),
    #     (bucket_name, "demo/data_7.dat", bucket_name, "dest/data_7.dat"),
    #     (bucket_name, "demo/data_8.dat", bucket_name, "dest/data_8.dat"),
    #     (bucket_name, "demo/data_9.dat", bucket_name, "dest/data_9.dat"),
    #     (bucket_name, "demo/data_11.dat", bucket_name, "dest/data_11.dat"),
    #     (bucket_name, "demo/data_12.dat", bucket_name, "dest/data_12.dat"),
    #     (bucket_name, "demo/data_13.dat", bucket_name, "dest/data_13.dat"),
    #     (bucket_name, "demo/data_14.dat", bucket_name, "dest/data_14.dat"),
    #     (bucket_name, "demo/data_15.dat", bucket_name, "dest/data_15.dat"),
    #     (bucket_name, "demo/data_16.dat", bucket_name, "dest/data_16.dat"),
    #     (bucket_name, "demo/data_17.dat", bucket_name, "dest/data_17.dat"),
    #     (bucket_name, "demo/data_18.dat", bucket_name, "dest/data_18.dat"),
    #     (bucket_name, "demo/data_19.dat", bucket_name, "dest/data_19.dat"),
    # ]
    # fmt: on

    start = datetime.datetime.now()

    # copy_all(from_to_pairs)  # (1)
    # copy_all_concurrent_using_submit(from_to_pairs)  # (2)
    # copy_all_concurrent_using_map(from_to_pairs)  # (3)
    # asyncio.run(copy_all_async(from_to_pairs)) # (4)
    # asyncio.run(copy_all_run_in_executor(from_to_pairs)) # (5)
    end = datetime.datetime.now()
    print("all tasks are completed!")
    print("it took", end - start)


if __name__ == "__main__":
    main()

from TikTokApi import TikTokApi

import asyncio
import os
import pandas as pd
import time
import csv
import multiprocessing as mp

from pathlib import Path


# ===================================================================
# CONFIGURATION
# ===================================================================

INPUT_FILE = "all_videos.csv"
INPUT_COLUMN = "url"

OUTPUT_DIR = Path("output_data")
WORK_DIR = OUTPUT_DIR / "cluster_chunks"

# -------------------------------------------------------------------
# CLUSTER CONFIGURATION
# -------------------------------------------------------------------

# Number of independent Python processes.
#
# Example:
#
#   1 worker  = normal
#   4 workers = 4 independent TikTokApi instances
#   8 workers = 8 independent TikTokApi instances
#
NUM_WORKERS = 9

# Number of TikTokApi browser sessions INSIDE EACH worker.
#
# Total sessions =
#
#     NUM_WORKERS * SESSIONS_PER_WORKER
#
SESSIONS_PER_WORKER = 1


# Number of concurrent video.info() requests INSIDE EACH worker.
#
# Total theoretical concurrency =
#
#     NUM_WORKERS * CONCURRENCY_PER_WORKER
#
CONCURRENCY_PER_WORKER = 7


# Number of URLs assigned to each worker at a time.
#
# 100,000 URLs with CHUNK_SIZE=1,000 means approximately
# 100 chunks.
#
# Workers continuously take chunks until everything is complete.
CHUNK_SIZE = 45


# TikTokApi settings
SLEEP_AFTER = 3

TIKTOK_BROWSER = os.getenv(
    "TIKTOK_BROWSER",
    "chromium",
)


# -------------------------------------------------------------------
# Input test limit
# -------------------------------------------------------------------
#
# Set to None for the entire input file.
#
# For your current 100 URL test:
#
# TEST_LIMIT = 100
#
# For production:
#
# TEST_LIMIT = None
#
TEST_LIMIT = 500


# -------------------------------------------------------------------
# Output
# -------------------------------------------------------------------

FINAL_OUTPUT = OUTPUT_DIR / "test_output_file_ALL.csv"


# ===================================================================
# ENVIRONMENT / TOKENS
# ===================================================================

# IMPORTANT:
#
# Set these in your shell:
#
# export MS_TOKEN_1="..."
# export MS_TOKEN_2="..."
# ...
#
# Do NOT put the actual tokens directly into this source file.

MS_TOKENS = [
    os.environ.get("MS_TOKEN_1"),
    os.environ.get("MS_TOKEN_2"),
    os.environ.get("MS_TOKEN_3"),
    os.environ.get("MS_TOKEN_4"),
    os.environ.get("MS_TOKEN_5"),
]

MS_TOKENS = [
    token
    for token in MS_TOKENS
    if token
]


# ===================================================================
# OUTPUT COLUMNS
# ===================================================================

COL_NAMES = [
    "url",
    "author_name",
    "author_ID",
    "video_id",
    "duration",
    "description",
    "music",
    "music_url",
    "challenges",
    "d_labels",
    "sug_words",
    "hashtags",
]


# ===================================================================
# HELPERS
# ===================================================================

def extract_hashtags(text):
    if not text:
        return []

    return [
        word[1:]
        for word in str(text).split()
        if word.startswith("#")
    ]


# ===================================================================
# PROCESS ONE URL
# ===================================================================

async def process_url(api, url, semaphore):

    total_start = time.perf_counter()

    try:

        # -----------------------------------------------------------
        # Create Video object
        # -----------------------------------------------------------

        video = api.video(url=url)


        # -----------------------------------------------------------
        # Wait for concurrency slot
        # -----------------------------------------------------------

        semaphore_wait_start = time.perf_counter()

        async with semaphore:

            semaphore_wait_time = (
                time.perf_counter()
                - semaphore_wait_start
            )

            # -------------------------------------------------------
            # TikTok API request
            # -------------------------------------------------------

            api_start = time.perf_counter()

            await video.info()

            api_time = (
                time.perf_counter()
                - api_start
            )


        # -----------------------------------------------------------
        # Process returned data
        # -----------------------------------------------------------

        processing_start = time.perf_counter()

        data = video.as_dict

        data.setdefault("desc", "")
        data.setdefault("challenges", "Missing")
        data.setdefault(
            "diversificationLabels",
            "Missing",
        )
        data.setdefault(
            "suggestedWords",
            "Missing",
        )

        # -----------------------------------------------------------
        # Music
        # -----------------------------------------------------------

        data.setdefault("music", {})

        if not isinstance(data["music"], dict):
            data["music"] = {}

        data["music"].setdefault(
            "id",
            "Missing",
        )

        data["music"].setdefault(
            "playUrl",
            "Missing",
        )


        # -----------------------------------------------------------
        # Challenges
        # -----------------------------------------------------------

        if data["challenges"] != "Missing":

            challenges = [
                c.get("title")
                for c in data["challenges"]
            ]

        else:

            challenges = "Missing"


        # -----------------------------------------------------------
        # Author
        # -----------------------------------------------------------

        author = data.get("author", {})

        author_name = author.get(
            "uniqueId",
            "Missing",
        )

        author_id = author.get(
            "id",
            "Missing",
        )


        # -----------------------------------------------------------
        # Video
        # -----------------------------------------------------------

        video_data = data.get(
            "video",
            {},
        )

        duration = video_data.get(
            "duration",
            "Missing",
        )


        # -----------------------------------------------------------
        # Build row
        # -----------------------------------------------------------

        row = [
            url,
            author_name,
            str(author_id),
            str(data.get("id", "Missing")),
            duration,
            data["desc"],
            str(data["music"]["id"]),
            str(data["music"]["playUrl"]),
            challenges,
            data["diversificationLabels"],
            data["suggestedWords"],
            extract_hashtags(
                data["desc"]
            ),
        ]


        processing_time = (
            time.perf_counter()
            - processing_start
        )

        total_time = (
            time.perf_counter()
            - total_start
        )


        # -----------------------------------------------------------
        # Worker output
        # -----------------------------------------------------------

        #print(
         #   f"    ✅ {url} | "
          #  f"Wait: {semaphore_wait_time:.2f}s | "
           # f"API: {api_time:.2f}s | "
           # f"Process: {processing_time:.3f}s | "
           # f"Total: {total_time:.2f}s",
           # flush=True,
        #)

        return row


    except Exception as e:

        total_time = (
            time.perf_counter()
            - total_start
        )

        #print(
         #   f"    ❌ FAILED {url} | "
          #  f"{type(e).__name__}: {e} | "
           # f"Total: {total_time:.2f}s",
            #flush=True,
        #)

        return {
            "url": url,
            "error": str(e),
        }

# ===================================================================
# PROCESS ONE CHUNK
# ===================================================================

async def process_chunk(
    api,
    urls,
    worker_id,
    chunk_id,
):

    semaphore = asyncio.Semaphore(
        CONCURRENCY_PER_WORKER
    )

    print(
        f"\n"
        f"  Worker {worker_id} "
        f"starting chunk {chunk_id} "
        f"({len(urls)} URLs)",
        flush=True,
    )

    start_time = time.perf_counter()


    # ---------------------------------------------------------------
    # IMPORTANT
    #
    # We only create tasks for THIS CHUNK.
    #
    # If CHUNK_SIZE = 1000, we create at most 1000 tasks.
    #
    # Not 100,000.
    # ---------------------------------------------------------------

    tasks = [
        process_url(
            api,
            url,
            semaphore,
        )
        for url in urls
    ]


    results = await asyncio.gather(
        *tasks,
        return_exceptions=True,
    )


    successful_rows = []
    failed_rows = []


    for url, result in zip(
        urls,
        results,
    ):

        if isinstance(
            result,
            Exception,
        ):

            failed_rows.append({
                "url": url,
                "error": str(result),
            })

        elif (
            isinstance(result, dict)
            and "error" in result
        ):

            failed_rows.append(result)

        else:

            successful_rows.append(result)


    elapsed = (
        time.perf_counter()
        - start_time
    )


    print(
        f"\n"
        f"  Worker {worker_id} "
        f"finished chunk {chunk_id} | "
        f"Success: {len(successful_rows)} | "
        f"Failed: {len(failed_rows)} | "
        f"Time: {elapsed:.2f}s",
        flush=True,
    )


    return (
        successful_rows,
        failed_rows,
    )


# ===================================================================
# WRITE CHUNK RESULT
# ===================================================================

def write_chunk_result(
    worker_id,
    chunk_id,
    successful_rows,
    failed_rows,
):

    WORK_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )


    # ---------------------------------------------------------------
    # Successful data
    # ---------------------------------------------------------------

    success_path = (
        WORK_DIR
        / f"worker_{worker_id:03d}_chunk_{chunk_id:06d}.csv"
    )

    success_df = pd.DataFrame(
        successful_rows,
        columns=COL_NAMES,
    )

    success_df.to_csv(
        success_path,
        index=False,
    )


    # ---------------------------------------------------------------
    # Failed URLs
    # ---------------------------------------------------------------

    failed_path = (
        WORK_DIR
        / f"worker_{worker_id:03d}_chunk_{chunk_id:06d}_failed.csv"
    )

    failed_df = pd.DataFrame(
        failed_rows,
        columns=[
            "url",
            "error",
        ],
    )

    failed_df.to_csv(
        failed_path,
        index=False,
    )


    return success_path, failed_path


# ===================================================================
# WORKER PROCESS
# ===================================================================

def worker_main(
    worker_id,
    chunks,
):

    process_start = time.perf_counter()


    print(
        "\n"
        + "=" * 70,
        flush=True,
    )

    print(
        f"WORKER {worker_id} STARTING",
        flush=True,
    )

    print(
        f"Chunks assigned: {len(chunks)}",
        flush=True,
    )

    print(
        f"Sessions: {SESSIONS_PER_WORKER}",
        flush=True,
    )

    print(
        f"Concurrency: {CONCURRENCY_PER_WORKER}",
        flush=True,
    )

    print(
        "=" * 70,
        flush=True,
    )


    try:

        asyncio.run(
            worker_async(
                worker_id,
                chunks,
            )
        )


    except Exception as e:

        print(
            f"\n"
            f"💥 WORKER {worker_id} CRASHED\n"
            f"{type(e).__name__}: {e}",
            flush=True,
        )

        raise


    elapsed = (
        time.perf_counter()
        - process_start
    )


    print(
        f"\n"
        f"🏁 WORKER {worker_id} FINISHED "
        f"in {elapsed:.2f}s",
        flush=True,
    )


# ===================================================================
# ASYNC WORKER
# ===================================================================

async def worker_async(
    worker_id,
    chunks,
):

    session_start = time.perf_counter()


    print(
        f"Worker {worker_id}: "
        f"creating TikTokApi sessions...",
        flush=True,
    )


    async with TikTokApi() as api:

        await api.create_sessions(
            ms_tokens=MS_TOKENS,
            num_sessions=SESSIONS_PER_WORKER,
            sleep_after=SLEEP_AFTER,
            browser=TIKTOK_BROWSER,
            allow_partial_sessions=True,
            min_sessions=1,
        )


        session_time = (
            time.perf_counter()
            - session_start
        )


        print(
            f"Worker {worker_id}: "
            f"created {len(api.sessions)} sessions "
            f"in {session_time:.2f}s",
            flush=True,
        )


        # -----------------------------------------------------------
        # Process assigned chunks
        # -----------------------------------------------------------

        for chunk_id, urls in chunks:

            chunk_start = time.perf_counter()


            successful_rows, failed_rows = (
                await process_chunk(
                    api,
                    urls,
                    worker_id,
                    chunk_id,
                )
            )


            success_path, failed_path = (
                write_chunk_result(
                    worker_id,
                    chunk_id,
                    successful_rows,
                    failed_rows,
                )
            )


            chunk_time = (
                time.perf_counter()
                - chunk_start
            )


            print(
                f"\n"
                f"  💾 Worker {worker_id} "
                f"saved chunk {chunk_id} | "
                f"Success: {len(successful_rows)} | "
                f"Failed: {len(failed_rows)} | "
                f"Time: {chunk_time:.2f}s",
                flush=True,
            )

            print(
                f"     {success_path}",
                flush=True,
            )


        print(
            f"\n"
            f"Worker {worker_id}: "
            f"all assigned chunks complete",
            flush=True,
        )


# ===================================================================
# SPLIT URLS INTO CHUNKS
# ===================================================================

def create_chunks(urls):

    chunks = []

    total = len(urls)

    chunk_id = 0


    for start in range(
        0,
        total,
        CHUNK_SIZE,
    ):

        end = min(
            start + CHUNK_SIZE,
            total,
        )

        chunk_urls = urls[start:end]

        chunks.append(
            (
                chunk_id,
                chunk_urls,
            )
        )

        chunk_id += 1


    return chunks


# ===================================================================
# DISTRIBUTE CHUNKS TO WORKERS
# ===================================================================

def distribute_chunks(
    chunks,
    num_workers,
):

    worker_chunks = [
        []
        for _ in range(num_workers)
    ]


    # Round-robin distribution.
    #
    # Chunk 0 -> worker 0
    # Chunk 1 -> worker 1
    # Chunk 2 -> worker 2
    # ...
    # Chunk N -> worker 0
    #
    # This keeps the workload reasonably balanced.

    for index, chunk in enumerate(chunks):

        worker_id = (
            index % num_workers
        )

        worker_chunks[
            worker_id
        ].append(chunk)


    return worker_chunks


# ===================================================================
# COMBINE RESULTS
# ===================================================================

def combine_results(
    original_urls,
):

    print(
        "\n"
        + "=" * 70,
    )

    print(
        "💾 COMBINING WORKER RESULTS"
    )

    print(
        "=" * 70
    )


    success_files = sorted(
        WORK_DIR.glob(
            "worker_*_chunk_*.csv"
        )
    )

    # Remove failed CSVs from successful list.
    success_files = [
        path
        for path in success_files
        if not path.name.endswith(
            "_failed.csv"
        )
    ]


    failed_files = sorted(
        WORK_DIR.glob(
            "worker_*_chunk_*_failed.csv"
        )
    )


    print(
        f"Successful chunk files: "
        f"{len(success_files)}"
    )

    print(
        f"Failed chunk files: "
        f"{len(failed_files)}"
    )


    # ---------------------------------------------------------------
    # Read successful data
    # ---------------------------------------------------------------

    result_frames = []

    for path in success_files:

        try:

            df = pd.read_csv(
                path
            )

            if not df.empty:

                result_frames.append(df)

        except Exception as e:

            print(
                f"⚠️ Could not read {path}: "
                f"{e}"
            )


    if result_frames:

        result_df = pd.concat(
            result_frames,
            ignore_index=True,
        )

    else:

        result_df = pd.DataFrame(
            columns=COL_NAMES
        )


    # ---------------------------------------------------------------
    # Merge against original URLs
    # ---------------------------------------------------------------

    final_df = pd.merge(
        original_urls.to_frame(
            name="url"
        ),
        result_df,
        on="url",
        how="left",
        suffixes=(
            "",
            "_result",
        ),
    )


    # ---------------------------------------------------------------
    # Write final CSV
    # ---------------------------------------------------------------

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )


    final_df.to_csv(
        FINAL_OUTPUT,
        index=False,
    )


    # ---------------------------------------------------------------
    # Failed URLs
    # ---------------------------------------------------------------

    all_failed = []

    for path in failed_files:

        try:

            df = pd.read_csv(
                path
            )

            if not df.empty:

                all_failed.append(df)

        except Exception:
            pass


    failed_output = (
        OUTPUT_DIR
        / "failed_urls.csv"
    )


    if all_failed:

        failed_df = pd.concat(
            all_failed,
            ignore_index=True,
        )

        failed_df.to_csv(
            failed_output,
            index=False,
        )

    else:

        failed_df = pd.DataFrame(
            columns=[
                "url",
                "error",
            ]
        )

        failed_df.to_csv(
            failed_output,
            index=False,
        )


    # ---------------------------------------------------------------
    # Statistics
    # ---------------------------------------------------------------

    successful_count = len(
        result_df
    )

    failed_count = len(
        failed_df
    )

    total_count = len(
        original_urls
    )


    print(
        "\n"
        f"Successful: {successful_count}"
    )

    print(
        f"Failed:     {failed_count}"
    )

    print(
        f"Total URLs: {total_count}"
    )

    print(
        f"\nFinal output:"
    )

    print(
        f"  {FINAL_OUTPUT}"
    )

    print(
        f"\nFailed output:"
    )

    print(
        f"  {failed_output}"
    )


    return (
        successful_count,
        failed_count,
    )


# ===================================================================
# MAIN
# ===================================================================

def main():

    total_start = time.perf_counter()


    print(
        "=" * 70
    )

    print(
        "TikTok LOCAL CLUSTER PROCESSOR"
    )

    print(
        "=" * 70
    )


    # ---------------------------------------------------------------
    # Load URLs
    # ---------------------------------------------------------------

    print(
        f"\n📂 Reading {INPUT_FILE}..."
    )


    input_df = pd.read_csv(
        INPUT_FILE
    )


    if INPUT_COLUMN not in input_df.columns:

        raise ValueError(
            f"Column '{INPUT_COLUMN}' "
            f"not found in {INPUT_FILE}"
        )


    original_urls = (
        input_df[INPUT_COLUMN]
        .dropna()
        .astype(str)
        .reset_index(drop=True)
    )


    if TEST_LIMIT is not None:

        original_urls = (
            original_urls.iloc[
                :TEST_LIMIT
            ]
            .reset_index(drop=True)
        )


    urls = original_urls.tolist()


    # ---------------------------------------------------------------
    # Calculate cluster
    # ---------------------------------------------------------------

    chunks = create_chunks(
        urls
    )


    actual_workers = min(
        NUM_WORKERS,
        len(chunks),
    )


    worker_chunks = (
        distribute_chunks(
            chunks,
            actual_workers,
        )
    )


    total_concurrency = (
        actual_workers
        * CONCURRENCY_PER_WORKER
    )


    total_sessions = (
        actual_workers
        * SESSIONS_PER_WORKER
    )


    print(
        f"\n📊 CLUSTER CONFIGURATION"
    )

    print(
        f"   URLs:                 {len(urls):,}"
    )

    print(
        f"   Chunk size:           {CHUNK_SIZE:,}"
    )

    print(
        f"   Chunks:               {len(chunks):,}"
    )

    print(
        f"   Workers:              {actual_workers}"
    )

    print(
        f"   Sessions / worker:    "
        f"{SESSIONS_PER_WORKER}"
    )

    print(
        f"   Total sessions:       "
        f"{total_sessions}"
    )

    print(
        f"   Concurrency / worker: "
        f"{CONCURRENCY_PER_WORKER}"
    )

    print(
        f"   Total concurrency:    "
        f"{total_concurrency}"
    )


    # ---------------------------------------------------------------
    # Show distribution
    # ---------------------------------------------------------------

    print(
        "\n📦 CHUNK DISTRIBUTION"
    )


    for worker_id, worker_chunk_list in enumerate(
        worker_chunks
    ):

        url_count = sum(
            len(urls)
            for _, urls in worker_chunk_list
        )

        print(
            f"   Worker {worker_id}: "
            f"{len(worker_chunk_list)} chunks / "
            f"{url_count:,} URLs"
        )


    # ---------------------------------------------------------------
    # Clean old chunk results
    # ---------------------------------------------------------------

    WORK_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )


    old_files = list(
        WORK_DIR.glob("*.csv")
    )


    if old_files:

        print(
            f"\n🧹 Removing "
            f"{len(old_files)} old chunk files..."
        )

        for path in old_files:

            path.unlink()


    # ---------------------------------------------------------------
    # Start worker processes
    # ---------------------------------------------------------------

    print(
        "\n🚀 STARTING WORKERS"
    )


    print(
        f"Launching "
        f"{actual_workers} independent processes..."
    )


    processes = []


    for worker_id in range(
        actual_workers
    ):

        process = mp.Process(
            target=worker_main,
            args=(
                worker_id,
                worker_chunks[
                    worker_id
                ],
            ),
            name=f"tiktok-worker-{worker_id}",
        )

        process.start()

        processes.append(
            process
        )


    # ---------------------------------------------------------------
    # Wait for workers
    # ---------------------------------------------------------------

    print(
        "\n⏳ Waiting for workers..."
    )


    for process in processes:

        process.join()


    # ---------------------------------------------------------------
    # Check worker status
    # ---------------------------------------------------------------

    failed_workers = []


    for process in processes:

        if process.exitcode != 0:

            failed_workers.append(
                process.name
            )


    if failed_workers:

        print(
            "\n⚠️ WORKERS FAILED:"
        )

        for worker in failed_workers:

            print(
                f"   {worker}"
            )

        print(
            "\n"
            "The chunk CSVs from successful "
            "workers will still be combined."
        )


    else:

        print(
            "\n✅ All workers completed successfully."
        )


    # ---------------------------------------------------------------
    # Combine
    # ---------------------------------------------------------------

    successful_count, failed_count = (
        combine_results(
            original_urls
        )
    )


    # ---------------------------------------------------------------
    # Final statistics
    # ---------------------------------------------------------------

    total_time = (
        time.perf_counter()
        - total_start
    )


    print(
        "\n"
        + "=" * 70
    )

    print(
        "🎉 CLUSTER COMPLETE"
    )

    print(
        "=" * 70
    )

    print(
        f"Total URLs:       {len(urls):,}"
    )

    print(
        f"Successful:       {successful_count:,}"
    )

    print(
        f"Failed:           {failed_count:,}"
    )

    print(
        f"Workers:          {actual_workers}"
    )

    print(
        f"Total concurrency: {total_concurrency}"
    )

    print(
        f"Total sessions:    {total_sessions}"
    )

    print(
        f"Total time:        {total_time:.2f}s"
    )

    print(
        f"Videos/second:     "
        f"{len(urls) / total_time:.2f}"
    )

    print(
        f"\nOutput:"
    )

    print(
        f"  {FINAL_OUTPUT}"
    )

    print(
        "=" * 70
    )


# ===================================================================
# ENTRY POINT
# ===================================================================

if __name__ == "__main__":

    # ---------------------------------------------------------------
    # Required for multiprocessing, especially when using spawn.
    # ---------------------------------------------------------------

    mp.freeze_support()

    main()


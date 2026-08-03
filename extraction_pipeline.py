from TikTokApi import TikTokApi
import asyncio
import os
import pandas as pd
import zipfile
import re
import time
from math import ceil

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

ms_token = os.environ.get(
 "mENYTwoZbaLCL4LpbNh0PespUlB4XMiNROcOOBH6bdTTq9SYveGuWIUEoiOcp_zdEfkE2JRWHVLJKMckqO8_MGsxhY4SqMX2ABE6uKYNjy-ZJJrMPDrC1KPwX3Oyk9-RRDRLHLRACH33FQ==",
    None
)

video_urls = pd.read_csv('all_videos.csv')

video_urls = video_urls['url']

CHUNK_SIZE = 200
OUTPUT_DIR = "output_data"

os.makedirs(OUTPUT_DIR, exist_ok=True)

col_names = [
    'url', 'author_name', 'author_ID', 'video_id',
    'duration', 'description', 'music', 'music_url',
    'challenges', 'd_labels', 'sug_words'
]

# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------

def extract_hashtags(text):
    return [w[1:] for w in text.split() if w.startswith("#")]

# -------------------------------------------------------------------
# PROCESS A SINGLE CHUNK (USES SHARED API SESSION)
# -------------------------------------------------------------------

async def process_chunk(api, urls, chunk_idx):
    start_time = time.time()

    async def process_url(url):
        video = api.video(url=url)
        await video.info()
        data = video.as_dict
       
        
      
        data.setdefault('desc', '')
        data.setdefault('challenges', 'Missing')
        data.setdefault('diversificationLabels', 'Missing')
        data.setdefault('suggestedWords', 'Missing')
        data.setdefault('music', {'id': 'Missing'})
        data.setdefault('music', {'playUrl': 'Missing'})
        #data.setdefault('video', {}).setdefault('subtitleInfos', [{}])[0].setdefault('Url', 'missing')

        return [
            url,
            data['author']['uniqueId'],
            str(data['author']['id']),
            str(data['id']),
            data['video']['duration'],
            data['desc'],
            str(data['music']['id']),
            str(data['music']['playUrl']),
            [c.get('title') for c in data['challenges']]
            if data['challenges'] != 'Missing' else 'Missing',
            data['diversificationLabels'],
            data['suggestedWords'],
            #data['video']['subtitleInfos'][0]['Url'],
            extract_hashtags(data['desc']),
        ]

    tasks = [process_url(url) for url in urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    rows = [r for r in results if not isinstance(r, Exception)]

    df = pd.DataFrame(rows, columns=col_names + ['hashtags'])
    
    output_file = os.path.join(
        OUTPUT_DIR, f"test_output_file_chunk_{chunk_idx}.csv"
    )
    df.to_csv(output_file, index=False)

    print(
        f"✅ Finished chunk {chunk_idx} | "
        f"{len(df)} videos | "
        f"{time.time() - start_time:.1f}s"
    )

    return output_file

# -------------------------------------------------------------------
# ZIP HELPERS
# -------------------------------------------------------------------

def read_zip_file(ending):
    with zipfile.ZipFile(zip_path, "r") as zipf:
        target = next((n for n in zipf.namelist() if n.endswith(ending)), None)
        if target is None:
            raise FileNotFoundError(f"{ending} not found in archive")
        return zipf.read(target)


# -------------------------------------------------------------------
# MAIN – ONE SESSION, MANY CHUNKS
# -------------------------------------------------------------------

async def main():

    urls = video_urls
   
    num_chunks = ceil(len(urls) / CHUNK_SIZE)
    csv_files = []

    async with TikTokApi() as api:
        await api.create_sessions(
            ms_tokens=[ms_token],
            num_sessions=1,
            sleep_after=3,
            browser=os.getenv("TIKTOK_BROWSER", "chromium"),
        )

        print("✅ TikTokApi session created")
        print(f"Processing {len(urls)} videos in {num_chunks} chunks")

        for i in range(num_chunks):
            start = i * CHUNK_SIZE
            end = start + CHUNK_SIZE

            print(f"\n🚀 Starting chunk {i + 1}/{num_chunks}")

            csv_path = await process_chunk(
                api,
                urls[start:end],               
                i + 1
            )
            csv_files.append(csv_path)

    final_df = pd.concat(
        [pd.read_csv(f) for f in csv_files],
        ignore_index=True
    )
    
    final_df = pd.merge(final_df, video_urls, how = 'right')

    final_path = os.path.join(OUTPUT_DIR, "test_output_file_ALL.csv")
    final_df.to_csv(final_path, index=False)

    print(
        f"\n🎉 DONE! Combined {len(final_df)} videos → {final_path}"
    )

# -------------------------------------------------------------------
# ENTRY POINT
# -------------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(main())

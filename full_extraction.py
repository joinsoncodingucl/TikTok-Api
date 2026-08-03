from TikTokApi import TikTokApi
import asyncio
import os
import pandas as pd
import zipfile
import re
import time
from math import ceil
import json

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

ms_token = os.environ.get(
    "Up0vYny2vX38JAMhSccLxJUUdJtFxyvA4xx-sJCGwHbOeeni9LpaFlGWcriZN9qYyvRniLOkb3y_Gz8ghlP_IMIk72niCEGv7_2oGND99cq2HYTgj71ihyDTZo7ALQc-mHrPjDm_yuwcBK2bGaZIkKhBhQ==",
    None
)

async def get_video_example():
    async with TikTokApi() as api:
        await api.create_sessions(
            ms_tokens=[ms_token],
            num_sessions=1,
            sleep_after=3,
            browser=os.getenv("TIKTOK_BROWSER", "chromium"),
        )
        video = api.video(
            url="https://www.tiktok.com/@georgefcrump/video/7646361276507082006"
        )


        video_info = await video.info()  # is HTML request, so avoid using this too much
        print(video_info)
        video_bytes = await video.bytes()
        with open("video.mp4", "wb") as f:
            f.write(video_bytes)
            
        with open("output.json", "w", encoding="utf-8") as f:
        	json.dump(video_info, f, indent=4)

if __name__ == "__main__":
    asyncio.run(get_video_example())

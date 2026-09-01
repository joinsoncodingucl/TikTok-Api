"""
Almost identical to json_convertor - instead of removing duplicates, this keeps all the videos in + labels which participant the datapoint comes from. Converts Next json files to dataframe of watch history labelled by participant

Output file is all_videos_participants.csv
"""


import json
import re
from pathlib import Path
import pandas as pd

# Input and output folders
input_dir = Path("/mnt/c/Users/daniel/Desktop/20260827_ucl_tiktok_study")
output_dir = Path("Imported_files")
output_dir.mkdir(exist_ok=True)

# Find all files ending in tiktok.json
json_files = list(input_dir.glob("*tiktok.json"))

print(f"Found {len(json_files)} TikTok files")

all_videos = []

#Loop through each participant's json file + extract the relevant watch history info + convert to dataframe + concat together

for json_file in json_files:

    try:
      
        with open(json_file, "r", encoding="utf-8") as file:
            data = json.load(file)

        watch_hist = data[0]
        videos = list(watch_hist.values())

        # Remove final count item if present
        videos.pop()

        videos_str = str(videos)

        url_list = re.findall(r"https?://\S+", videos_str)
        date_list = re.findall(r"\d{4}-\d{2}-\d{2}", videos_str)
        time_list = re.findall(r"\d{2}:\d{2}:\d{2}", videos_str)
        
        video_frame = pd.DataFrame({
            "url": url_list,
            "watch_date": date_list,
            "watch_time": time_list
        })
        

        video_frame["watch_datetime"] = pd.to_datetime(
            video_frame["watch_date"] + " " + video_frame["watch_time"]
        )

        video_frame = (
            video_frame
            .sort_values("watch_datetime")
            .reset_index(drop=True)
        )
        

         # Create output filename based on source file
        csv_name = json_file.stem + "_watch_history.csv"
        output_path = output_dir / csv_name
        
        video_frame['PID'] = csv_name


        #video_frame.to_csv(output_path, index=False)


        print(f"Saved: {output_path}")
        print(" ")
        
        all_videos.append(video_frame)
        
        

    except Exception as e:
        if videos == [[]]:
            print("Participant deleted their entire watch history")
            print(" ")
        else: 
            print(f"Error processing {json_file.name}: {e}")
            print(" ")
          
all_videos_df = pd.concat(all_videos, ignore_index = True)
all_videos_df['url'] = all_videos_df['url'].str.replace('}', '')
all_videos_df['url'] = all_videos_df['url'].str.replace(']', '')
all_videos_df['url'] = all_videos_df['url'].str.replace(',', '')
all_videos_df['url'] = all_videos_df['url'].str.replace("'", "")
all_videos_df.to_csv('all_videos_participant.csv')

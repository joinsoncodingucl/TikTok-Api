"""
The purpose of this program is to convert the TikTok watch history files from all the participants into a single dataframe containing all the unique videos watched by all the participants (e.g. no duplicates). The program identifies the watch history data from the json files collected on Next, and loops through each extracting the relevant information.

The output is a file called all_videos.csv
"""


#Import packages

import json
import re
from pathlib import Path
import pandas as pd

# Input and output folders
input_dir = Path("/mnt/c/Users/daniel/Desktop/20260806_ucl_tiktok_study")
output_dir = Path("Imported_files")
output_dir.mkdir(exist_ok=True)

# Find all files ending in tiktok.json
json_files = list(input_dir.glob("*tiktok.json"))

print(f"Found {len(json_files)} TikTok files")


#Loop through valid json files and extract all the information as a dataframe
all_videos = []

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
     
        all_videos.append(video_frame['url'])
        
        
    
    except Exception as e:
        if videos == [[]]:
            print("Participant deleted their entire watch history")
            print(" ")
        else: 
            print(f"Error processing {json_file.name}: {e}")
            print(" ")
            
            
#Tidying up the dataframes - prune html files + remove duplicates          
          
all_videos_df = pd.concat(all_videos, ignore_index = True)
print(len(all_videos_df))  
all_videos_df = all_videos_df.drop_duplicates()
print(len(all_videos_df))
all_videos_df = all_videos_df.str.replace('}', '')
all_videos_df = all_videos_df.str.replace(']', '')
all_videos_df = all_videos_df.str.replace(',', '')
all_videos_df = all_videos_df.str.replace("'", "")
all_videos_df.to_csv('all_videos.csv')
print(all_videos_df.head())

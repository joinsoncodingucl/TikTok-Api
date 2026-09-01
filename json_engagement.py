"""
Purpose of this program is to extract enagement data (e.g. like/share/repost/favourite histories + follow list) from a participant's json file, and convert into dataframes containing all the participant's engagement data

Output is five dataframes, each containing one engagement category or following accounts
"""


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


#Empty list which will receive the data during the loop
all_likes = []
all_favs = []
all_reposts = []
all_follows = []
all_shares = []


"""
Processor function:
Goes through a dictionary of a participant's TikTok engagement data and extracts the relevant information
- First checks if the inputted file is labelled 'empty' - if it is, Processor does not proceed
- If the data is not 'empty', Processor checks if the inputted data contains follow information or other engagement content. 
- If the data is follows, Processor extracts all the relevant follow info (e.g. username, date followed)
- If the data comprises likes, shares, reposts or favourites, Processor extracts the video http and date it was engaged with.
- The extracted file is appended to the empty lists.
"""
def processor(tiktok_dict, save_name, empty_list):
    if tiktok_dict == ['empty']:
        print(f'No {save_name}')

    else:

        tiktok_dict.pop()
        tiktok_str = str(tiktok_dict)

        if save_name == 'follows':
            tiktok_follow_list = re.findall(r"'UserName':\s*'([^']+)'", tiktok_str)
            tiktok_date_list = re.findall(r"\d{4}-\d{2}-\d{2}", tiktok_str)
            tiktok_time_list = re.findall(r"\d{2}:\d{2}:\d{2}", tiktok_str)

            tiktok_frame = pd.DataFrame({
                "follow": tiktok_follow_list,
                "follow_date": tiktok_date_list,
                "follow_time": tiktok_time_list
            })

            tiktok_frame["follow_datetime"] = pd.to_datetime(
                tiktok_frame["follow_date"] + " " + tiktok_frame["follow_time"]
            )

            tiktok_frame = (
                tiktok_frame
                .sort_values("follow_datetime")
                .reset_index(drop=True)
            )


        else:

            tiktok_url_list = re.findall(r"https?://\S+", tiktok_str)
            tiktok_date_list = re.findall(r"\d{4}-\d{2}-\d{2}", tiktok_str)
            tiktok_time_list = re.findall(r"\d{2}:\d{2}:\d{2}", tiktok_str)

            tiktok_frame = pd.DataFrame({
                "url": tiktok_url_list,
                "watch_date": tiktok_date_list,
                "watch_time": tiktok_time_list
            })

            tiktok_frame["watch_datetime"] = pd.to_datetime(
                tiktok_frame["watch_date"] + " " + tiktok_frame["watch_time"]
            )

            tiktok_frame = (
                tiktok_frame
                .sort_values("watch_datetime")
                .reset_index(drop=True)
            )

        # Create output filename based on source file
        csv_name = json_file.stem + f'_{save_name}_history.csv'
        output_path = output_dir / csv_name
        tiktok_frame['PID'] = csv_name
        print(f"Saved: {output_path}")
        print(" ")
        empty_list.append(tiktok_frame)


"""
This is the program's central loop - all the files are looped through, with the relevant info extracted
- Unlike watch history - there is a potential for a participant to not have engaged with a video or follow anyone.
- Thus, we need to account for this by adding missing data into each json file - this ensures each file structure is identical. 
- Once this processing is done - each engagement data is converted to a list, and run through Processor.
- End point is that the previously empty lists are full of items for each of the processed dataframes
"""

for json_file in json_files:

    try:

        with open(json_file, "r", encoding="utf-8") as file:
            data = json.load(file)

        if 'tiktok_repost_history' not in (list(data[1].keys())):
            data.insert(1, {'tiktok_repost_history': 'empty'})

        if 'tiktok_favorite_videos' not in (list(data[2].keys())):
            data.insert(2, {'tiktok_favorite_videos': 'empty'})

        if 'tiktok_following' not in (list(data[3].keys())):
            data.insert(3, {'tiktok_following': 'empty'})

        if 'tiktok_like_list' not in (list(data[4].keys())):
            data.insert(4, {'tiktok_list_list': 'empty'})

        if 'tiktok_share_history' not in (list(data[5].keys())):
            data.insert(5, {'tiktok_share_history': 'empty'})

        repost_hist = data[1]

        fav_hist = data[2]

        follow_hist = data[3]

        like_hist = data[4]

        share_hist = data[5]

        likes = list(like_hist.values())

        favs = list(fav_hist.values())

        reposts = list(repost_hist.values())

        follows = list(follow_hist.values())
        
        shares = list(share_hist.values())
       

        processor(reposts, 'repost', all_reposts)
        processor(likes, 'like', all_likes)
        processor(favs, 'fav', all_favs)
        processor(follows, 'follows', all_follows)
        processor(shares, 'shares', all_shares)




    except Exception as e:
        if all_likes == [[]]:
            print("Participant deleted their entire like history")
            print(" ")
        if all_favs == [[]]:
            print("Participant deleted their entire fav history")
            print(" ")
        if all_reposts == [[]]:
            print("Participant deleted their entire repost history")
            print(" ")
        if all_shares == [[]]:
            print("Participant deleted their entire share history")
            print(" ")
        if all_follows == [[]]:
            print("Participant deleted their entire follow history")
            print(" ")
        else:
            print(f"Error processing {json_file.name}: {e}")
            print(" ")


"""
These concatinator functions convert the lists into dataframes + prune the URLs + save big dataframes
"""

def concatinator(full_list, save_name):
    all_videos_df = pd.concat(full_list, ignore_index=True)
    all_videos_df['url'] = all_videos_df['url'].str.replace('}', '')
    all_videos_df['url'] = all_videos_df['url'].str.replace(']', '')
    all_videos_df['url'] = all_videos_df['url'].str.replace(',', '')
    all_videos_df['url'] = all_videos_df['url'].str.replace("'", "")
    all_videos_df.to_csv(f'all_{save_name}_participant.csv')
    
def concatinator_follows(full_list, save_name):
    all_videos_df = pd.concat(full_list, ignore_index=True)
    all_videos_df.to_csv(f'all_{save_name}_participant.csv')


concatinator_follows(all_follows, 'follows')
concatinator(all_likes, 'likes')
concatinator(all_favs, 'favs')
concatinator(all_reposts, 'reposts')
concatinator(all_shares, 'shares')

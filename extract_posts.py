import csv
import instaloader
from datetime import datetime, timedelta
import json
import logging
import time 

logging.basicConfig(level=logging.INFO)

CONFIG_PATH = 'config.json'

def read_config():
    try:
        with open(CONFIG_PATH, 'r') as config_file:
            return json.load(config_file)
    except (FileNotFoundError, ValueError, KeyError) as e:
        logging.error(f"Error reading config: {e}")
        return {}

def update_last_pull_date_in_config():
    config_data = read_config()
    config_data["last_pull_date"] = datetime.now().strftime(config['date_format'])
    with open(CONFIG_PATH, 'w') as config_file:
        json.dump(config_data, config_file)


with open('secrets.json') as f:
    secrets = json.load(f)
username = secrets['instagram_username']
password = secrets['instagram_password']




def write_data_to_new_csv(accounts, date_cutoff, config):
    L = instaloader.Instaloader()
    L.context.log(username, password)
    L.context.login(username, password)
    L.download_comments = False
    L.download_videos = False
    L.download_video_thumbnails = False

    with open(config['csv_filename'], 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(config['fieldnames'])
        post_count = 0

        for account in accounts:
            profile = instaloader.Profile.from_username(L.context, account)
            posts = profile.get_posts()

            for i, post in enumerate(posts):
                if i >= config['num_posts_to_fetch'] or post.date <= datetime.now() - timedelta(days=config['days_limit']):
                    break

                if post.date <= date_cutoff:
                    continue

                writer.writerow([
                    account, "post", post.mediaid, post.caption, post.date, str(post.location),
                    f"https://www.instagram.com/p/{post.shortcode}", post.typename, post.mediacount,
                    post.caption_hashtags, post.caption_mentions, post.likes, post.comments, post.title, post.url
                ])

                logging.info(f"Extracted from account: {account}, Media ID: {post.mediaid}, Date: {post.date}")

                post_count += 1

                time.sleep(1)

                
            
        return post_count

if __name__ == "__main__":
    config = read_config()
    date_cutoff = datetime.strptime(config.get('last_pull_date', '01/01/01 00:00'), config['date_format'])


    if not date_cutoff:
        logging.error("Max datetime not found in config.json. Please set it.")
    else:
        post_count = write_data_to_new_csv(config['accounts_to_extract'], date_cutoff, config)  # Capture the returned post_count
        print(f"Posts extracted: {post_count}")
        update_last_pull_date_in_config()






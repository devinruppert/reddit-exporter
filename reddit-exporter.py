import praw
import csv
from datetime import datetime
import time

# Reddit API Configuration
# Get your credentials from: https://www.reddit.com/prefs/apps
REDDIT_CLIENT_ID = 'your_client_id_here'
REDDIT_CLIENT_SECRET = 'your_client_secret_here'
REDDIT_USER_AGENT = 'python:reddit_scraper:v1.0 (by /u/your_username)'

def scrape_subreddit(subreddit_name, start_date, end_date, limit=1000):
    """
    Scrape posts from a subreddit within a date range
    
    Args:
        subreddit_name: Name of subreddit (without r/)
        start_date: Start date as datetime object
        end_date: End date as datetime object
        limit: Maximum number of posts to fetch (default 1000)
    """
    # Initialize Reddit instance
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT
    )
    
    subreddit = reddit.subreddit(subreddit_name)
    
    # Convert dates to Unix timestamps
    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())
    
    posts_data = []
    
    print(f"Scraping r/{subreddit_name}...")
    
    # Fetch posts (using 'new' to get chronological order)
    for post in subreddit.new(limit=limit):
        post_time = int(post.created_utc)
        
        # Check if post is within date range
        if start_timestamp <= post_time <= end_timestamp:
            # Get all comments (this expands comment trees)
            post.comments.replace_more(limit=0)
            
            # Collect post data
            post_data = {
                'post_id': post.id,
                'post_title': post.title,
                'post_author': str(post.author),
                'post_created_utc': datetime.fromtimestamp(post.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
                'post_score': post.score,
                'post_upvote_ratio': post.upvote_ratio,
                'post_num_comments': post.num_comments,
                'post_text': post.selftext,
                'post_url': post.url,
                'comment_id': '',
                'comment_author': '',
                'comment_text': '',
                'comment_score': '',
                'comment_created_utc': ''
            }
            
            # Add the main post as a row
            posts_data.append(post_data.copy())
            
            # Add each comment as a separate row
            for comment in post.comments.list():
                if hasattr(comment, 'body'):  # Ensure it's a comment, not a MoreComments object
                    comment_data = post_data.copy()
                    comment_data['comment_id'] = comment.id
                    comment_data['comment_author'] = str(comment.author)
                    comment_data['comment_text'] = comment.body
                    comment_data['comment_score'] = comment.score
                    comment_data['comment_created_utc'] = datetime.fromtimestamp(comment.created_utc).strftime('%Y-%m-%d %H:%M:%S')
                    posts_data.append(comment_data)
            
            print(f"Scraped: {post.title[:50]}... ({post.num_comments} comments)")
            
            # Be respectful of rate limits
            time.sleep(0.5)
        
        elif post_time < start_timestamp:
            # Posts are getting too old, stop searching
            break
    
    return posts_data

def save_to_csv(data, filename):
    """Save scraped data to CSV file"""
    if not data:
        print("No data to save!")
        return
    
    keys = data[0].keys()
    
    with open(filename, 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)
    
    print(f"\nData saved to {filename}")
    print(f"Total rows: {len(data)}")

# Example usage
if __name__ == "__main__":
    # Configure your scraping parameters
    SUBREDDIT = 'python'  # Change to your target subreddit
    START_DATE = datetime(2024, 11, 1)  # Start date
    END_DATE = datetime(2024, 11, 30)    # End date
    OUTPUT_FILE = 'reddit_data.csv'
    
    # Scrape the data
    scraped_data = scrape_subreddit(SUBREDDIT, START_DATE, END_DATE, limit=1000)
    
    # Save to CSV
    save_to_csv(scraped_data, OUTPUT_FILE)
import pandas as pd
import streamlit as st
import mysql.connector
import pymongo
from pymongo import MongoClient
from googleapiclient.discovery import build
# Add icons for each section
icons = {
    "Central Hub": "🌐",
    "Data Collection & Processing": "📊",
    "SQL Query Generator": "🔍"
}
# Add the YouTube logo as the title
youtube_logo_url = "https://www.logo.wine/a/logo/YouTube/YouTube-Icon-Full-Color-Logo.wine.svg"
st.sidebar.image(youtube_logo_url, use_column_width=True)


# Use Markdown to format the title with a custom font size and color
st.sidebar.markdown("<h1 style='color: red; font-size: 25px;'>  😊🎥YouTube Data Harvesting & Warehousing</h1>", unsafe_allow_html=True)

# Create a radio button to select sections
selected  = st.sidebar.radio("", list(icons.keys()), format_func=lambda option: f"{icons[option]} {option}")
# Display the selected section's icon and label
st.sidebar.markdown(f"📂 Category Picker: {icons[selected]} {selected}")

# Bridging a connection with MongoDB  and Creating a new database(youtube)
client = pymongo.MongoClient("mongodb+srv://sherinjoyabraham:IJoAVOGAEeYWdr8N@cluster0.onjbvih.mongodb.net/?retryWrites=true&w=majority")
db = client.youtube_joy

# CONNECTING WITH MYSQL DATABASE
mydb = mysql.connector.connect(host="localhost",
                               user="root",
                               password="stc123",
                               database="youtube_joy"
                               )
mycursor = mydb.cursor(buffered=True)

# BUILDING CONNECTION WITH YOUTUBE API
api_key = "AIzaSyDCMbCXZPHS102yWfP505zjVJvPzB1jWX4"
youtube = build('youtube', 'v3', developerKey=api_key)


# FUNCTION TO GET CHANNEL DETAILS
def get_channel_details(channel_id):
    ch_data = []
    response = youtube.channels().list(part='snippet,contentDetails,statistics',
                                       id=channel_id).execute()

    for i in range(len(response['items'])):
        data = dict(Channel_id=channel_id[i],
                    Channel_name=response['items'][i]['snippet']['title'],
                    Playlist_id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers=response['items'][i]['statistics']['subscriberCount'],
                    Views=response['items'][i]['statistics']['viewCount'],
                    Total_videos=response['items'][i]['statistics']['videoCount'],
                    Description=response['items'][i]['snippet']['description'],
                    Country=response['items'][i]['snippet'].get('country')
                    )
        ch_data.append(data)
    return ch_data


# FUNCTION TO GET VIDEO IDS
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id,
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id,
                                           part='snippet',
                                           maxResults=50,
                                           pageToken=next_page_token).execute()

        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


# FUNCTION TO GET VIDEO DETAILS
import re
from datetime import datetime
import isodate

def get_video_details(v_ids):
    video_stats = []

    for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(v_ids[i:i + 50])).execute()
        for video in response['items']:
            # Extract minutes and seconds from the ISO 8601 duration using regex
            duration_iso = video['contentDetails']['duration']
            match = re.match(r'PT(\d+)M(\d+)S', duration_iso)
            if match:
                minutes = int(match.group(1))
                seconds = int(match.group(2))
                duration_seconds = minutes * 60 + seconds
            else:
                duration_seconds = None  # Handle invalid duration format gracefully

            # Convert published date to a Pandas datetime object
            published_date_iso = video['snippet']['publishedAt']
            try:
                published_date = pd.to_datetime(published_date_iso, format='%Y-%m-%dT%H:%M:%SZ')
            except pd.errors.OutOfBoundsDatetime:
                # Handle any invalid date formats or other errors here
                # For example, you can replace invalid dates with NaT (Not-a-Time)
                published_date = pd.NaT

            video_details = dict(
                Channel_name=video['snippet']['channelTitle'],
                Channel_id=video['snippet']['channelId'],
                Video_id=video['id'],
                Title=video['snippet']['title'],
                Thumbnail=video['snippet']['thumbnails']['default']['url'],
                Description=video['snippet']['description'],
                Published_date=published_date,
                Duration_seconds=duration_seconds,
                Views=video['statistics']['viewCount'],
                Likes=video['statistics'].get('likeCount'),
                Comments=video['statistics'].get('commentCount'),
                Favorite_count=video['statistics']['favoriteCount'],
                Definition=video['contentDetails']['definition'],
                Caption_status=video['contentDetails']['caption']
            )
            video_stats.append(video_details)
    return video_stats


# FUNCTION TO GET COMMENT DETAILS
def get_comments_details(v_id):
    comment_data = []
    for i in v_id:
        try:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                     videoId=i,
                                                     maxResults=100).execute()
            for cmt in response['items']:
                data = dict(Comment_id=cmt['id'],
                            Video_id=cmt['snippet']['videoId'],
                            Comment_text=cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author=cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date=cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count=cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count=cmt['snippet']['totalReplyCount']
                            )
                comment_data.append(data)
        except:
            pass
    return comment_data
# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():
    ch_name = []
    for i in db.channel_details.find():
        ch_name.append(i['Channel_name'])
    return ch_name


# HOME PAGE
if selected == "Central Hub":
    
    st.title(":red[🎥YouTube Data Harvesting & Warehousing]")

    # Section: About
    st.markdown("## :green[📚About]")
    st.write("<span style='font-size: 24px;'> 👋 Welcome to the world of data exploration and warehousing !!</span>", unsafe_allow_html=True)

    # Section: Overview
    st.markdown("## :orange[🚀Project Overview]")
    st.write("<span style='font-size: 24px;'> In this project , we embark on a journey to harness the power of YouTube data 🎥📈. We collect valuable insights from channels using the YouTube Data API 🔍📊 and create a robust data warehousing system 💾🏢.</span>", unsafe_allow_html=True)

    # Section: Technologies
    st.markdown("## :blue[📱Technologies]")
    st.write(" <span style='font-size: 24px;'> Our toolkit includes Python 🐍, MongoDB 🏦, YouTube Data API 🔍, MySQL 🐬, and Streamlit 🚀. These technologies come together to make this project possible.</span>", unsafe_allow_html=True)

    # Section: Mission
    st.markdown("## :green[🔮Mission]")
    st.write("<span style='font-size: 24px;'> Our mission is to 🌾 harvest, 📦 organize, and 🌟 empower data. We transform raw YouTube data into meaningful information 📊💡, making it accessible 🌈 and insightful 🧠.</span>", unsafe_allow_html=True)

    # Section: Vision
    st.markdown("## :red[🔭Vision]")
    st.write("<span style='font-size: 24px;'> Our vision is to 🌟 empower decision-makers with real-time 📈 analytics and trends from YouTube. We aim to provide a user-friendly interface to explore the vast world of video content 📺🌐.</span>", unsafe_allow_html=True)

    # Section: Join Us
    st.markdown("## :orange[🤝Join Us]")
    st.write("<span style='font-size: 24px;'>Join us on this exciting journey as we explore the depths of YouTube data! </span>", unsafe_allow_html=True)

    
        
    
    # EXTRACT AND TRANSFORM PAGE
if selected == "Data Collection & Processing":
    tab1, tab2 = st.tabs(["📥Data Collection", "🤖Processing"])

    # GET DATA TAB
    with tab1:
        st.markdown("#    ")
        st.write("## :red[😎 paste the channel_id here :]")
        ch_id = st.text_input( "").split(',')

        if ch_id and st.button(":blue[Information Extraction 🙂]"):
            ch_details = get_channel_details(ch_id)
            st.write(f'#### Extracted data from :green["{ch_details[0]["Channel_name"]}"] channel')
            st.table(ch_details)

        if st.button(":green[🚚Shift to MongoDB 📦]"):
            with st.spinner(':orange[🌈 Enter the Wonderland of Data..]'):
                ch_details = get_channel_details(ch_id)
                v_ids = get_channel_videos(ch_id)
                vid_details = get_video_details(v_ids)
                comm_details = get_comments_details(v_ids)

                

                collections1 = db.channel_details
                collections1.insert_many(ch_details)

                collections2 = db.video_details
                collections2.insert_many(vid_details)

                collections3 = db.comments_details
                collections3.insert_many(comm_details)
                st.success(":green[Upload to MongoDB: Mission Successful! 🌠]")
                

                # TRANSFORM TAB
    with tab2:
        st.markdown("#   ")
        st.markdown("### :blue[🚀 Choose a Channel for SQL Transformation]")

        ch_names = channel_names()
        user_inp = st.selectbox(":orange[Select channel 🙂]", options=ch_names)
        
    
        def table_exists(table_name):
            # Check if the table exists
            mycursor.execute("SHOW TABLES LIKE %s", (table_name,))
            return mycursor.fetchone() is not None

        def create_table(table_name, create_query):
            # Create a table if it doesn't exist
            if not table_exists(table_name):
                mycursor.execute(create_query)
                print(f"Table '{table_name}' created.")

        # Create channel_details table
        channel_details_query = (
            "CREATE TABLE cha ("
            "channel_id VARCHAR(255), "
            "channel_name VARCHAR(255), "
            "Playlist_id VARCHAR(255), "
            "subscription_count INT, "
            "views INT, "
            "Total_videos INT, "
            "channel_description TEXT, "
            "country TEXT);"
        )
        create_table("cha", channel_details_query)

        # Create video_details table
        video_details_query = (
            "CREATE TABLE vide ("
            "channel_name VARCHAR(255), "
            "channel_id VARCHAR(255), "
            "video_id VARCHAR(255), "
            "title VARCHAR(255), "
            "thumbnail VARCHAR(255), "
            "video_description TEXT, "
            "published_date VARCHAR(255), "
            "Duration VARCHAR(255), "
            "views INT, "
            "likes INT, "
            "comments INT, "
            "favorite_count INT, "
            "Definition VARCHAR(255), "
            "Caption_status VARCHAR(255));"
        )
        create_table("vide", video_details_query)

        # Create comments_details table
        comments_details_query = (
            "CREATE TABLE comme ("
            "comment_Id VARCHAR(255), "
            "video_Id VARCHAR(255), "
            "comment_text TEXT, "
            "comment_author VARCHAR(255), "
            "comment_posted_date VARCHAR(255), "
            "like_count INT, "
            "Reply_count INT);"
        )
        create_table("comme", comments_details_query)

       #Insert channel details into mysql
        def insert_into_cha():
            collections = db.channel_details
            query = """INSERT INTO cha VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""

            for i in collections.find({"Channel_name": user_inp}, {'_id': 0}):
                mycursor.execute(query, tuple(i.values()))
                mydb.commit()

        #Insert video details into mysql
        def insert_into_vide():
            collections1 = db.video_details
            query1 = """INSERT INTO vide VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

            for i in collections1.find({"Channel_name": user_inp}, {'_id': 0}):
                mycursor.execute(query1, tuple(i.values()))
                mydb.commit()

        #Insert comment details into mysql
        def insert_into_comme():
            collections1 = db.video_details
            collections2 = db.comments_details
            query2 = """INSERT INTO comme VALUES(%s,%s,%s,%s,%s,%s,%s)"""

            for vid in collections1.find({"Channel_name": user_inp}, {'_id': 0}):
                for i in collections2.find({'Video_id': vid['Video_id']}, {'_id': 0}):
                    mycursor.execute(query2, tuple(i.values()))
                    mydb.commit()

         #insert all the details into mysql
        if st.button(":violet[The Big Send-off 🚀]"):
            try:
                insert_into_cha()
                insert_into_vide()
                insert_into_comme()

                st.success("Mission MySQL Accomplished!! 🪄✨")
                

            except:
                st.error("✅ Channel Details Transformation Done!!")

#sql 10 queries
if selected == "SQL Query Generator":

    st.write("## :red[🤔 Explore Questions Selection❓]")
    questions = st.selectbox(':green[Question Quest 🧐❓]',
                             ['1. What are the names of all the videos and their corresponding channels?',
                              '2. Which channels have the most number of videos, and how many videos do they have?',
                              '3. What are the top 10 most viewed videos and their respective channels?',
                              '4. How many comments were made on each video, and what are their corresponding video names?',
                              '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                              '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                              '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                              '8. What are the names of all the channels that have published videos in the year 2022?',
                              '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                              '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT title AS Video_Title, channel_name AS Channel_Name
                            FROM vide
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, total_videos AS Total_Videos
                            FROM cha
                            ORDER BY total_videos DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)


    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, title AS Video_Title, views AS Views 
                            FROM vide
                            ORDER BY views DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)


    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT a.video_id AS Video_id, a.title AS Video_Title, b.Total_Comments
                            FROM vide AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comme GROUP BY video_id) AS b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,title AS Title,likes AS Likes_Count 
                            FROM vide
                            ORDER BY likes DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)


    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT title AS Title, likes AS Likes_Count
                            FROM vide
                            ORDER BY likes DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, views AS Views
                            FROM cha
                            ORDER BY views DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)


    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT channel_name,published_date
                            FROM vide
                            WHERE YEAR(published_date) = 2022""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,
                            AVG(duration)/60 AS "Average_Video_Duration (mins)"
                            FROM vide
                            GROUP BY channel_name
                            ORDER BY AVG(duration)/60 DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)



    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,video_id AS Video_ID,comments AS Comments
                            FROM vide
                            ORDER BY comments DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

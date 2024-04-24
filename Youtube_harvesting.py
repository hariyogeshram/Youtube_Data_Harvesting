
#                                                           Importing Libraries 

import mysql.connector
import os
import googleapiclient.discovery
from googleapiclient.errors import HttpError
import json
import pandas as pd
import isodate
from datetime import time
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns

# ------------------------------------------------------------------------------------------------------------------------------

#                                                           API Connections
def Api():

    # api = "AIzaSyBKDTLZrBHl_sv9oxJhDlSCiqV-Bu57Mw0"
    # api = "AIzaSyCyQRDdPf43lhTeiDZnu8t_bPEWDmbfokE"
    # api  = "AIzaSyDTnfWSKSLBxDrvbaWgbYU3OmEJ1XON5As"
    # api  = "AIzaSyC08IQfBdqdxN5WKHNUYppm5uiIryz091o"
    api = "AIzaSyAQ_1wcuCYHmdRLYrCqO3D-Nvwx8MXn0-o"      # give Your API Key here
    api_service_name = "youtube"
    api_version = "v3"  

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=api)

    return youtube

youtube = Api()

# ------------------------------------------------------------------------------------------------------------------------------

#                                                          MySQL Connections
mydb=mysql.connector.connect(
    host = '127.0.0.1', 
    port = '3306',
    user = 'root',   # give your username
    password = 'Sharma@45177', # give your password
    database = 'Youtube'  # give your database name
    
)

cursor = mydb.cursor()

# ------------------------------------------------------------------------------------------------------------------------------

#                                                           Channel Details 
def channel_info(id):

    cursor.execute("""CREATE TABLE IF NOT EXISTS channel (
                        channel_Name VARCHAR(255),
                        channel_Id VARCHAR(255) PRIMARY KEY,
                        subscribers INT,
                        views INT,
                        Total_videos INT,
                        channel_description TEXT,
                        Playlist_Id VARCHAR(255)
                    )""")
    
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=id)

    response = request.execute()

    for i in response.get('items', []):

        data = dict(channel_Name=i['snippet']['title'],
                    channel_Id=i['id'],
                    subscribers=i['statistics']['subscriberCount'],
                    views=i['statistics']['viewCount'],
                    Total_videos=i['statistics']['videoCount'],
                    channel_description=i['snippet']['description'],
                    Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])
        
        try:

            channel_Id = data['channel_Id']

            cursor.execute("SELECT * FROM channel WHERE channel_Id = %s", (channel_Id,))

            result = cursor.fetchone()

            if not result:

                sql = """INSERT INTO channel (channel_Name, channel_Id, subscribers, views, Total_videos, channel_description, Playlist_Id) 
                         VALUES (%s, %s, %s, %s, %s, %s, %s)"""

                values = (data['channel_Name'], data['channel_Id'], data['subscribers'], data['views'], data['Total_videos'], data['channel_description'], data['Playlist_Id'])

                cursor.execute(sql, values)

                st.success("Data inserted successfully!")

            else:

                st.warning("Record already exists in the table")

        except mysql.connector.Error as error:

            print("Failed to insert record into channel_info table {}".format(error))

    mydb.commit()

    return data

# ------------------------------------------------------------------------------------------------------------------------------

#                                                     get Video IDs

def get_Videos_Ids(channel_id):

    Video_Ids=[]
    
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()

    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    next_page_token=None
    
    while True:

        response1=youtube.playlistItems().list(
                                        part='snippet',
                                        playlistId=Playlist_Id,
                                        maxResults=50,
                                        pageToken=next_page_token).execute()

        for i in range(len(response1['items'])):

            Video_Ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])

        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:

            break

    mydb.commit()

    return Video_Ids 
    
# ------------------------------------------------------------------------------------------------------------------------------


#                                                      get video information
def get_video_info(Video_Ids):

    video_data=[]

    for video_id in Video_Ids:

        request=youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id
        )

        response=request.execute()
        
        cursor.execute("""CREATE TABLE IF NOT EXISTS video (
                            channel_Name VARCHAR(255),
                            channel_Id VARCHAR(255),
                            video_Id VARCHAR(255) PRIMARY KEY,
                            Title VARCHAR(255),
                            Tags TEXT,
                            Thumbnail TEXT,
                            Description TEXT,
                            Published_date VARCHAR(255),
                            Duration VARCHAR(255),
                            views INT,
                            likes INT,
                            comments INT,
                            Favorite_Count INT 
                        )""")

        for item in response["items"]:

            # Duration in secounds :
            duration_iso8601 = item['contentDetails']['duration']
            duration_seconds = isodate.parse_duration(duration_iso8601).total_seconds()

            # Convert the total seconds to hours, minutes, and seconds
            hours = int(duration_seconds // 3600)
            minutes = int((duration_seconds % 3600) // 60)
            seconds = int(duration_seconds % 60)

            # Create a time object
            duration_time = time(hours, minutes, seconds)
            
            # If tags are not present, set to an NULL
            tags = json.dumps(item['snippet'].get('tags')) if item['snippet'].get('tags') else None

            data=dict(channel_Name=item['snippet']['channelTitle'],
                     channel_Id=item['snippet']['channelId'],
                     video_Id=item['id'],
                     Title=item['snippet']['title'],
                     Tags=tags,
                     Thumbnail=json.dumps(item['snippet']['thumbnails']),
                     Description=item['snippet'].get('description'),
                     Published_date=item['snippet']['publishedAt'].replace('T', ' ').replace('Z', ''),
                     Duration=duration_time,
                     views=item['statistics'].get('viewCount'),
                     likes=item['statistics'].get('likeCount'),
                     comments=item['statistics'].get('commentCount'),
                     Favorite_Count=item['statistics'].get('favoriteCount')
                     )

            video_data.append(data)

            # Insert data into MySQL
            cursor.execute("INSERT INTO video (channel_Name, channel_Id, video_Id, Title, Tags, Thumbnail, Description, Published_date, Duration, views, likes, comments, Favorite_Count) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                           (data['channel_Name'], data['channel_Id'], data['video_Id'], data['Title'], data['Tags'], data['Thumbnail'], data['Description'], data['Published_date'], data['Duration'], data['views'], data['likes'], data['comments'], data['Favorite_Count']))

            mydb.commit()

    return video_data


# ------------------------------------------------------------------------------------------------------------------------------

#                                                         get playlist details

def get_playlist_details(channel_id):

    all_data = []

    try:

        cursor.execute("""CREATE TABLE IF NOT EXISTS playlist(
                            Playlist_Id VARCHAR(255) PRIMARY KEY,
                            channel_Id VARCHAR(255),
                            channel_Name VARCHAR(255),
                            playlist_name VARCHAR(255)
                        )""")

        request = youtube.playlists().list(
            part="snippet",
            channelId=channel_id,
            maxResults=50
        )

        response = request.execute()

        for item in response['items']:

            Playlist_Id = item['id']
            channel_Name = item['snippet']['channelTitle']
            playlist_name = item['snippet']['title']

            data = {
                "Playlist_Id": Playlist_Id,
                "channel_Id": channel_id,
                "channel_Name": channel_Name,
                "playlist_name": playlist_name
            }

            all_data.append(data)

            # Insert data into MySQL
            cursor.execute("INSERT INTO playlist (Playlist_Id, channel_Id, channel_Name, playlist_name) VALUES (%s, %s, %s, %s)",
                           (Playlist_Id, channel_id, channel_Name, playlist_name))

            mydb.commit()

    except Exception as e:
        print(f"Error: {e}")

    return all_data

# ------------------------------------------------------------------------------------------------------------------------------


#                                                     get comment details

def get_comment_info(video_ids):

    comment_data=[]

    try:

        cursor.execute("""CREATE TABLE IF NOT EXISTS comment (
                            comment_Id VARCHAR(255) PRIMARY KEY,
                            video_Id VARCHAR(255),
                            comment_Text TEXT,
                            comment_Author VARCHAR(255),
                            comment_Published VARCHAR(255)
                        )""")
        
        for video_id in video_ids:

            request=youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=50
            )

            response=request.execute()

            for item in response['items']:

                data=dict(comment_Id=item['snippet']['topLevelComment']['id'],
                         video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                         comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                         comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                         comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'].replace('T', ' ').replace('Z', ''))

                comment_data.append(data)

                # Insert data into MySQL
                cursor.execute("INSERT INTO comment (comment_Id, video_Id, comment_Text, comment_Author, comment_Published) VALUES (%s, %s, %s, %s, %s)",
                               (data['comment_Id'], data['video_Id'], data['comment_Text'], data['comment_Author'], data['comment_Published']))

                mydb.commit()

    except Exception as e:

        error_message = e.content.decode('utf-8')

        if 'commentsDisabled' in error_message:

            print("Comments are disabled for this video.")

            return []

        else:

            print("An error occurred:", error_message)

            return None
    
    return comment_data


# ------------------------------------------------------------------------------------------------------------------------------


def get_channel_details(channel_id):

    channel_details = channel_info(channel_id)
    Video_Ids = get_Videos_Ids(channel_id)
    video_details = get_video_info(Video_Ids)
    playlist_details = get_playlist_details(channel_id)
    comment_details = get_comment_info(Video_Ids)
    
    # Convert dictionaries to DataFrames
    channel_df = pd.DataFrame([channel_details])
    video_df = pd.DataFrame(video_details)
    playlist_df = pd.DataFrame(playlist_details)
    comment_df = pd.DataFrame(comment_details)
    
    return {
        "channel_details": channel_df,
        "video_details": video_df,
        "playlist_details": playlist_df,
        "comment_details": comment_df
    }

# #-------------------------------------------------------------------------------------------------------------------------

#                                          Function to fetch and return Youtube details as DataFrame                           

# mysql connection
connection =mysql.connector.connect(
    host = '127.0.0.1', 
    port = '3306',
    user = 'root',   # give your username
    password = 'Sharma@45177', # give your password
    database = 'Youtube'  # give your database name 
    
)

cursor = connection.cursor()

# Function to fetch and return channel details as DataFrame
def get_channel_data():

    cursor.execute('SELECT * FROM channel')
    channel_data = cursor.fetchall()
    channel_df = pd.DataFrame(channel_data, columns=[i[0] for i in cursor.description])
    return channel_df

# Function to fetch and return playlist details as DataFrame
def get_playlist_data():

    cursor.execute('SELECT * FROM playlist')
    playlist_data = cursor.fetchall()
    playlist_df = pd.DataFrame(playlist_data, columns=[i[0] for i in cursor.description])
    return playlist_df

# Function to fetch and return video details as DataFrame
def get_video_data():

    cursor.execute('SELECT * FROM video')
    video_data = cursor.fetchall()
    video_df = pd.DataFrame(video_data, columns=[i[0] for i in cursor.description])
    return video_df

# Function to fetch and return comment details as DataFrame
def get_comment_data():

    cursor.execute('SELECT * FROM comment')
    comment_data = cursor.fetchall()
    comment_df = pd.DataFrame(comment_data, columns=[i[0] for i in cursor.description])
    return comment_df

# # ------------------------------------------------------------------------------------------------------------------------------

#                                                            Streamlit UI Part   
def main():

    st.sidebar.title("Navigations")

    option = st.sidebar.radio("Select an option", ["Home", "Add data to MySQL", "View Tables", "Queries"])

    if option == "Home":

        st.title(":blue[YOUTUBE HARVESTING AND DATA WAREHOUSING]")
        st.write(
            """This project aims to develop a user-friendly Streamlit application, 
               That utilizes the Google API to extract information on a YouTube channel,
               Stores it in a SQL database(MySQL),
               It enables users to search for channel details and join tables to view data in the Streamlit app.
            """
        )
        st.header(":red[Technologies Used :]")

        st.markdown("- PYTHON")
        st.markdown("- MySQL")
        st.markdown("- STREAMLIT")

    elif option == "Add data to MySQL":

        st.title(':red[YouTube Channel Details]')

        channel_id = st.text_input(':blue[Enter YouTube Channel ID:]')

        if st.button('Store and collect Data'):

            details = get_channel_details(channel_id)

            st.subheader('Channel Details')
            st.write(details["channel_details"])

            st.subheader('Video Details')
            st.write(details["video_details"])

            st.subheader('Playlist Details')
            st.write(details["playlist_details"])

            st.subheader('Comment Details')
            st.write(details["comment_details"])

    # View Tables in Streamlit
    elif option == 'View Tables':

        st.subheader(":blue[Select the Table to display the Youtube Details]")

        selected_table = st.selectbox(":red[Select a table]", ["Channel", "Playlist", "Video", "Comment"])

        if selected_table == "Channel":

            st.header(":green[Channel Data :]")
            channel_df = get_channel_data()
            st.dataframe(channel_df)

        elif selected_table == "Playlist":

            st.header(":green[Playlist Data :]")
            playlist_df = get_playlist_data()
            st.dataframe(playlist_df)

        elif selected_table == "Video":

            st.header(":green[Video Data :]")
            video_df = get_video_data()
            st.dataframe(video_df)

        elif selected_table == "Comment":

            st.header(":green[Comment Data :]")
            comment_df = get_comment_data()
            st.dataframe(comment_df)

    
    # Query for all the 10 questions
    elif option == "Queries":

        st.session_state.page = 'questions_page'
        questions_page()
        
def questions_page():

    questions = [
        "1) Names of all the videos and their corresponding channels",
        "2) Channels with the most number of videos and how many videos they have",
        "3) Top 10 most viewed videos and their respective channels",
        "4) Number of comments for each video and their corresponding video names",
        "5) Videos with the highest number of likes and their corresponding channel names",
        "6) Total number of likes for each video and their corresponding video names",
        "7) Total number of views for each channel and their corresponding channel names",
        "8) Names of all the channels that have published videos in the year 2022",
        "9) Average duration of all videos in each channel and their corresponding channel names",
        "10) Videos with the highest number of comments and their corresponding channel names"
    ]

    selected_question = st.selectbox(":red[Select a question :]", questions, key="selectbox_unique_key")

    # Fetch data based on the selected question
    if st.button('Submit'):

        if selected_question == questions[0]:

            cursor.execute("SELECT  Title, channel_Name FROM video")

            data = cursor.fetchall()

            df = pd.DataFrame(data, columns=['video Title','Channel_Name'])

            st.write(df)


        elif selected_question == questions[1]:

            cursor.execute("SELECT channel_Name, COUNT(*) as video_count FROM video GROUP BY channel_Name ORDER BY video_count DESC")

            data = cursor.fetchall()

            df = pd.DataFrame(data, columns=['Channel_Name', 'No of videos'])

            st.write(df)

            # Create a bar chart using Matplotlib
            fig, ax = plt.subplots()
            plt.bar(df['Channel_Name'], df['No of videos'])
            plt.xlabel('Channel_Name')
            plt.ylabel('No of videos')
            plt.title('No of videos by channel')
            plt.xticks(rotation=45, ha='right', fontsize=8)
            st.pyplot(fig)



        elif selected_question == questions[2]:

            cursor.execute("SELECT Title, channel_Name, views FROM video ORDER BY views DESC LIMIT 10")

            data = cursor.fetchall()

            df = pd.DataFrame(data, columns=['Video_Title', 'Channel name','View Count'])

            st.write(df)

            # Create a bar chart using Matplotlib
            fig, ax = plt.subplots()
            plt.bar(df['Video_Title'], df['View Count'])
            plt.xlabel('Video_Title')
            plt.ylabel('View Count')
            plt.title('Top 10 Videos by Views')
            plt.xticks(rotation=50, ha='right', fontsize=8)
            st.pyplot(fig)


        elif selected_question == questions[3]:

            # cursor.execute("SELECT Title, COUNT(*) as comments FROM video GROUP BY Title")

            cursor.execute( """
                SELECT Title, COUNT(*) AS comment_count
                FROM comment AS c
                JOIN video AS v
                ON c.video_id = v.video_id
                GROUP BY v.video_id, v.Title
            """)

            data = cursor.fetchall()

            df = pd.DataFrame(data, columns=['Video Title', 'No of comments'])

            st.write(df)


        elif selected_question == questions[4]:

            cursor.execute("""
                SELECT v.Title, v.likes, v.channel_Name
                FROM video AS v
                JOIN (
                    SELECT MAX(likes) AS max_likes
                    FROM video
                ) AS max_likes_table
                ON v.likes = max_likes_table.max_likes
            """)

            data = cursor.fetchall()

            df = pd.DataFrame(data, columns=['Video Title', 'Likes', 'Channel Name'])

            st.write(df)

            # Create a bar chart using Matplotlib
            fig, ax = plt.subplots()
            plt.bar(df['Video Title'], df['Likes'], color='green')
            plt.xlabel('Video Title')
            plt.ylabel('Number of Likes')
            plt.title('Videos with the Highest Number of Likes')
            plt.xticks(rotation=45, ha='right', fontsize=8)
            plt.tight_layout()

            # Show the plot in Streamlit
            st.pyplot(fig)


        elif selected_question == questions[5]:

            cursor.execute("SELECT Title, SUM(likes) as total_likes FROM video GROUP BY Title")

            data = cursor.fetchall()

            df = pd.DataFrame(data, columns=['Video Title','Like Count'])

            st.write(df)

        # Create a grouped bar chart using Matplotlib

            fig, ax = plt.subplots()
            width = 0.35  # the width of the bars
            ind = range(len(df))  # the x locations for the groups
            ax.bar(ind, df['Like Count'], width, label='Likes Count', color='blue')
            ax.set_xlabel('Video Title')
            ax.set_ylabel('Count')
            ax.set_title('Likes per Video')
            ax.legend()
            st.pyplot(fig)

        elif selected_question == questions[6]:
            
            cursor.execute("SELECT channel_Name, SUM(views) as total_views FROM video GROUP BY channel_Name")

            data = cursor.fetchall()

            df = pd.DataFrame(data, columns=['Channel Name', 'No of views'])

            st.write(df)

            # Create a bar chart using Matplotlib

            fig, ax = plt.subplots()
            plt.bar(df['Channel Name'],df['No of views'], color= 'blue')
            plt.xlabel('Channel Name')
            plt.ylabel('No of views')
            plt.title('Total Views per channel')
            plt.xticks(rotation=45, ha='right', fontsize=8)
            plt.tight_layout()
            st.pyplot(fig)

        elif selected_question == questions[7]:

            cursor.execute("SELECT DISTINCT channel_Name FROM video WHERE YEAR(Published_date) = 2022")

            data = cursor.fetchall()

            df = pd.DataFrame(data, columns=['Channel Name'])

            st.write(df)

            # Count the number of unique channels
            num_channels = len(df)
            
            # Create a horizontal bar chart using Matplotlib
            fig, ax = plt.subplots(figsize=(8, num_channels * 0.5))  # Adjust height based on the number of channels
            ax.barh(df['Channel Name'], 1, color='skyblue')  # Bar width set to 1
            ax.set_xlabel('Count')
            ax.set_ylabel('Channel Name')
            ax.set_title('Unique Channels in 2022')
            st.pyplot(fig)
            

        elif selected_question == questions[8]:

            cursor.execute("""SELECT channel_Name, AVG(TIME_TO_SEC(SUBSTRING(duration, 1, 2)) * 3600 + 
                                             TIME_TO_SEC(SUBSTRING(duration, 4, 2)) * 60 + 
                                             TIME_TO_SEC(SUBSTRING(duration, 7))) AS avg_duration_seconds 
                  FROM video 
                  GROUP BY channel_Name """)



            data = cursor.fetchall()

            df = pd.DataFrame(data, columns=['Channel Name', 'Average Duration'])

            st.write(df)

            # Set the font to one that includes the required glyphs
            plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']

            # Specify a font that supports Tamil characters
            plt.rcParams["font.family"] = "Arial Unicode MS"  # You can replace "Arial Unicode MS" with any font that supports Tamil characters
            
            # Create a vertical bar chart using Matplotlib
            
            fig, ax = plt.subplots(figsize=(10, 6))  # Set figure size (width, height)
            ax.bar(df['Channel Name'], df['Average Duration'], color='skyblue')  # Corrected column name

            ax.set_xlabel('Channel Name')
            ax.set_ylabel('Average Duration (in minutes)')  # Update y-axis label

            ax.set_title('Average Duration of Videos per Channel')

            plt.xticks(rotation=45, ha='right', fontsize=8)  # Rotate x-axis labels and adjust fontsize
            plt.tight_layout()  # Adjust layout to prevent overlapping labels

            st.pyplot(fig)


        
        elif selected_question == questions[9]:

            cursor.execute('''select Title as video_name, channel_Name as channel_name, comments as comments from video where comments is
                not null order by comments desc LIMIT 1''')
            
            data = cursor.fetchall()

            df = pd.DataFrame(data, columns=['video_name', 'Channel Name', 'No of Counts'])

            st.write(df)

            # Create a bar chart using Matplotlib
            fig, ax = plt.subplots(figsize=(8, 6))
            ax.bar(df['Channel Name'], df['No of Counts'], color='skyblue')
            ax.set_xlabel('Channel Name')
            ax.set_ylabel('No of Counts')
            ax.set_title('Comment Count for Video with Most Comments')
            plt.xticks(rotation=45, ha='right', fontsize=8)
            plt.tight_layout()
            st.pyplot(fig)

        data = cursor.fetchall()

        df = pd.DataFrame()

        st.write()
    
    
    if st.button('Go to Home Page'):

        st.session_state.page = 'main_page'


if __name__ == '__main__':

        if 'page' not in st.session_state:
            st.session_state.page = 'main_page'

        if st.session_state.page == 'main_page':
            main()

        elif st.session_state.page == 'questions_page':
            questions_page()

mydb.close()

#--------------------------------------------------------------------------------------------------------------------------------

# Youtube_Data_Harvesting

# Project - 1 : "YouTube Data Harvesting and Warehousing"

# Introduction

The primary objective of this system is to collect comprehensive data from YouTube, including channel information, video details, comments, and playlist information. This data is then stored within a MySQL database for further analysis and insights generation.

# Key Technologies and Skills:

1) YouTube Data API
2) Python
3) MySQL
4) Streamlit
5) Data Processing and Analysis
6) Matplotlib and Seaborn

# Installation:

## To run this project, you need to install the following packages:

### 1) pip install mysql-connector-python
### 2) pip install pandas
### 3) pip install google-api-python-client
### 4) pip install isodate
### 5) pip install streamlit
### 6) pip install matplotlib
### 7) pip install seaborn

# Features:

 YouTube Data API to fetch various data such as channel details, video information, comments, and playlist details

 It connects to a MySQL database to store the retrieved YouTube data.

 Streamlit to create an interactive web application.

 The retrieved data is processed and stored in the MySQL database

 Users can select from predefined questions or queries.

 Data retrieval, database interaction, or analysis.

 This modular design enhances code readability, maintainability, and reusability.

# YouTube Data Retrieval:

The script interacts with the YouTube Data API to fetch various data such as channel details, video information, comments, and playlist details.

# MySQL Database Integration:

It connects to a MySQL database to store the retrieved YouTube data. Tables are created as needed for storing channel information, video details, comments, and playlist details.

# Streamlit:

The script utilizes Streamlit to create an interactive web application. Users can input a YouTube channel ID to retrieve and display comprehensive details about the channel, including videos, comments, and playlists.

# Data Processing and Storage

The retrieved data is processed and stored in the MySQL database. Functions are defined to insert channel information, video details, comment data, and playlist details into their respective tables in the database.

# Data Analysis and Visualizations

The script provides functionality to perform various analyses on the YouTube data stored in the database. Users can select from predefined questions or queries, such as finding the most-viewed videos, channels with the highest number of videos, total likes and dislikes for each video, and more. The results of these analyses are displayed using Matplotlib and Seaborn for data visualization.

• Channel Analysis Channel analysis includes insights on playlists, videos, subscribers, views, likes, comments, and durations. Gain a deep understanding of the channel's performance and audience engagement through detailed summaries.

• Video Analysis Video analysis focuses on views, likes, comments, and durations, enabling both an overall channel and specific channel perspectives.

# Modular Design

The script is modularized into functions, each responsible for specific tasks such as fetching data from the YouTube API, interacting with the MySQL database, performing analyses, and generating visualizations. This modular design enhances code readability, maintainability, and reusability.

# Navigation and Interaction

The Streamlit app provides a navigation sidebar where users can select different options such as Home, Add data to MySQL,  viewing tables, Queries, and navigating back to the home page. The interface dynamically updates based on user interactions and selections.

# Contact

📧 Email: hariyogeshram882@gmail.com

# For any further questions or inquiries, feel free to reach out. We are happy to assist you with any queries.

import time
import pymongo
import base64
import requests
import pandas as pd
from selenium import webdriver
import mysql.connector as conn
from flask import Flask, render_template, request,jsonify
from flask_cors import CORS,cross_origin
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup as bs

client = pymongo.MongoClient("mongodb+srv://vidisha:nyTqJWXxtzd5rrcz@cluster0.ce2othw.mongodb.net/?retryWrites=true&w=majority")
db = client.test
print(db)

database = client['youtube']
collection1 = database['videos']
collection2 = database['comments']

mydb = conn.connect(host = "localhost", user = "root", passwd = "Algorithm@2k20")
cursor = mydb.cursor()

DRIVER_PATH = r'chromedriver.exe'

app = Flask(__name__)
@app.route('/',methods=['GET'])  # route to display the home page
@cross_origin()
def homePage():
    return render_template("index.html")


@app.route('/scrape',methods=['POST','GET'])
@cross_origin()
def youtube_latest():
    cursor.execute('truncate table youtube.video')
    mydb.commit()
    cursor.execute('truncate table youtube.all_comments')
    mydb.commit()
    if request.method == 'POST':
        try:
            url = request.form['content']
            latest_videos = 50
            driver_path = DRIVER_PATH
            driver = webdriver.Chrome(executable_path=driver_path)
            # fetch search url
            driver.get(url)
            time.sleep(2)
            contents = []
            prev_h = 0

            while True:
                height = driver.execute_script("""
                            function getActualHeight() {
                                return Math.max(
                                    Math.max(document.body.scrollHeight,document.documentElement.scrollHeight),
                                    Math.max(document.body.offsetHeight,document.documentElement.offsetHeight),
                                    Math.max(document.body.clientHeight,document.documentElement.clientHeight)
                                );
                            }
                            return getActualHeight();
                    """)
                driver.execute_script(f"window.scrollTo({prev_h},{prev_h + 200})")
                time.sleep(2)
                prev_h += 300
                videos = driver.find_elements(by=By.CLASS_NAME, value='style-scope ytd-grid-video-renderer')
                initial_count = 0
                pointer = len(videos)
                for video in videos[initial_count:pointer]:
                    title = video.find_element(by=By.XPATH, value='.//*[@id="video-title"]').get_attribute('title')
                    link = video.find_element(by=By.XPATH, value='.//*[@id="video-title"]').get_attribute('href')
                    thumbnail = video.find_element(by=By.CSS_SELECTOR,
                                                   value='#contents #items #thumbnail #img').get_attribute(
                        'src')
                    contents.append({
                        "title": title,
                        "link": link,
                        "thumbnail": thumbnail
                    })

                    #extract image from thumbnail url and inserting blob into DB
                    if thumbnail and "watch" in link:      # only adding the youtube video, and eleminating shorts
                        image_content = requests.get(thumbnail).content
                        file = base64.b64encode(image_content)
                        args = link, title, thumbnail, file
                        cursor.execute("use youtube")
                        cursor.execute("insert into video(video_url,title,thumnail_url,image) values(%s,%s,%s,%s)",
                                       args)  # inserting in mysql
                        # inserting in mongoDB
                        collection1.insert_one({
                            "title": title,
                            "link": link,
                            "thumbnail": thumbnail
                        })
                    else:
                        args = link, title, thumbnail
                        cursor.execute("use youtube")
                        cursor.execute("insert into video(video_url,title,thumnail_url) values(%s,%s,%s)",
                                       args)  # inserting in mysql without image when not found
                        # inserting in mongoDB
                        collection1.insert_one({
                            "title": title,
                            "link": link,
                            "thumbnail": thumbnail
                        })


                if prev_h >= height or pointer > latest_videos:
                    break
                mydb.commit()
            driver.close()
            return render_template('page.html')
        except Exception as e:
            print('The exception message is:', e)
            return 'Something went wrong'
    else:
        return render_template('index.html')

@app.route('/fetch',methods=['GET'])  # route to start scraping individual video and saving results
@cross_origin()
def video_scraping():
    results = []
    driver_path = DRIVER_PATH
    driver = webdriver.Chrome(executable_path=driver_path)
    cursor.execute("select video_url from youtube.video limit 5")
    for i in cursor.fetchall():
        driver.get(i[0])
        time.sleep(5)
        prev_h = 0
        while True:
            height = driver.execute_script("""
                    function getActualHeight() {
                        return Math.max(
                            Math.max(document.body.scrollHeight,document.documentElement.scrollHeight),
                            Math.max(document.body.offsetHeight,document.documentElement.offsetHeight),
                            Math.max(document.body.clientHeight,document.documentElement.clientHeight)
                        );
                    }
                    return getActualHeight();
            """)
            driver.execute_script(f"window.scrollTo({prev_h},{prev_h + 200})")
            time.sleep(2)
            prev_h += 300
            if prev_h >= height:
                break
        soup = bs(driver.page_source, 'html.parser')

        try:
            likes = soup.select_one('#top-level-buttons-computed #text').text
        except:
            likes = "0"
        try:
            comments_count = soup.select_one('#header h2#count span').text
        except:
            comments_count = "0"
        # updating likes & comments in mysql
        args = likes,comments_count,i[0]
        smt = """update youtube.video set likes = %s, comments = %s where video_url = %s"""
        cursor.execute(smt,args)
        # updating likes & comments in mongodb
        newvalue = {"$set":{"likes":likes,"comments_count":comments_count}}
        collection1.update_one({"link":i[0]},newvalue)

        comment_div = soup.select('#author-text span')
        commenters = [x.text for x in comment_div]
        comment_all = soup.select('#content #content-text')
        comment_one = [x.text for x in comment_all]

        for j in range(0,len(comment_all)):
            args2 = i[0],commenters[j],comment_one[j][0:500]
            cursor.execute("insert into youtube.all_comments(v_url,commenter_name,comment_added) values (%s,%s,%s)",args2)
            d = {
                "url":i[0],
                "commenter":commenters[j].strip(),
                "comment":comment_one[j][0:500]
            }
            collection2.insert_one(d)
        mydb.commit()
    driver.close()
    cursor.execute('select distinct all_comments.commenter_name, video.video_url,video.likes,video.comments ,video.title as Commented_Video_Title , all_comments.comment_added from all_comments inner join video on all_comments.v_url = video.video_url;')
    for i in cursor.fetchall():
        results.append({
            "Video Title":i[4],
            "Video URL":i[1],
            "Likes":i[2],
            "Comments":i[3],
            "Commenter Name":i[0].strip(),
            "Comment Added":i[5]
        })
    return render_template('results.html',results = results[0:len(results)-1])


# video_contents = youtube_latest(url=search_url, driver_path=DRIVER_PATH, latest_videos=50)
#function to scrape individual video
# video_scraping(driver_path = DRIVER_PATH)
# df = pd.DataFrame(video_contents)
# df1 = pd.DataFrame(scraped_video_results)
# print(df.head(50))
# print(df1.head())
# cursor.execute("select * from youtube.all_comments")
# for i in cursor.fetchall():
#   print(i)

if __name__ == "__main__":
    #app.run(host='127.0.0.1', port=8001, debug=True)
	app.run(debug=True)
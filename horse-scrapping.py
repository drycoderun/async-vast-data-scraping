import asyncio
from arsenic import get_session, browsers, services
import time

import logging
import structlog #pip install structlog

import csv
from urls import urls

def set_arsenic_log_level(level = logging.WARNING):
    #Create logger
    logger = logging.getLogger('arsenic')

    def logger_factory():
        return logger

    structlog.configure(logger_factory=logger_factory)
    logger.setLevel(level)

async def get_product_data(body_container):
    path_to_file = "horses.csv"
    csvFile = open(path_to_file, 'a', encoding="utf-8")
    csvWriter = csv.writer(csvFile)
    # csvWriter.writerow(['reviewer', 'rating', 'written_date', 'title', 'review_text', 'branch'])
    # print(body_container)
    # print(f'ini list_containe{list_container}')
    print("-------------------",body_container, "***************")
    for j in range(len(body_container)-1):

        # parse reviewer
        reviewerEl = await body_container[j].get_element(".DrjyGw-P._1SRa-qNz.NGv7A1lw._2yS548m8._2cnjB3re._1TAWSgm1._1Z1zA2gh._2-K8UW3T._2AAjjcx8")
        reviewer = await reviewerEl.get_text()
        if reviewer == "":
            reviewer = "NA"

        # parse rating
        ratingEl = await body_container[j].get_element(".zWXXYhVR")
        rating = await ratingEl.get_attribute("title")
        rating = str(rating).split(" ")[0]

        # parse written_date
        written_dateEl = await body_container[j].get_element(".DrjyGw-P._26S7gyB4._1z-B2F-n._1dimhEoy")
        written_date = await written_dateEl.get_text()
        written_date = written_date.replace("Written ", "")
        if written_date == "":
            written_date = "NA"

        # parse review_title
        titleEl = await body_container[j].get_element("._2tsgCuqy")
        title = await titleEl.get_text()
        if title == "":
            title = "NA"

        # parse review text
        review_El = await body_container[j].get_element(".DrjyGw-P._26S7gyB4._2nPM5Opx")
        review_textEl = await review_El.get_element("._2tsgCuqy")
        review_text = await review_textEl.get_text()
        review_text = review_text.replace("\n", " ")
        if review_text == "":
            review_text = "NA"
        
        # branch
        # branch = "Universal Studios Florida"
        branch = "Universal Studios Singapore"
        # branch = "Universal Studios Japan"
        if branch == "":
            branch = "Universal Studios Florida"

        # print(review)
        data = (reviewer,
                rating,
                written_date,
                title,
                review_text,
                branch)
        csvWriter.writerow(data)

async def scraper(url, limit):
    service = services.Chromedriver(binary='C:/Users/Umi/Documents/chromedriver.exe')
    browser = browsers.Chrome()
    browser.capabilities = {
        "goog:chromeOptions": {"args": ["--headless", "--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"]}
    }
    async with limit:
        async with get_session(service, browser) as session:
            
            await session.get(url)
            body = await session.get_element("._1c8_1ITO")
            review_div = await session.get_elements("div._1c8_1ITO > div")
            await get_product_data(review_div)
  
async def run():
    limit = asyncio.Semaphore(10)
    from_page = 1
    number_page = 10
    
    for i in range(from_page, number_page):
        # url = f'https://www.tripadvisor.com/Attraction_Review-g34515-d102432-Reviews-or{i}0-Universal_Studios_Florida-Orlando_Florida.html'
        url = urls[i]
        print(url)
        # url = f"https://www.tripadvisor.com/Attraction_Review-g298566-d320976-Reviews-or{i}0-Universal_Studios_Japan-Osaka_Osaka_Prefecture_Kinki.html"
        await scraper(url, limit)

if __name__ == "__main__":
    set_arsenic_log_level()
    start = time.time()
    asyncio.run(run())
    end = time.time() - start
    print(f'total time is: {end}')
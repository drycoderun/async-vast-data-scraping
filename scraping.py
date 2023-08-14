import asyncio
import aiohttp
from bs4 import BeautifulSoup
import logging
import structlog
import pandas as pd
import time
from urls import urls

def set_arsenic_log_level(level = logging.WARNING):
    logger = logging.getLogger('arsenic')
    def logger_factory():
        return logger
    structlog.configure(logger_factory=logger_factory)
    logger.setLevel(level)

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

async def Horse(url):
    async with aiohttp.ClientSession() as session:
        response = await fetch(session, url)
        soup = BeautifulSoup(response, 'html.parser')
    META_DATA = {}
    All = []
    horseObj = {}

    results = soup.find("div", class_="racecard__runner__column racecard__runner__column--grow racecard__runner__column--wrap")
    horseObj["horseUrl"] = url
    results = soup.find(class_="racecard__info__table")

    for elem in results:
        item = elem.find("span", class_="text-muted")
        if item.text.startswith("Owner"):
            Owner = elem.find("span", class_="")
            horseObj["Owner"] = Owner.text
        if item.text.startswith("Age"):
            Age = elem.find("span", class_="")
            horseObj["Age"] = Age.text
        if item.text.startswith("Official "):
            Official_rating = elem.find("span", class_="")
            horseObj["Official_rating"] = Official_rating.text
        if item.text.startswith("Sex"):
            Sex = elem.find("span", class_="")
            horseObj["Sex"] = Sex.text
        if item.text.startswith("Colour"):
            Colour = elem.find("span", class_="")
            horseObj["Colour"] = Colour.text
        if item.text.startswith("Pedigree"):
            pedigree = elem.find("span", class_="")
            data = pedigree.text.split(",")
            horseObj["Sire"] = data[0]
            horseObj["Dam"] = data[1]
            horseObj["Damssire"] = data[2]
    results = soup.find("table", class_="racecard__record__table")
    tfoot = soup.find("tfoot")
    td = tfoot.find("tr")
    type, starts, wins, sec, thr = td
    horseObj["Starts"] = starts.text
    horseObj["Wins"] = wins.text
    horseObj["2nds"] = sec.text
    horseObj["3rds"] = thr.text

    # horseObj["ride"] = rides

    results = soup.find("table", class_="racecard__form__table")
    tbody = results.find("tbody")
    owner = 1
    trainer = 1
    for tr in tbody:
        td = tr.find("td", class_="racecard__form__notification")
        if td is not None:
            if td.text.startswith("Change of Owner"):
                data = td.text.split("to")
                Owner = {"from": data[0].split(":")[1], "to": data[1]}
                key = "OwnerChanges" + str(owner)
                horseObj[key] = Owner
                owner = owner + 1
            if td.text.startswith("Change of Stable"):
                data = td.text.split("to")
                Owner = {"from": data[0].split(":")[1], "to": data[1]}
                key = "OwnerChanges" + str(trainer)
                horseObj[key] = Owner
                trainer = trainer + 1

    results = soup.find("table", class_="racecard__form__table")
    tbody = results.find("tbody")
    tr = tbody.find("tr")
    owner = 1
    trainer = 1
    for tr in tbody:
        temp = {}
        item = tr.find("td", class_="racecard__form__date")
        
        if item is not None:
            RaceDate = item.find("a").text
            temp["Date"] = RaceDate
    
        item = tr.find("td", class_="racecard__form__detail_text ellipsis") 
        if item is not None:
            data = item.text.split(" ")
            temp["Course"] = {}
            if len(data) == 3:
                temp["Course"]["Name"] = data[0]
                temp["Course"]["Class"] = data[1]
                temp["Course"]["Type"] = data[2]
            elif len(data) == 2:
                temp["Course"]["Name"] = data[0]
                temp["Course"]["Class"] = data[1]
    
        Dist = tr.find("td", class_="racecard__form__distance")
        if Dist is not None:
            temp["Dist"] = Dist.text

        Pos = tr.find("td", class_="racecard__form__position") 
        if Pos is not None:
            data = Pos.text.split("/")
            temp["Pos"] = {}
            temp["Pos"]["On"] = data[0]
            temp["Pos"]["Ran"] = int(data[1])

        Btn = tr.find("td", class_="racecard__form__beaten")
        if Btn is not None:
            temp["Btn"] = Btn.text
        
        Wgt = tr.find("td", class_="racecard__form__weight") 
        if Wgt is not None:
            temp["Wgt"] = Wgt.text
        
        Going = tr.find("td", class_="racecard__form__going") 
        if Going is not None:
            temp["Going"] = Going.text

        item = tr.find("td", class_="racecard__form__jockey ellipsis") 
        if item is not None:
            url = item.find("a")
            jockey = url["href"]
            if jockey is not None:
                temp["Jockey"] = jockey
        
        item = tr.find("td", class_="racecard__form__sp")
        if item is not None:
            sp = item.find("ruk-odd")
            if sp is not None: 
                temp["SP"] = sp.text

        Equip = tr.find("td", class_="racecard__form__equip") 
        if Equip is not None:
            temp["Equip"] = Equip.text

        Type = tr.find("td", class_="racecard__form__race_type") 
        if Type is not None:
            temp["Type"] = Type.text
        
        InPlay = tr.find("td", class_="racecard__form__hi_lo") 
        if InPlay is not None:
            temp["InPlay"] = InPlay.text
        
        Or = tr.find("td", class_="racecard__form__or") 
        if Or is not None:
            temp["Or"] = Or.text

        item = tr.find("td", class_="racecard__form__replay") 
        if item is not None:
            url = item.find("a")
            if url is not None:
                Replay = url["href"]
                temp["Replay"] = Replay

        horseObj["ride"] = temp
        All.append(horseObj)   
        META_DATA["Url"] = horseObj["horseUrl"]
        META_DATA["Data"] = All 
    return META_DATA

async def scrape(urls):   
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            task = asyncio.ensure_future(Horse(url))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        df = pd.DataFrame(results)
        print(df)
        df.to_csv('output.csv', index=False)

if __name__ == '__main__':
    start = time.time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(scrape(urls))
    end = time.time() - start
    print(f'total time is: {end}')
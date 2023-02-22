from threading import *
from typing import Dict
import pandas as pd
import time
import uuid

from scraper import scrape_job_positions
from redis_client import flush_redis_db

JOB_TITLES = pd.read_csv("job_titles.csv")

job_positions = []

def get_job_titles() -> Dict[str, Dict[str, str]]:
    job_titles: Dict[str, Dict[str, str]] = {}
    for _, row in JOB_TITLES.iterrows():
        title: str = row["Job Title"]
        city: str = row["City"]
        row_id = str(uuid.uuid4())
        job_titles[row_id] = {
            'title': title,
            'city': city
        }
    return job_titles

class JobPositionScraper:
    def __init__(self, job_titles: Dict[str, Dict[str, str]]) -> None:
        self.job_titles = job_titles

    def run(self) -> None:
        global job_positions
        for row_id, data in self.job_titles.items():
            print(f'Running row_id {row_id}')              
            job_title: str = data['title']
            city: str = data['city']
            redis_key = f"{row_id}-job-{job_title}-city-{city}"
            scraped_job_positions = scrape_job_positions(redis_key, job_title, city)
            job_positions += scraped_job_positions

    def write_data_to_xls(self) -> None:
        global job_positions
        df = pd.DataFrame(job_positions, columns=['Title', 'Key', 'Company', 'Location', 'Description', 'Qualifications', 'Benefits', 'Job type', 'Email', 'Phone'])
        df = df.drop_duplicates(subset=['Key'], keep='first')
        df.to_excel(f"simply_hired-{time.strftime('%m-%d-%Y')}.xlsx", index=False)
        flush_redis_db()   


if __name__ == "__main__":
    job_titles = get_job_titles()

    num_threads = 16
    threads = [0] * num_threads

    job_positions_scraper = JobPositionScraper(job_titles)

    for i in range(0, num_threads):
        threads[i] = Thread(target=job_positions_scraper.run)

    for i in range(0, num_threads):
        threads[i].start()

    for i in range(0, num_threads):
        threads[i].join() 

    job_positions_scraper.write_data_to_xls()
import re
from typing import List
import requests
from bs4 import BeautifulSoup
from redis_client import get_job_positions, get_next_page_from_redis, get_page_number_from_redis, get_thread, set_job_positions, set_next_page_in_redis

def get_jobs_from_simply_hired(job_title: str, location: str, page_number: int) -> str:
    url =  f"https://www.simplyhired.co.in/search?q={job_title}&l={location}&pn={page_number}" 
    return requests.get(url).text

def post_job_position_to_ez_jobs() -> None:
    pass

def scrape_job_positions(redis_key: str, job_title: str, location: str) -> List[List[str]]:    
    is_next_page = get_next_page_from_redis(redis_key)

    while is_next_page:
            page_number = get_page_number_from_redis(redis_key)
            response = get_jobs_from_simply_hired(job_title, location, page_number)

            if "Next page" in response:
                print(f'Found next page in {redis_key} for {get_thread()}')
                job_positions = extract_job_positions(response)
                set_job_positions(redis_key, job_positions)
            else:
                is_next_page = set_next_page_in_redis(redis_key)

    print(f'Exiting for {redis_key}, no more pages found. - {get_thread()}')
    job_positions = get_job_positions(redis_key)
    return job_positions

    

def extract_job_positions(response: str) -> List[List[str]]:
    cleaned_jobs: list[list[str]] = []
    job_ids: list[str] = []

    soup = BeautifulSoup(response, "html.parser")
    job_keys = soup.find_all(attrs={"data-jobkey": True})

    for job_key in job_keys:
        if job_key['data-jobkey'] in job_ids:
            continue
        job_ids.append(job_key['data-jobkey'])

    for job_id in job_ids:
        job_info = requests.get(f'https://www.simplyhired.co.in/api/job?key={job_id}').json()
        regex = re.compile(r"<br /?>", re.IGNORECASE)
        c_job = job_info["job"] if 'job' in job_info else None
        if not c_job:
            continue

        job_description = job_info["job"]["description"] if 'description' in job_info["job"] else None
        if not job_description:
            continue
        
        newtext = re.sub(regex, '\n', job_description)
        description = BeautifulSoup(newtext, "html.parser")
        full_description = description.get_text()

        # Check if the description contains an email
        email_match = re.search(r'[\w.-]+@[\w.-]+', full_description)
        if email_match:
            email = email_match.group()
        else:
            email = 'none'
        # Check if the description contains a phone number starting with +91
        phone_match = re.search(r'\d{10}', full_description)
        if phone_match:
            phone = phone_match.group()
        else:
            phone = 'none'

        # Check if email and phone exist
        if phone == 'none' and email == 'none':
            continue

        jobkey = job_info["jobKey"]
        job_title = job_info["job"]["title"]
        company_name = job_info["job"]["company"]
        location = job_info["job"]["location"]
        qualifications = ""
        benefits = ""
        job_type = ""

        if "educationEntities" in job_info:
            qualifications += ", ".join(job_info["educationEntities"])
        if "skillEntities" in job_info:
            qualifications += ", ".join(job_info["skillEntities"])
        if "jobType" in job_info["job"]:
            job_type = job_info["job"]["jobType"]
        if "benefitEntities" in job_info:
            benefits += ", ".join(job_info["benefitEntities"])  

        post_job_position_to_ez_jobs()

        cleaned_jobs.append([job_title, jobkey, company_name, location, full_description, qualifications, benefits, job_type, email, phone])

    return cleaned_jobs
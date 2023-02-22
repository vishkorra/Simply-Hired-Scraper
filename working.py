import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import concurrent.futures
import time
import random
import numpy as np
import datetime

PATH = '/usr/local/bin/chromedriver'
today = datetime.datetime.now().date()
job_list = pd.read_csv("job_titles.csv")
data = []
data1 = []
count = 0
start_date = datetime.datetime.now().date()


def main(JobTitle, Location):
    statsCount = 0
    num = 1
    next_page = True
    while next_page:
        jobID = []
        main_page_url = f"https://www.simplyhired.co.in/search?q={JobTitle}&l={Location}&pn={num}"
        main_response = requests.get(main_page_url).text
        if "Next page" not in main_response:
            next_page = False
        else:
            num += 1
        soup = BeautifulSoup(main_response, "html.parser")
        jobkeys = soup.find_all(attrs={"data-jobkey": True})
        for jobkey in jobkeys:
            if jobkey['data-jobkey'] in jobID:
                continue
            jobID.append(jobkey['data-jobkey'])

        for id in jobID:
            job_info = requests.get(f'https://www.simplyhired.co.in/api/job?key={id}').json()

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

            post_data = requests.post('https://testapi.ezjobs.io/admin/uploadJobData', data={
                'secretToken': 'KzjQQ5H4ZTSn7Efm4DFDcHcKUhvPNK7PRyWMyz7QPuwv3Djgvk',
                'countryCode': 91,
                'firstName': company_name,
                'phone': phone,
                'type': "M",
                'email': email,
                'address1': location,
                'city': 'none',
                'state': 'none',
                'latitude': 'none',
                'longitude': 'none',
                'zipCode': 'none',
                'country': 'IN',
                'jobTitle': job_title,
                'jobDescription': full_description,
                'categoryId': jobkey,
                'skills': qualifications,
                'jobType': job_type,
                'companyName': company_name,
                'source': 'SimplyHired',
                'classified': 'No',
                'validUpTo': 'none',
                'contactSource': 'w'
            })

            global count
            count += 1
            statsCount += 1

            data.append(
                [job_title, jobkey, company_name, location, full_description, qualifications, benefits, job_type, email,
                 phone])
    data1.append(['Simply Hired', Location.title(), JobTitle.title(), statsCount, datetime.datetime.now().date()])
    return "Success"


# Start threads
threaded_start = time.time()

current_data = []
chunk_iter = 1
chunk_size = 16


def run_the_executor(current_rows):
    global chunk_iter
    print(f"running chunk # {chunk_iter}")
    chunk_iter += 1
    with concurrent.futures.ThreadPoolExecutor(16) as executor:
        futures = []
        for title, city in current_rows:
            futures.append(executor.submit(main, JobTitle=title, Location=city))
        for future in concurrent.futures.as_completed(futures):
            try:
                print(future.result())
            except Exception as e:
                time.sleep(10)
                print(f"{e} || TRYING AGAIN")
                print(future.result())


for index, row in job_list.iterrows():
    current_data.append((row["Job Title"], row["City"]))
    if len(current_data) == chunk_size:
        run_the_executor(current_data)
        current_data.clear()

# latest chunk
if current_data:
    run_the_executor(current_data)

print("Threaded time:", time.time() - threaded_start)

# Build Pandas Dataframe
df = pd.DataFrame(data, columns=['Title', 'Key', 'Company', 'Location', 'Description', 'Qualifications', 'Benefits',
                                 'Job type', 'Email', 'Phone'])
df = df.drop_duplicates(subset=['Key'], keep='first')

# Add Count & Date
# df["Count"] = count
# df.iloc[1:, df.columns.get_loc("Count")] = np.nan
# df["Date Started"] = start_date
# df.iloc[1:, df.columns.get_loc("Date Started")] = np.nan
# df["Date Ended"] = datetime.datetime.now().date()
# df.iloc[1:, df.columns.get_loc("Date Ended")] = np.nan

# path = "/Users/vishkorra/Desktop/Simply Hired/"

df.to_excel(f"SimplyHired{time.strftime(' %m-%d-%Y')}.xlsx", index=False)

# Build Second Data

df1 = pd.DataFrame(data1, columns=['Source', 'Location', 'Job Title', 'Count', 'Date Ended'])

# df1["Count"] = count
# df1.iloc[1:, df1.columns.get_loc("Count")] = np.nan
# df1["Date Ended"] = datetime.datetime.now()
# df1.iloc[1:, df1.columns.get_loc("Date Ended")] = np.nan

# path = "/Users/vishkorra/Desktop/Simply Hired/"

df1.to_excel(f"Statistics of Simply Hired All{time.strftime(' %m-%d-%Y')}.xlsx", index=False)






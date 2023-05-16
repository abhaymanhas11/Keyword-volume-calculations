import csv
import json

# from multiprocessing import Process
import asyncio
import aiohttp
import nest_asyncio
import time

nest_asyncio.apply()
# Define the list of queries to send
queries = []

with open("input.csv", newline="", encoding="utf-8") as csvfile:
    data = csvfile.read()
    data = data.replace("\x00", "")  # remove null bytes
    reader = csv.DictReader(data.splitlines())
    for row in reader:
        queries.append(
            {
                "keyword": row["Keyword"],
                "difficulty": row["Difficulty"],
                "volume": row["Volume"],
                "link": row["Link"],
                "position": row["Position"],
            }
        )


# Filters the queries which has position greater than 5 & also that one which contain the position value non digit
filtered_queries = [
    q
    for q in queries
    if (
        q.get("position")
        and isinstance(q.get("position"), str)
        and q["position"].isdigit()
        and int(q["position"]) <= 5
    )
]


# Define the CSV output file
output_file = "output21.csv"


# Open the output file for writing
async def fetch_data(url, data, auth):
    async with aiohttp.ClientSession() as session:
        async with session.post(
            url, json=data, auth=aiohttp.BasicAuth(auth[0], auth[1])
        ) as response:
            return await response.json()


async def main():
    f = open(output_file, mode="w", newline="")
    # Create a CSV writer object
    writer = csv.writer(f)

    # Write the header row to the CSV file
    writer.writerow(
        [
            "Keyword",
            "Link",
            "Position",
            "Difficulty",
            "Volume",
            "Backlink_Count",
            "Max_Page_Authority",
            "Min_Page_Authority",
            "Avg_Page_Authority",
            "Median_Page_Authority",
            "Max_Domain_Authority",
            "Min_Domain_Authoriry",
            "Avg_Domain_Authority",
            "Median_Domain_Authority",
            "Max_Link_Propensity",
            "Min_Link_Propensity",
            "Avg_Link_Propensity",
            "Median_Link_Propensity",
            "Max_Spam_Score",
            "Min_Spam_Score",
            "Avg_Spam_Score",
            "Median_Spam_Score",
            "Unique_Domains_Count",
        ]
    )
    response_file = open("response.csv", "w+")
    fields = [
        "link",
        "source_page",
        "page_authority",
        "domain_authority",
        "link_propensity",
        "spam_score",
    ]
    response_writer = csv.DictWriter(response_file, fieldnames=fields)
    response_writer.writeheader()
    for i in range(0, 4):
        iter_start = time.time()
        link = filtered_queries[i]["link"]
        auth = ("mozscape-7929f94189", "8011e5cee4a554fd81454c7e2e1b539a")
        url = "https://lsapi.seomoz.com/v2/links"
        # data = {
        #         "target": "https://www.cryptonewsz.com/forecast/ethereum-price-prediction/",
        #         "target_scope": "page",
        #         "filter": "external+follow",
        #         "limit": 50,
        #     }
        results = []
        next_token = None
        print(f"f{i}: {link}")
        while next_token != "" and len(results) < 2000:
            # print(f"{filtered_queries[i]['link']} - {next_token} - {len(results)}")
            data = {
                "target": link,
                "target_scope": "page",
                "filter": "external+follow",
                "limit": 50,
            }
            if next_token:
                data["next_token"] = next_token
            url = url

            response = await fetch_data(url, data, auth)
            
            
            
            response_dict = list(
                map(
                    lambda x: {
                        "link": link,
                        "source_page": x["source"]["page"],
                        "page_authority": x["source"]["page_authority"],
                        "domain_authority": x["source"]["domain_authority"],
                        "link_propensity": x["source"]["link_propensity"],
                        "spam_score": x["source"]["spam_score"],
                    },
                    response["results"]
                )
            )
            response_writer.writerows(response_dict)
            source_data = []
            next_token = response.get("next_token", "")
            for result in response.get("results", []):
                source = result["source"]
                filtered_source = {
                    key: source[key]
                    for key in [
                        "domain_authority",
                        "page_authority",
                        "spam_score",
                        "link_propensity",
                        "root_domains_from_page",
                        "page",
                        "root_domain",
                    ]
                }
                source_data.append(filtered_source)

            results.extend(source_data)

        n = len(results)
        if n < 1:
            continue
        unique_domains = set()
        for item in results:
            unique_domains.add(item["root_domain"])

        unique_domains_count = len(unique_domains)

        # sorting the results of every field

        sorted_on_page_authority = sorted(results, key=lambda x: x["page_authority"])
        sorted_on_domain_authority = sorted(
            results, key=lambda x: x["domain_authority"]
        )
        # sorted_on_root_domains_from_page = sorted(results, key=lambda x: x["root_domains_from_page"])
        sorted_on_spam_score = sorted(results, key=lambda x: x["spam_score"])
        sorted_on_link_propensity = sorted(results, key=lambda x: x["link_propensity"])

        # page authority field calcuations
        min_page_authority = sorted_on_page_authority[0]["page_authority"]
        max_page_authority = sorted_on_page_authority[-1]["page_authority"]
        sum_page_authority = sum(
            item["page_authority"] for item in sorted_on_page_authority
        )
        avg_page_authority = sum_page_authority / len(sorted_on_page_authority)
        median_page_authority = 0
        if n % 2 == 0:
            median_page_authority = (
                sorted_on_page_authority[(n // 2)]["page_authority"]
                + sorted_on_page_authority[(n // 2) - 1]["page_authority"]
            ) / 2
        else:
            median_page_authority = sorted_on_page_authority[((n + 1) // 2)-1][
                "page_authority"
            ]

        # domain authority field calculatoins
        min_domain_authority = sorted_on_domain_authority[0]["domain_authority"]
        max_domain_authority = sorted_on_domain_authority[-1]["domain_authority"]
        sum_domain_authority = sum(
            item["domain_authority"] for item in sorted_on_domain_authority
        )
        avg_domain_authority = sum_domain_authority / len(sorted_on_domain_authority)
        median_domain_authority = 0
        if n % 2 == 0:
            median_domain_authority = (
                sorted_on_domain_authority[(n // 2)]["domain_authority"]
                + sorted_on_domain_authority[(n // 2)-1]["domain_authority"]
            ) / 2
        else:
            median_domain_authority = sorted_on_domain_authority[((n + 1) // 2)-1][
                "domain_authority"
            ]

        # root domain page authority field calculations
        # min_root_domains_from_page = sorted_on_root_domains_from_page[0]['root_domains_from_page']
        # max_root_domains_from_page = sorted_on_root_domains_from_page[-1]['root_domains_from_page']
        # sum_root_domains_from_page = sum(item['root_domains_from_page'] for item in sorted_on_root_domains_from_page)
        # avg_root_domains_from_page = sum_root_domains_from_page / len(sorted_on_root_domains_from_page)
        # median_root_domains_from_page = 0
        # if n % 2 == 0:
        #     median_root_domains_from_page = (sorted_on_root_domains_from_page[n//2 - 1]['root_domains_from_page'] + sorted_on_root_domains_from_page[n//2]['root_domains_from_page']) / 2
        # else:
        #     median_root_domains_from_page = sorted_on_root_domains_from_page[n//2]['root_domains_from_page']

        # spam score field calculations

        min_spam_score = sorted_on_spam_score[0]["spam_score"]
        max_spam_score = sorted_on_spam_score[-1]["spam_score"]
        sum_spam_score = sum(item["spam_score"] for item in sorted_on_spam_score)
        avg_spam_score = sum_spam_score / len(sorted_on_spam_score)
        median_spam_score = 0
        if n % 2 == 0:
            median_spam_score = (
                sorted_on_spam_score[(n // 2)]["spam_score"]
                + sorted_on_spam_score[(n // 2)-1]["spam_score"]
            ) / 2
        else:
            median_spam_score = sorted_on_spam_score[((n + 1) // 2)-1]["spam_score"]

        # link propensity field calculations
        min_link_propensity = sorted_on_link_propensity[0]["link_propensity"]
        max_link_propensity = sorted_on_link_propensity[-1]["link_propensity"]
        sum_link_propensity = sum(
            item["link_propensity"] for item in sorted_on_link_propensity
        )
        avg_link_propensity = sum_link_propensity / len(sorted_on_link_propensity)
        median_link_propensity = 0
        if n % 2 == 0:
            median_link_propensity = (
                sorted_on_link_propensity[(n // 2)]["link_propensity"]
                + sorted_on_link_propensity[(n // 2)-1]["link_propensity"]
            ) / 2
        else:
            median_link_propensity = sorted_on_link_propensity[((n + 1) // 2)-1][
                "link_propensity"
            ]

        keyword = filtered_queries[i]["keyword"]
        link = filtered_queries[i]["link"]
        position = filtered_queries[i]["position"]
        difficulty = filtered_queries[i]["difficulty"]
        volume = filtered_queries[i]["volume"]
        link = filtered_queries[i]["link"]

        print(keyword, link)
        writer.writerow(
            [
                keyword,
                link,
                position,
                difficulty,
                volume,
                n,
                max_page_authority,
                min_page_authority,
                avg_page_authority,
                median_page_authority,
                max_domain_authority,
                min_domain_authority,
                avg_domain_authority,
                median_domain_authority,
                max_link_propensity,
                min_link_propensity,
                avg_link_propensity,
                median_link_propensity,
                max_spam_score,
                min_spam_score,
                avg_spam_score,
                median_spam_score,
                unique_domains_count,
            ]
        )
        iter_end = time.time()
        print("iter time: ", iter_end - iter_start)
    f.close()
    response_file.close()

if __name__ == "__main__":
    start = time.time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    end = time.time()
    print("Process Time: ", end - start)
    # p = Process(target=main)
    # p.start()
    # p.join()

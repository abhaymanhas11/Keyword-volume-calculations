import csv
import json
import multiprocessing
import asyncio
import aiohttp
import nest_asyncio
import time
import pandas as pd
import pprint
import requests
import numpy as np

nest_asyncio.apply()

class Multi_processing:
    user ="mozscape-7929f94189"
    password ="8011e5cee4a554fd81454c7e2e1b539a"
    url= "https://lsapi.seomoz.com/v2/links"
    chunks = multiprocessing.cpu_count()-1
    queries = []
    filtered_queries = []
    data = []
    #open input file and fetch some fields
    def _open_csv(self):
        with open("input.csv", newline="", encoding="utf-8") as csvfile:
            data = csvfile.read()
            data = data.replace("\x00", "")  # remove null bytes
            reader = csv.DictReader(data.splitlines())
            for row in reader:
                self.queries.append(
                    {
                        "keyword": row["Keyword"],
                        "difficulty": row["Difficulty"],
                        "volume": row["Volume"],
                        "link": row["Link"],
                        "position": row["Position"],
                    }
              )

       #filter queries those position is less then five  and  generate csv
    def _filter_queries(self):
        self.filtered_queries = [q for q in self.queries
                                    if (
                                        q.get("position")
                                        and isinstance(q.get("position"), str)
                                        and q["position"].isdigit()
                                        and int(q["position"]) <= 5
                                    )
                                ]
    
        df = pd.DataFrame(self.filtered_queries)
        self.data = np.array_split(df, self.chunks)

        # self.filter_csv_file=self.filter_dataframe.to_csv('filter_queries.csv',index=False)


    def _fetch_data(self,data):
        auth = (self.user, self.password)
        response = requests.post(self.url, json=data, auth=auth)
        # print(response)
        return response.json()
    
    #generating resutls after hit  the api
    def _process_data(self, results, row):
        f = open("output22.csv", mode="a", newline="")
        writer = csv.writer(f)
        n = len(results)
    
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

        # keyword = self.data[i]["keyword"]
        # link = self.data[i]["link"]
        # position = self.data["position"]
        # difficulty = self.data["difficulty"]
        # volume = self.data["volume"]
        # link = self.data["link"]

        # print(keyword, link)
        writer.writerow(
            [
                row["keyword"],
                row["link"],
                row["position"],
                row["difficulty"],
                row["volume"],
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
        f.close()

    def _generating_results(self, chunk):
        results=[]
        for i, row in chunk.iterrows():
            print("Current Iteration: ",i)
            start_time = time.time()
            link = row["link"]
            # auth = ("mozscape-7929f94189", "8011e5cee4a554fd81454c7e2e1b539a")
            # url = "https://lsapi.seomoz.com/v2/links"
                # data = {
                #         "target": "https://www.cryptonewsz.com/forecast/ethereum-price-prediction/",
                #         "target_scope": "page",
                #         "filter": "external+follow",
                #         "limit": 50,
                #     }
                # results = []
            next_token = None
            while next_token != "" and len(results) < 2000:
                    data = {
                        "target": link,
                        "target_scope": "page",
                        "filter": "external+follow",
                        "limit": 50,
                    }
                    if next_token:
                        data["next_token"] = next_token

                    response = self._fetch_data(data)
                    print(response)
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
            print("urls backlink processing time : ", time.time()-start_time)
            if results:
                self._process_data(results, row)
            end_time = time.time()
            print("single url process time: ", end_time-start_time)
                
    # to generate output file
    def _generating_output(self):
        f = open("output22.csv", mode="w", newline="")
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
        f.close()
        start_time = time.time()
        with multiprocessing.Pool(processes=len(self.data)) as p:
            p.map(self._generating_results, self.data)
        end_time = time.time()
        print("Chunk Process time: ", end_time-start_time)

    def process_batches(self):
        self._open_csv()
        self._filter_queries()
        self._generating_output()
        

Multi_processing().process_batches()

    
    
   








        






# if __name__ == "__main__":
#     start = time.time()
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(main())
#     end = time.time()
#     print("Process Time: ", end - start)
#     # p = Process(target=main)
#     # p.start()
#     # p.join()

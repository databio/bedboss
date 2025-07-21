from typing import Dict, List, Tuple, Any

import numpy as np
import json
import requests
from pephubclient.helpers import RequestManager
from bbconf import bbagent
from tqdm import tqdm


# BASE_URL = "https://api.bedbase.org/v1/bed/search/text"
# BASE_URL = "http://localhost:8000/v1/bed/search/text"

bbagent = bbagent.BedBaseAgent(
    "/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml"
)

req_man = RequestManager()


def single_query_eval(
    search_results: List[Dict], relevant_results: List[str]
) -> Tuple[float, float, float]:
    """
    Evaluate a single query

    :param search_results: List of search results, ordered by similarity, must contain key "id" which is the store id
    :param relevant_results: List if store id which are relevant search results

    :return: a Tuple of (Average Precision, AUC-ROC, R-precision)
    """
    num_relevant = len(relevant_results)
    retrieved_relevant = 0
    k = len(search_results)
    sum_precision = 0
    x = [0]  # (false_positive/(false_positive + true_negative)
    y = [0]  # recall or  true_positive / (true_positive + false_negative)
    false_positive = 0
    true_negative = k - num_relevant
    true_positive = 0
    false_negative = num_relevant
    r_precision = 0
    for i in range(k):
        result = search_results[i]
        result_id = result["id"]
        if result_id in relevant_results:  # one relevant is retrieved
            true_positive += 1
            false_negative -= 1
            retrieved_relevant += 1

            sum_precision += retrieved_relevant / (i + 1)

        else:  # one irrelevant is retrieved
            false_positive += 1
            true_negative -= 1
        x.append(false_positive / (false_positive + true_negative))
        y.append(true_positive / (true_positive + false_negative))
        if i == num_relevant - 1:
            r_precision = retrieved_relevant / num_relevant
    average_precision = sum_precision / num_relevant
    # compute AUC-ROC
    auc = np.trapz(y, x)
    return average_precision, auc, r_precision


def eval(
    query_relevance: Dict[str, List[str]],
    search_results: Dict[str, List],
) -> Dict[str, float]:
    """
    With a query dictionary, return the Mean Average Precision, AUC-ROC and R-precision of query retrieval

    :param query_relevance:a dictionary that contains query and relevant results in this format:
        {
            <query string>:[
                <store id in backend>,
                ...
            ],
            ...
        }
    :param search_results: a dictionary that contains search results in this format:
        {
            <query string>:[
            # a list of search results, ordered by similarity
                {
                    "id": <store id in backend>, # a required key
                    ...
                },
                ...
            ],
            ...
        }

    :return: a Dictionary with Mean Average Precision, AUC-ROC and R-precision
    """

    sum_ap = 0  # sum of all average precision
    sum_rp = 0  # sum of all R-Precision
    sum_auc = 0  # sum of all AUC-ROC
    query_count = len(search_results)

    if query_count == 0:
        return {
            "Mean Average Precision": 0.0,
            "Mean AUC-ROC": 0.0,
            "Average R-Precision": 0.0,
        }

    for query_str, ids in query_relevance.items():
        try:
            query_results = search_results[
                query_str
            ]  # retrieve search results for the query
        except KeyError:
            continue

        ap, auc, rp = single_query_eval(query_results, ids)
        sum_ap += ap
        sum_rp += rp
        sum_auc += auc

    return {
        "Mean Average Precision": sum_ap / query_count,
        "Mean AUC-ROC": sum_auc / query_count,
        "Average R-Precision": sum_rp / query_count,
    }


def open_relevance_dict(path: str) -> Dict[str, List[str]]:
    """
    Open a relevance dictionary from a file

    :param path: path to the file containing the relevance dictionary
    :return: a dictionary with query strings as keys and lists of relevant store ids as values
    """

    with open(path, "r") as file:
        query_relevance = json.load(file)
    return query_relevance


def run_search_bedbase(query: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    ...
    """


if __name__ == "__main__":
    # Example usage
    # query_relevance_example = {
    #     "query1": ["id1", "id2"],
    #     "query2": ["id3"],
    # }
    # search_results_example = {
    #     "query1": [{"id": "id1"}, {"id": "id2"}, {"id": "id4"}],
    #     "query2": [{"id": "id3"}, {"id": "id5"}],
    # }
    #
    # results = eval(query_relevance_example, search_results_example)
    # print(results)
    p = open_relevance_dict(
        "/home/bnt4me/virginia/repos/bedboss/scripts/eval/queries_ids/biosample_synonyms_target.json"
    )
    # get first 20 elements from dict :

    p = dict(list(p.items()))
    # p = dict(list(p.items())[:500])

    found_dict = {}

    for one_query in tqdm(p.keys(), desc="Processing queries", unit="query"):

        # response = req_man.send_request(
        #     method="POST",
        #     url=BASE_URL,
        #     params={
        #         "query": one_query,
        #         "genome": "hg38",
        #         "limit": 200,
        #     },
        # )
        #
        # response_json = req_man.decode_response(response, output_json=True)
        #
        # found_dict[one_query] = response_json["results"]

        found_dict[one_query] = bbagent.bed.comp_search(
            query=one_query,
            genome_alias="hg38",
            limit=200,
        ).model_dump()["results"]

        # found_dict[one_query] = bbagent.bed.text_to_bed_search(
        #     query=one_query,
        #     # genome="hg38",
        #     limit=200,
        # ).results

    eval_dict = eval(p, found_dict)

    print(eval_dict)

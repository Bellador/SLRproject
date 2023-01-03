import time
import json
import requests
import pandas as pd
from sqlalchemy import or_
from dbclasses import ScopusEntry, connect_to_db
'''
way to get references:
https://api.elsevier.com/content/abstract/EID:[]?apiKey=[]&view=REF

'''
PATH_API_KEY = './scopus_key.txt'

# read search terms
def load_search_terms(PATH_SEARCH_TERMS = './search_terms_adapted.txt', sections=['<UGC>', '<SDG3>', '<SDG11>']):
    current_section = None
    search_terms = {}
    # assign empty lists to all sections of the file
    for section in sections:
        search_terms[section] = []
    with open(PATH_SEARCH_TERMS, 'rt') as f:
        content = f.readlines()
        for line in content:
            line = line.strip('\n')
            if line in sections:
                current_section = line
                continue
            else:
                # check if a section is assigned (only true when reading the file header)
                if current_section is not None and line is not '':
                    search_terms[current_section].append(line)
    return search_terms


# read api key
def load_api_key():
    with open(PATH_API_KEY, 'rt') as f:
        API_KEY = f.read()
    return API_KEY

def build_query_api(search_terms):
    search_queries = {
                    '<SDG3>': [],
                    '<SDG11>': [],
                    }
    # iterate over each list of SDG (3 and 11) search terms while combining them (AND) with the UGC search term list
    for sdg in search_terms:
        if sdg != '<UGC>':
            for sdg_term in search_terms[sdg]:
                query = f'({sdg_term}) AND ('
                for index, ugc_term in enumerate(search_terms['<UGC>'], 1):
                    if index < len(search_terms['<UGC>']):
                        query = query + f'({ugc_term}) OR '
                    # its the last element -> different formatting
                    else:
                        query = query + f'({ugc_term}))'
                # add query to list for that SDG
                search_queries[sdg].append(query)
    return search_queries

def build_query_treemap_plot(search_field, ugc_term, sdg_term):
    '''
    this function will return necessary queries to build the sunburst (tree)map plot for which the amount of returns
    per query are needed to get an idea of which search terms are most prominent
    :param search_terms:
    :return: search_queries
    '''
    query = f'{search_field}(({ugc_term}) AND ({sdg_term}))'
    return query

def query_core(API_KEY, request):
    resp = requests.get(request,
                 headers={'Accept': 'application/json',
                          'X-ELS-APIKey': API_KEY})
    # retrieve rate limit and remaining requests from request header
    rate_limit = int(resp.headers['x-ratelimit-limit'])
    rate_remaining = int(resp.headers['x-ratelimit-remaining'])
    if rate_remaining < 5000:
        print(f"[*] alert: only {rate_remaining} from {rate_limit} requests")
    # print(f'\r[*] {rate_remaining} of {rate_limit} requests remaining', end='')
    return resp, rate_remaining

def query_for_treemap_plot(search_terms, output_path='./treemap_data.csv', search_fields=['TITLE', 'KEY']):
    '''
    query each possible combination of UGC and SDG3, SDG11 search terms and return their total results. This dataset is
    needed to construct the sunburst treemap diagramm
    NOTE: the treemap is then generated in a jupyter notebook
    :param search_queries:
    :param session:
    :return:
    '''
    print(f'Generating treemap data with the search fields: {search_fields}')
    df = pd.DataFrame()
    # relevant parameter in json resp is: 'opensearch:totalResults'
    API_KEY = load_api_key()
    for index, ugc_term in enumerate(search_terms['<UGC>'], 1):
        for index, sdg in enumerate(search_terms, 1):
            if sdg != '<UGC>':
                for index2, sdg_term in enumerate(search_terms[sdg], 1):
                    # summerise total results over ALL search fields
                    total_results = 0
                    print('-' * 30)
                    for index3, search_field in enumerate(search_fields, 1):
                        query = build_query_treemap_plot(search_field, ugc_term, sdg_term)
                        print(f'Query {index3}: {query}')
                        request = f"http://api.elsevier.com/content/search/scopus?query={query}"
                        resp = query_core(API_KEY, request)
                        json_ = json.loads(resp[0].text)
                        total_results = total_results + int(json_['search-results']['opensearch:totalResults'])
                        print(f'Results {search_field}: {total_results}')
                    print(f'Total results: {total_results}')
                    df.loc[ugc_term, sdg_term] = int(total_results)
                    # extract DOI number as unique identifier to store in dictionary -> directly deals with duplicates
                    # 1. check for key 'search-results'
                    if 'search-results' not in json_.keys():
                        print(
                            '\n[-] key search-results not found. Going to next query (no more pages of current query considered)\n')
                        break
    print(f'exporting df to csv here: {output_path}')
    df.to_csv(output_path, sep=';')

def query_pipeline(search_queries, session, start_index=0, search_fields=['TITLE', 'KEY']):
    '''
    function to return the metadata of all research documents inside the scopus database retrieved via constructed queries relevant
    to UGC in combination to SDG3 and SDG11 and their respective targets

    :param search_queries:
    :param session:
    :param start_index:
    :return:
    '''
    # what fields should be searched? keywords e.g. KEY(oscillator) in json authkeywords; title e.g. TITLE("neuropsychological evidence"); abstract e.g. ABS(dopamine)
    # search_fields = ['TITLE', 'KEY'] #, 'ABS'
    API_KEY = load_api_key()
    results = {}
    total_records = 0
    dublicates = 0
    start = time.time()
    for search_field in search_fields:
        for sdg in search_queries:
            records = 0
            print(f'\n[*] searching literature in {search_field} for {sdg}\n')
            for query in search_queries[sdg]:
                to_paginate = False
                query = f'{search_field}(' + query + ')'
                while True:
                    # embed query in given search field
                    request = f"http://api.elsevier.com/content/search/scopus?query={query}&start={start_index}"
                    resp = query_core(API_KEY, request)

                    json = resp[0].json()
                    # extract DOI number as unique identifier to store in dictionary -> directly deals with duplicates
                    # 1. check for key 'search-results'
                    if 'search-results' not in json.keys():
                        print('\n[-] key search-results not found. Going to next query (no more pages of current query considered)\n')
                        break
                    # try:
                    #     test = json['search-results']['entry']
                    # except Exception as e:
                    #     pass
                    for result in json['search-results']['entry']:
                        # check if emtpy result
                        if 'error' in result.keys():
                            if result['error'] == 'Result set was empty':
                                break
                        # db_store()
                        eid = result['eid']
                        try:
                            doi = result['prism:doi']
                        except:
                            doi = None
                        try:
                            title = result['dc:title']
                        except:
                            title = None
                        try:
                            subtype = result['subtypeDescription']
                        except:
                            subtype = None
                        try:
                            date = result['prism:coverDate'] #format YYYY-MM-DD
                        except:
                            date = None
                        try:
                            author = result['dc:creator']
                        except:
                            author = None # or skip the entry alltogether since not in-scope without author
                        try:
                            publication_name = result['prism:publicationName']
                        except:
                            publication_name = None
                        open_access = result['openaccessFlag']

                        # retrieve relevant links
                        paper_url = None
                        abstract_url = None
                        for element in result['link']:
                            # retrieve abstract api call
                            if element['@ref'] == 'self' and element['@_fa'] == 'true':
                                abstract_url = element['@href']
                            # retrieve paper url
                            if element['@ref'] == 'scopus' and element['@_fa'] == 'true':
                                paper_url = element['@href']

                        # set status if paper was already processed by the author for the systematic literature review (is False by default, therefore obsolete here)
                        processed = False

                        results[eid] = {
                            'doi': doi,
                            'title': title,
                            'subtype': subtype,
                            'date': date,
                            'author': author,
                            'openaccess': open_access,
                            'publicationname': publication_name,
                            'paperurl': paper_url,
                            'abstracturl': abstract_url,
                            'request': request,
                            'source': 'scopus',
                            'searchfield': search_field,
                            'query': query,
                            'sdg': sdg
                        }
                        # save to database
                        entry = ScopusEntry(eid,
                                            doi,
                                            title,
                                            subtype,
                                            date,
                                            author,
                                            open_access,
                                            publication_name,
                                            paper_url,
                                            abstract_url,
                                            request,
                                            'scopus',
                                            search_field,
                                            query,
                                            sdg,
                                            processed)
                        try:
                            session.add(entry)
                            session.commit()
                            records += 1
                            total_records += 1
                        except Exception as e:
                            # print(f'\n[-] db error: {e}\n')
                            dublicates += 1
                            session.rollback()
                    # deal with pagination
                    nr_results = int(json['search-results']['opensearch:totalResults'])
                    results_per_page = int(json['search-results']['opensearch:itemsPerPage'])
                    # increase the start index for potential pagination
                    start_index = start_index + results_per_page

                    if start_index < nr_results:
                        to_paginate = True
                        continue
                    else:
                        break
                # reset start index for next query
                start_index = 0
            print(f'\n[+] {records} retrieved for {sdg} in search field {search_field}\n')
    end = time.time()
    duration = round((end - start) / 60, 2)
    print(f'\n[+] total records retrieved: {total_records}\n[+] dublicates/potential db errors: {dublicates}\n[*] scopus search completed in {duration} min')

def get_abstract_keywords(session):
    API_KEY = load_api_key()
    no_abstract = 0
    # get all rows from which not yet abstracts have been retrieved
    to_get = session.query(ScopusEntry.eid, ScopusEntry.abstracturl). \
                filter(or_(ScopusEntry.abstract == None, ScopusEntry.keywords == None)). \
                filter(ScopusEntry.subtype == 'Article')

    for index, item in enumerate(to_get, 1):
        eid = item.eid
        abstract_url = item.abstracturl
        # embed query in given search field
        # request = f"https://api.elsevier.com/content/abstract/EID:{eid}?apiKey={API_KEY}&view=REF"
        request = abstract_url
        resp = query_core(API_KEY, request)

        json = resp[0].json()
        try:
            abstract = json['abstracts-retrieval-response']['coredata']['dc:description']
        except Exception as e:
            no_abstract += 1
            abstract = None
        try:
            keywords = json['abstracts-retrieval-response']['authkeywords']['author-keyword']
            # convert keywords to string
            keyword_string = ''
            for item in keywords:
                keyword_string += f"{item['$']};"
        except Exception as e:
            keyword_string = ''

        # store abstract in db
        session.query(ScopusEntry). \
            filter(ScopusEntry.eid == eid). \
            update({'abstract': abstract})
        # store keywords in db
        session.query(ScopusEntry). \
            filter(ScopusEntry.eid == eid). \
            update({'keywords': keyword_string})
        session.commit()
        print(f"\r{index} of {to_get.count()} data downloaded", end='')

    print(f'For {no_abstract} articles no abstract was found')

if __name__ == '__main__':
    # s = connect_to_db()
    search_terms = load_search_terms(PATH_SEARCH_TERMS='./search_terms_adapted.txt')
    search_queries = build_query_api(search_terms)
    query_pipeline(search_queries, s)
    get_abstract_keywords(s)
    query_for_treemap_plot(search_terms, output_path='./20220825_treemap_adapted_searchterms_no_abs.csv', search_fields=['TITLE', 'KEY']) #, 'ABS'

    # df = pd.read_csv('./treemap_data_ex_cc_wellbeing_health.csv', sep=';')
    # pass

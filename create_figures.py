import os
import re
import csv
import numpy as np
import pandas as pd
from collections import Counter
from collections import defaultdict
# plotting
import plotly
import plotly.express as px
import plotly.graph_objs as go

from db_querier import connect_db
from db_querier import query_result_return


def load_search_terms_per_target(PATH_SEARCH_TERMS = './sdg_search_terms_extended_w_manual_terms.txt', sections=['<SDG3>', '<SDG11>']):
    '''
    load the used search terms for ugc terms and sdg target related terms. they are used to be compared with the results to
    evaluate for which targets applicable papers were found and were a possible research gab for UGC exists.

    :param PATH_SEARCH_TERMS:
    :param sections:
    :return:
    '''
    current_section = None
    active_target = None
    target_pattern = r'target'
    search_terms_per_target_dict = {}
    with open(PATH_SEARCH_TERMS, 'rt') as f:
        content = f.readlines()
        for line in content:
            line = line.strip('\n')
            if line in sections:
                current_section = line
                continue
            else:
                # check if a section is assigned (only true when reading the file header)
                if current_section is not None:
                    # check if line stands for a sdg target (e.g. target 3.2)
                    try:
                        re.search(target_pattern, line).group(0)
                        active_target = line.lstrip('target ')
                        continue
                    except AttributeError:
                        if active_target != None and line != '':
                            # relate search term to its target
                            search_terms_per_target_dict[line] = active_target
                        else:
                            continue
                else:
                    active_target = None

    return search_terms_per_target_dict

def query_result_return(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        return np.array(rows)

def ugc_sources_to_latex(ugc_source_counter):
    OUTPUT_PATH_UGC_SOURCES_TO_LATEX = './ugc_sources_to_latex.txt'

    with open(OUTPUT_PATH_UGC_SOURCES_TO_LATEX, 'wt') as f:
        f.write(r"\begin{table}[]\label{review_results_table}" + "\n")
        f.write(r"\centering" + "\n")
        f.write(r"\begin{tabular}{ |p{6cm}|p{1cm}| }" + "\n")
        f.write(r"\hline" + "\n")
        f.write(r"\multicolumn{2}{|c|}{UGC sources with at least two occurrences} \\" + "\n")
        f.write(r"\hline" + "\n")

        for item in ugc_source_counter:
            f.write(f'{item[0].title()} & {item[1]}' + r' \\' + '\n')

        f.write(r"\hline" + "\n")
        f.write(r"\end{tabular}" + "\n")
        f.write(r"\caption{}" + "\n")
        f.write(r"\label{tab:my_label}" + "\n")
        f.write(r"\end{table}" + "\n")

def data_polishing_w_year(rows):
    # first clear rows with empty ugc source
    rows = np.array([[row[0].lower().replace(' ', '').split(';'), row[1]] for row in rows if row[0] is not None])
    rows = np.array([[row[1].year, list(map(str.strip, row[0]))] for row in rows if row[1] is not None])
    # ignore empty strings
    rows = np.array([[row[0], row[1]] for row in rows if row[1] != ['']])
    return rows

def fix_different_data_source_names(rows):
    '''
    if ugc sources appear in different variations and names it will be homonised here.
    E.g. sina weibo, weibo, sina..
    also fix empty string data sources
    :return:
    '''
    weibo_pattern = r'(weibo|sina)'
    google_pattern_1 = r'(google)'
    google_pattern_2 = r'(trend|search)'
    geo_wiki_pattern_1 = r'(geo)'
    geo_wiki_pattern_2 = r'(wiki)'
    osm_pattern = r'(openstreet)'
    strava_pattern = r'(strava)'
    facebook_pattern = r'(facebook)'
    tecent_pattern = r'(tencent)'
    twitter_pattern = r'(twitter)'
    healthmap_pattern = r'(healthmap)'
    flickr_pattern = r'(flickr)'
    wikidata_pattern = r'(wikidata)'
    wikiloc_pattern = r'(wikiloc)'

    fixed_rows = []
    for row in rows:
        ugc_sources = row[0]
        sdg_term = row[1]
        year = row[2]
        fixed_ugc_sources = []
        for source in ugc_sources:
            if source != '':
                if bool(re.search(weibo_pattern, source)):
                    # homogeniase to sina weibo
                    source = 'sina weibo'
                elif bool(re.search(google_pattern_1, source)) or bool(re.search(google_pattern_2, source)):
                    # homogeniase to google
                    source = 'google trends'
                elif bool(re.search(geo_wiki_pattern_1, source)) or bool(re.search(geo_wiki_pattern_2, source)):
                    # homogeniase to geo-wiki.org
                    source = 'geo wiki'
                elif bool(re.search(osm_pattern, source)):
                    # homogeniase to osm
                    source = 'openstreetmap (osm)'
                elif bool(re.search(strava_pattern, source)):
                    # homogeniase to strava
                    source = 'strava'
                elif bool(re.search(facebook_pattern, source)):
                    # homogeniase to facebook
                    source = 'facebook'
                elif bool(re.search(tecent_pattern, source)):
                    # homogeniase to tecent
                    source = 'tencent'
                elif bool(re.search(twitter_pattern, source)):
                    # homogeniase to twitter
                    source = 'twitter'
                elif bool(re.search(healthmap_pattern, source)):
                    # homogeniase to healthmap
                    source = 'healthmap'
                elif bool(re.search(flickr_pattern, source)):
                    # homogeniase to flickr
                    source = 'flickr'
                elif bool(re.search(wikiloc_pattern, source)):
                    # homogeniase to wikiloc
                    source = 'wikiloc'
                elif bool(re.search(wikidata_pattern, source)):
                    # homogeniase to wikidata
                    source = 'wikidata'

                fixed_ugc_sources.append(source)
        fixed_row = [fixed_ugc_sources, sdg_term, year] # year
        fixed_rows.append(fixed_row)
    fixed_rows = np.array(fixed_rows)
    return fixed_rows

def plot_ugc_sources_overview():
    '''
    simple figure that plots the occurrences of the found ugc sources

    :return:
    '''
    rows = query_result_return(conn, "select ugc_source, date from literature where decision_r_1 = '1' or decision_r_2 = '1' and subtype != 'Review';")
    rows = np.array([[row[0].lower().split(';'), row[1]] for row in rows])
    rows = np.array([[list(map(str.strip, row[0])), row[1]] for row in rows])
    ugc_source_list = np.concatenate(np.array([row[0] for row in rows]))
    ugc_source_counter = Counter(ugc_source_list)
    # transform Counter object to dataframe for visualisation
    # most_common = ugc_source_counter.most_common()
    df = pd.DataFrame.from_dict(ugc_source_counter, orient='index').reset_index()
    # rename columns
    df = df.rename(columns={'index': 'ugc', 0: 'count'})
    # sort by count
    df.sort_values(by='count', inplace=True, ascending=False)
    fig = px.bar(df, x='ugc', y='count')
    fig.show()

def plot_included_papers_by_year():
    rows = query_result_return(conn, "select ugc_source, date from literature where decision_r_1 = '1' or decision_r_2 = '1' and subtype != 'Review';")
    years = [row[1].year for row in rows if row[1] is not None]
    year_counter = Counter(years)
    # transform Counter object to dataframe for visualisation
    # most_common = ugc_source_counter.most_common()
    df = pd.DataFrame.from_dict(year_counter, orient='index').reset_index()
    # rename columns
    df = df.rename(columns={'index': 'year', 0: 'count'})
    # sort by count
    df.sort_values(by='count', inplace=True, ascending=False)
    fig = px.bar(df, x='year', y='count')
    fig.show()

def plot_top_ugc_sources_per_year(most_common_terms = 5):
    rows = query_result_return(conn, "select ugc_source, date from literature where decision_r_1 = '1' or decision_r_2 = '1' and subtype != 'Review';")
    rows = data_polishing_w_year(rows)
    # merge ugc_source lists by year
    year_dict = {}
    for row in rows:
        year = row[0]
        ugc_sources = row[1]
        if year not in year_dict.keys() and ugc_sources != '':
            year_dict[year] = ugc_sources
        else:
            # merge on existing
            existing_ugc_sources = year_dict[year]
            year_dict[year] = existing_ugc_sources + ugc_sources

    for key in year_dict.keys():
        year_dict[key] = Counter(year_dict[key]).most_common(most_common_terms)

    df = pd.DataFrame(columns=['year', 'term', 'count'])
    # append years and their top ugc sources as rows to df
    index = 0
    for year, top_items in year_dict.items():
        for item in top_items:
            term = item[0]
            count = item[1]
            row = [year, term, count]
            df.loc[index] = row
            index += 1
    fig = px.bar(df, x='year', y='term', title="Top ugc sources by year")
    fig.update_layout(
        font_size=17
    )
    fig.show()

def plot_ugc_sources_by_topic():
    '''
    load the used search terms for ugc terms and sdg target related terms. they are used to be compared with the results to
    evaluate for which targets applicable papers were found and were a possible research gab for UGC exists.
    Additionally, load the ugc sources and plot them with the targets they were used for
    Steps:
    1.
    2.
    3.
    :return:
    '''
    def query_extract(query, source):
        # check if source is from the initial data pool (which was automated) or from the later iterations (webofknowledge) which was manually
        if source == 'webofknowledge':
            return query
        else:
            # used in map function on np array to extract search query of the paper
            patter = r'[A-Z]+\({2}(\({0,1}.+)(?=\) AND \(\(citizen science collective sens\*\))'
            try:
                match = re.match(patter, query).group(1)
            except:
                pass
            return match

    rows = query_result_return(conn, "select ugc_source, query, source from literature where (decision_r_1 = '1' or decision_r_2 = '1') and subtype != 'Review';")
    rows = np.array([[row[0].lower().replace(' ', '').split(';'), row[1], row[2]] for row in rows])
    rows = np.array([[list(map(str.strip, row[0])), row[1], row[2]] for row in rows if row[1] is not None])
    # extract query term which resembles the sdg relevant keyword for which the paper was extracted
    sdg_terms_of_accepted_papers = np.array([[row[0], query_extract(row[1], row[2])] for row in rows])
    sdg_terms_of_accepted_papers = fix_different_data_source_names(sdg_terms_of_accepted_papers)
    '''
    1. match sdg search terms of accepted papers with the complete list used for querying Scopus and see where gaps exist
    '''
    search_terms_per_target_dict = load_search_terms_per_target()
    # creates a nested default dictionary
    papers_per_target = defaultdict(lambda: {'count': 0, 'ugc_sources': []})
    #papers_per_target = dict.fromkeys(search_terms_per_target_dict.values())
    for (ugc_sources, term) in sdg_terms_of_accepted_papers:
        try:
            target_str = search_terms_per_target_dict[term]
            #current_count = papers_per_target[target_str]['count']
            papers_per_target[target_str]['count'] += 1
            current_ugc_sources = papers_per_target[target_str]['ugc_sources']
            papers_per_target[target_str]['ugc_sources'] = current_ugc_sources + ugc_sources
        except Exception as e:
            # excepted will be terms that were manually added during the iteration of found LR
            # the terms therefore do not originate from the initial scopus api search
            print(f'here {term}')

    df = pd.DataFrame(columns=['target', 'ugc_sources', 'count'])
    top_ugc_sources = 3
    # append years and their top ugc sources as rows to df
    index = 0
    for target, target_dict in papers_per_target.items():
        top_ugc_sources_counter = Counter(target_dict['ugc_sources']).most_common(top_ugc_sources)
        total_ugc_sources_count = len(target_dict['ugc_sources'])
        total_top_ugc_sources_count = 0
        for nr_top, top_source_tuple in enumerate(top_ugc_sources_counter, 1):
            top_source = top_source_tuple[0]
            top_source_count = top_source_tuple[1]
            # add it to the total_top_ugc_sources_count to substract it later from the overall total count of ugc sources for the given target
            total_top_ugc_sources_count += top_source_count
            row = [target, top_source, top_source_count]  # IMPORTANT: ugc_source is a list, takeing only the first element is a approximation
            df.loc[index] = row
            index += 1
        # add the 'other' class to the bar graph - data sources that are not in the top X
        row = [target, 'other', (total_ugc_sources_count - total_top_ugc_sources_count)]
        df.loc[index] = row
        index += 1
    # excepted will be terms that were manually added during the iteration of found LR
    # the terms therefore do not originate from the initial scopus api search
    # sort df
    df.sort_values(by=['target', 'count'], inplace=True)
    # draw figure
    fig = px.bar(df, x='target', y='count', color='ugc_sources', title=f"top {top_ugc_sources} UGC sources per SDG target")
    # change layout to descending order
    fig.update_layout(barmode='stack', xaxis={'categoryorder': 'total descending'})
    fig.show()


def load_search_terms(PATH_SEARCH_TERMS = './sdg_search_terms_extended_w_manual_terms.txt', sections=['<SDG3>', '<SDG11>']):
    '''
    load the used search terms for ugc terms and sdg target related terms. they are used to be compared with the results to
    evaluate for which targets applicable papers were found and were a possible research gab for UGC exists.

    :param PATH_SEARCH_TERMS:
    :param sections:
    :return:
    '''
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
                if current_section is not None and line != '':
                    search_terms[current_section].append(line)
    return search_terms

def plot_ugc_sources_by_topic_revamped_sankeyplot():
    '''
    REVAMPTED:
    - define the top e.g. 5 data sources over all which shall always be displayed in the figure for each target (for legibility)
    - include the class Citizen Science projects

    load the used search terms for ugc terms and sdg target related terms. they are used to be compared with the results to
    evaluate for which targets applicable papers were found and were a possible research gab for UGC exists.
    Additionally, load the ugc sources and plot them with the targets they were used for
    Steps:
    1.
    2.
    3.
    :return:
    '''
    OUTPUT_HTML = './plots/ugc_sources_by_topic_sankeyplot.html'
    OUTPUT_PNG = './plots/ugc_sources_by_topic_sankeyplot.jpg'

    term_citizen_science = 'citizen science'
    top_sources_in_figure = 13 # + citizen science added separatly below
    def query_extract(query, source):
        # check if source is from the initial data pool (which was automated) or from the later iterations (webofknowledge) which was manually
        if source == 'webofknowledge':
            return query
        else:
            # used in map function on np array to extract search query of the paper
            patter = r'[A-Z]+\({2}(\({0,1}.+)(?=\) AND \(\(citizen science collective sens\*\))'
            try:
                match = re.match(patter, query).group(1)
            except:
                pass
            return match

    def check_cs(ugc_form):
        pattern = r'(citizen|crowed|crowd)'
        new_source = []
        for ugc in ugc_form:
            try:
                re.search(pattern, ugc, re.IGNORECASE).group(0)
                new_source.append(term_citizen_science)
            except Exception as e:
                new_source.append(ugc)
        return new_source

    rows = query_result_return(conn, "select ugc_source, sdg from literature where (decision_r_1 = '1' or decision_r_2 = '1') and subtype != 'Review';")

    rows = np.array([[row[0].lower().replace(' ', '').split(';'), row[1]] for row in rows])
    rows = np.array([[list(map(str.strip, row[0])), row[1]] for row in rows if row[1] is not None])
    # if citizen science is mentioned in remark or ugc_source it should be homogenised as 'CS' (citizen science)
    rows = np.array([[check_cs(row[0]), row[1]] for row in rows])
    rows = fix_different_data_source_names(rows)
    overall_most_common_ugc_list = [source[0] for source in Counter([item for list_ in rows for item in list_[0]]).most_common(top_sources_in_figure)]

    # add citizen science also to the categories that should appear in the figure if not already present
    if term_citizen_science not in overall_most_common_ugc_list:
        overall_most_common_ugc_list += [term_citizen_science]

    df = pd.DataFrame(rows, columns=['ugc_sources', 'target'])
    '''
    some targets will be excluded because they were originally not included in the SLR
    some papers from these targets were only included during the following iterations
    when other relevant reviews were screened
    '''
    targets_to_exclude = ['3.1', '3.2', '3.b', '3.5', '3.a', '3.7', '3.8', '3.c', '3.d']
    # tuple with ugc source and target as key, value is the recorded count
    ugc_per_target_dict = defaultdict(lambda: 0)
    for row_index, row in df.iterrows():
        # check if ugc source list not empty
        if row[0]:
            ugc_sources = row[0]
            target = row[1]
            if target not in targets_to_exclude:
                for ugc_source in ugc_sources:
                    if ugc_source == 'hardwarezone(forumhosting)':
                        print('here')
                        pass
                    if ugc_source in overall_most_common_ugc_list:
                        ugc_per_target_dict[(ugc_source, target)] += 1
                    else:
                        ugc_per_target_dict[('other', target)] += 1

    # convert final dict to df for further manipulation
    rows_to_insert_into_df = []
    for k, v in ugc_per_target_dict.items():
        row = [k[0], k[1], v]
        rows_to_insert_into_df.append(row)

    df_final = pd.DataFrame(rows_to_insert_into_df, columns=['ugc_sources', 'target', 'count'])
    # excepted will be terms that were manually added during the iteration of found LR
    # the terms therefore do not originate from the initial scopus api search
    # sort df
    df_final.sort_values(by=['target', 'count'], inplace=True, ascending=False)
    # dictionary that assigns legible names to specific targets for figure axis labeling

    target_name_dict = {
        '11.1': 'urban inequality',
        '11.2': 'mobility',
        '11.3': 'urban planning',
        '11.4': 'heritage protection',
        '11.5': 'disaster impact',
        '11.6': 'urban environmental impact',
        '11.7': 'green and public space',
        '11.a': 'development planning',
        '11.b': 'disaster risk reduction',
        '11.c': 'developing country support',
        '3.3': 'diseases',
        '3.4': 'mental health',
        '3.6': 'traffic accidents',
        '3.9': 'pollution'
    }
    def get_target_color_dict(opacity):
        target_color_dict = {
            '11.1': 'urban inequality',
            '11.2': 'mobility',
            '11.3': f'rgba(138, 146, 251, {opacitiy})',
            '11.4': 'heritage protection',
            '11.5': f'rgba(192, 138, 251, {opacitiy})',
            '11.6': 'urban environmental impact',
            '11.7': f'rgba(64, 217, 176, {opacitiy})',
            '11.a': 'development planning',
            '11.b': 'disaster risk reduction',
            '11.c': 'developing country support',
            '3.3': f'rgba(255, 184, 131, {opacitiy})',
            '3.4': 'mental health',
            '3.6': 'traffic accidents',
            '3.9': f'rgba(243, 127, 108, {opacitiy})'
        }
        return target_color_dict

    def get_ugc_color(opacity):
        ugc_color_dict = {
            'twitter': f'rgba(28, 147, 228, {opacitiy})',
            'facebook': f'rgba(73, 90, 148, {opacitiy})',
            'instagram': f'rgba(252, 1, 216, {opacitiy})',
            'youtube': f'rgba(254, 0, 0, {opacitiy})',
            'sina weibo': f'rgba(242, 145, 48, {opacitiy})',
            'foursquare': f'rgba(231, 62, 107, {opacitiy})',
            'citizen science': f'rgba(19, 138, 7, {opacitiy})',
            'other': f'rgba(173, 173, 173, {opacitiy})'
        }
        return ugc_color_dict
    # switch out numerical target names with legible terms
    df_final['target'] = df_final['target'].apply(lambda x: target_name_dict[x] + f' ({x})')
    # category sequence
    target_sequence = ['3.3', '3.4', '3.6', '3.9', '11.1', '11.2', '11.3', '11.4', '11.5', '11.6', '11.7', '11.a', '11.b']
    category_sequence = []
    for k in target_sequence:
        category_sequence.append(target_name_dict[k] + f' ({k})')
    # replace some labels in df
    df_final.loc[df_final["ugc_sources"] == "socialmedia-notfurtherspecified", "ugc_sources"] = 'social media (no details)'
    colors = [
        # "rgba(31, 119, 180, 0.8)",
        # "rgba(255, 127, 14, 0.8)",
        # "rgba(44, 160, 44, 0.8)",
        # "rgba(214, 39, 40, 0.8)",
        # "rgba(148, 103, 189, 0.8)",
        # "rgba(140, 86, 75, 0.8)",
        # "rgba(227, 119, 194, 0.8)",
        # "rgba(127, 127, 127, 0.8)",
        # "rgba(188, 189, 34, 0.8)",
        # "rgba(23, 190, 207, 0.8)",
        # "rgba(31, 119, 180, 0.8)",
        # "rgba(255, 127, 14, 0.8)",
        # "rgba(44, 160, 44, 0.8)",
        "rgba(214, 39, 40, 0.8)",
        "rgba(148, 103, 189, 0.8)",
        "rgba(140, 86, 75, 0.8)",
        "rgba(227, 119, 194, 0.8)",
        "rgba(127, 127, 127, 0.8)",
        "rgba(188, 189, 34, 0.8)",
        "rgba(23, 190, 207, 0.8)",
        "rgba(31, 119, 180, 0.8)",
        "rgba(255, 127, 14, 0.8)",
        "rgba(44, 160, 44, 0.8)",
        "rgba(214, 39, 40, 0.8)",
        "rgba(148, 103, 189, 0.8)",
        "rgba(140, 86, 75, 0.8)",
        "rgba(227, 119, 194, 0.8)",
        "rgba(127, 127, 127, 0.8)",
        "rgba(188, 189, 34, 0.8)",
        "rgba(23, 190, 207, 0.8)",
        "rgba(31, 119, 180, 0.8)",
        "rgba(255, 127, 14, 0.8)",
        "rgba(44, 160, 44, 0.8)",
        "rgba(214, 39, 40, 0.8)",
        "rgba(148, 103, 189, 0.8)",
        "rgba(255, 0, 255, 0.8)",
        "rgba(227, 119, 194, 0.8)",
        "rgba(127, 127, 127, 0.8)",
        "rgba(188, 189, 34, 0.8)",
        "rgba(23, 190, 207, 0.8)",
        "rgba(31, 119, 180, 0.8)",
        "rgba(255, 127, 14, 0.8)",
        "rgba(44, 160, 44, 0.8)",
        "rgba(214, 39, 40, 0.8)",
        "rgba(148, 103, 189, 0.8)",
        "rgba(140, 86, 75, 0.8)",
        "rgba(227, 119, 194, 0.8)",
        "rgba(127, 127, 127, 0.8)"
    ]
    # node labels are used by their element's respective index
    node_labels = list(df_final['target'].unique()) + list(df_final['ugc_sources'].unique())
    opacitiy = 0.75
    # get colors for targets
    target_color_dict = get_target_color_dict(opacitiy)
    # convert to rgb tuple
    standartised_target_colors = [tuple(int(hex_.lstrip('#')[i:i + 2], 16) for i in (0, 2, 4)) for hex_ in
                                  px.colors.qualitative.Plotly]  # holds exactly 10 elements
    # convert to rgba string
    standartised_target_colors = [f'rgba({t[0]}, {t[1]}, {t[2]}, {opacitiy})' for t in standartised_target_colors]
    node_colors = []
    for i, label in enumerate(node_labels):
        if label == 'twitter':
            node_colors.append(f'rgba(28, 147, 228, {opacitiy})')
        elif label == 'facebook':
            node_colors.append(f'rgba(73, 90, 148, {opacitiy})')
        elif label == 'flickr':
            node_colors.append(f'rgba(251, 19, 133, {opacitiy})')
        elif label == 'sina weibo':
            node_colors.append(f'rgba(242, 145, 48, {opacitiy})')
        elif label == 'instagram':
            node_colors.append(f'rgba(252, 1, 216, {opacitiy})')
        elif label == 'youtube':
            node_colors.append(f'rgba(254, 0, 0, {opacitiy})')
        elif label == 'foursquare':
            node_colors.append(f'rgba(231, 62, 107, {opacitiy})')
        elif label == 'citizen science':
            node_colors.append(f'rgba(19, 138, 7, {opacitiy})')
        elif label == 'other':
            node_colors.append(f'rgba(173, 173, 173, {opacitiy})')
        elif label == 'pollution (3.9)':
            node_colors.append(target_color_dict['3.9'])
        elif label == 'disaster impact (11.5)':
            node_colors.append(target_color_dict['11.5'])
        elif label == 'diseases (3.3)':
            node_colors.append(target_color_dict['3.3'])
        elif label == 'green and public space (11.7)':
            node_colors.append(target_color_dict['11.7'])
        elif label == 'sustainable urban planning (11.3)':
            node_colors.append(target_color_dict['11.3'])
        else:
            node_colors.append(colors[i])

    link_sources = []
    link_targets = []
    link_values = []
    link_colors = []
    # compile data
    for index, (label, label_df) in enumerate(df_final.groupby('ugc_sources')):
        for i, row in label_df.iterrows():
            link_sources.append(node_labels.index(row['ugc_sources']))
            link_targets.append(node_labels.index(row['target']))
            link_values.append(row['count'])
            # color based on link source
            link_colors.append(node_colors[node_labels.index(row['ugc_sources'])])

    fig = go.Figure(data=[go.Sankey(
        # textfont=dict(color="rgba(0,0,0,0)", size=1), ### EFFECTS VISIBLITY OF LABELS, but are cluttering sankey plot
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=node_labels,
            color=node_colors
        ),
        link=dict(
            source=link_sources,
            target=link_targets,
            value=link_values,
            color=link_colors
        ))])

    fig.update_layout( # title_text="Break down of data sources used to measure SDG 3 and 11",
        font_size=17
    )
    fig.update_yaxes(
        tickangle=90,
        ticklabelposition='outside right'
    )
    fig.update_xaxes(
        tickangle=90,
        ticklabelposition='outside right'
    )
    fig.show()

    print('saving figures in different formats..')
    fig.write_html(OUTPUT_HTML)
    plotly.io.write_image(fig, format='png', file=OUTPUT_PNG, width=1900, height=800, engine='auto')
    # fig.write_image(OUTPUT_PNG, )
    # fig.write_image(OUTPUT_SVG )

def plot_ugc_sources_by_topic_revamped_stackedbar():
    '''
    REVAMPTED:
    - define the top e.g. 5 data sources over all which shall always be displayed in the figure for each target (for legibility)
    - include the class Citizen Science projects

    load the used search terms for ugc terms and sdg target related terms. they are used to be compared with the results to
    evaluate for which targets applicable papers were found and were a possible research gab for UGC exists.
    Additionally, load the ugc sources and plot them with the targets they were used for
    Steps:
    1.
    2.
    3.
    :return:
    '''
    OUTPUT_HTML = './plots/ugc_sources_by_topic_stackedbar.html'
    OUTPUT_PNG = './plots/ugc_sources_by_topic_stackedbar.jpg'

    term_citizen_science = 'citizen science'
    top_sources_in_figure = 10 # + citizen science added seperatly below
    def query_extract(query, source):
        # check if source is from the initial data pool (which was automated) or from the later iterations (webofknowledge) which was manually
        if source == 'webofknowledge':
            return query
        else:
            # used in map function on np array to extract search query of the paper
            patter = r'[A-Z]+\({2}(\({0,1}.+)(?=\) AND \(\(citizen science collective sens\*\))'
            try:
                match = re.match(patter, query).group(1)
            except:
                pass
            return match

    def check_cs(source):
        pattern = r'(citizen|crowed)'
        new_source = []
        for element in source:
            try:
                re.search(pattern, element, re.IGNORECASE).group(0)
                new_source.append(term_citizen_science)
            except Exception as e:
                new_source.append(element)
        return new_source

    rows = query_result_return(conn, "select ugc_source, query, source from literature where (decision_r_1 = '1' or decision_r_2 = '1') and subtype != 'Review';")

    rows = np.array([[row[0].lower().replace(' ', '').split(';'), row[1], row[2]] for row in rows])
    rows = np.array([[list(map(str.strip, row[0])), row[1], row[2]] for row in rows if row[1] is not None])
    # extract query term which resembles the sdg relevant keyword for which the paper was extracted
    sdg_terms_of_accepted_papers = np.array([[row[0], query_extract(row[1], row[2])] for row in rows])
    # if citizen science is mentioned in remark or ugc_source it should be homogeniased as 'CS' (citizen science)
    sdg_terms_of_accepted_papers = np.array([[check_cs(row[0]), row[1]] for row in sdg_terms_of_accepted_papers])
    sdg_terms_of_accepted_papers = fix_different_data_source_names(sdg_terms_of_accepted_papers)
    # just for fetching the overall most common data sources
    overall_most_common_ugc_list = [source[0] for source in Counter([item for list_ in sdg_terms_of_accepted_papers for item in list_[0]]).most_common(top_sources_in_figure)]
    # ugc_sources_to_latex(overall_most_common_ugc_list_counter)
    # add citizen science also to the categories that should appear in the figure if not already present
    if term_citizen_science not in overall_most_common_ugc_list:
        overall_most_common_ugc_list += [term_citizen_science]
    '''
    1. match sdg search terms of accepted papers with the complete list used for querying Scopus and see where gaps exist
    '''
    search_terms_per_target_dict = load_search_terms_per_target()
    # creates a nested default dictionary
    papers_per_target = defaultdict(lambda: {'count': 0, 'ugc_sources': []})
    papers_with_multiple_data_source = 0
    papers_with_multiple_data_source_list = []

    #papers_per_target = dict.fromkeys(search_terms_per_target_dict.values())
    for (ugc_sources, term) in sdg_terms_of_accepted_papers:
        try:
            target_str = search_terms_per_target_dict[term]
            #current_count = papers_per_target[target_str]['count']
            papers_per_target[target_str]['count'] += 1 #
            current_ugc_sources = papers_per_target[target_str]['ugc_sources']
            if len(ugc_sources) > 1:
                papers_with_multiple_data_source += 1
                papers_with_multiple_data_source_list.append(ugc_sources)
            papers_per_target[target_str]['ugc_sources'] = current_ugc_sources + ugc_sources
        except Exception as e:
            # excepted will be terms that were manually added during the iteration of found LR
            # the terms therefore do not originate from the initial scopus api search
            print(f'here {term}')
    print(f'papers with multiple data sources: {papers_with_multiple_data_source}')

    color_pallet = px.colors.qualitative.Pastel
    step = int(len(color_pallet) / (top_sources_in_figure + 1))
    ugc_source_figure_color_dict = {
        'flickr': color_pallet[0],
        'citizen science': color_pallet[step],
        'sina weibo': color_pallet[2 * step],
        'twitter': color_pallet[3 * step],
        'facebook': color_pallet[4 * step],
        'socialmedia-notfurtherspecified': color_pallet[5 * step],
        'openstreetmap (osm)': color_pallet[6 * step],
        'instagram': color_pallet[7 * step],
        'google trends': color_pallet[8 * step],
        'foursquare': color_pallet[9 * step],
        'other': color_pallet[10 * step]
    }

    df = pd.DataFrame(columns=['target', 'ugc_sources', 'count'])
    # append years and their top ugc sources as rows to df
    index = 0
    '''
    some targets will be excluded because they were originally not included in the SLR
    some papers from these targets were only included during the following iterations
    when other relevant reviews were screened
    '''
    targets_to_exclude = ['3.1', '3.2', '3.3', '3.b', '3.5', '3.a', '3.7', '3.8', '3.c', '3.d']
    for target, target_dict in papers_per_target.items():
        if target not in targets_to_exclude:
            top_ugc_sources_counter = Counter(target_dict['ugc_sources'])
            total_ugc_sources_count = len(target_dict['ugc_sources'])
            total_top_ugc_sources_count = 0
            for top_source in overall_most_common_ugc_list:
                top_source_count_in_this_target = top_ugc_sources_counter[top_source]
                # add it to the total_top_ugc_sources_count to substract it later from the overall total count of ugc sources for the given target
                total_top_ugc_sources_count += top_source_count_in_this_target
                row = [target, top_source, top_source_count_in_this_target]  # IMPORTANT: ugc_source is a list, taking only the first element is an approximation
                df.loc[index] = row
                index += 1
            # add the 'other' class to the bar graph - data sources that are not in the top X
            row = [target, 'other', (total_ugc_sources_count - total_top_ugc_sources_count)]
            df.loc[index] = row
            index += 1
    # excepted will be terms that were manually added during the iteration of found LR
    # the terms therefore do not originate from the initial scopus api search
    # sort df
    df.sort_values(by=['target', 'count'], inplace=True, ascending=False)
    # dictionary that assigns legible names to specific targets for figure axis labeling
    target_name_dict = {
        '11.1': 'urban inequality',
        '11.2': 'mobility',
        '11.3': 'sustainable urban planning',
        '11.4': 'cultural and natural heritage protection',
        '11.5': 'disaster impact',
        '11.6': "citie's environmental impact",
        '11.7': 'green and public space',
        '11.a': 'development planning',
        '11.b': 'disaster risk reduction',
        '11.c': 'developing country support',
        '3.4': 'mental health and well-being',
        '3.6': 'traffic accidents',
        '3.9': 'pollution'
    }
    # switch out numerical target names with legible terms
    df['target'] = df['target'].apply(lambda x: target_name_dict[x] + f' ({x})')
    # category sequence
    target_sequence_OLD = ['3.3', '3.4', '3.5', '3.6', '3.8', '3.9', '11.1', '11.2', '11.3', '11.4', '11.5', '11.6', '11.7', '11.a', '11.b']
    target_sequence = ['3.4', '3.6', '3.9', '11.1', '11.2', '11.3', '11.4', '11.5', '11.6', '11.7', '11.a', '11.b']
    category_sequence = []
    for k in target_sequence:
        category_sequence.append(target_name_dict[k] + f' ({k})')
    # changes that 'other' cathegory is at the bottom
    fig = go.Figure()
    other_class_set = False
    ugc_classes_set = False

    # draw figure
    while True:
        for index, (label, label_df) in enumerate(df.groupby('ugc_sources')):
            # first element create figure, then append
            display_label = label
            if label == 'socialmedia-notfurtherspecified':
                display_label = 'social media (no details)'
            # check if 'other' class was set
            if not other_class_set:
                if label == 'other':
                    fig = go.Figure(go.Bar(x=label_df['target'], y=label_df['count'], name=display_label,
                                           marker={'color': ugc_source_figure_color_dict[label]}))
                    other_class_set = True
                    break
                else:
                    continue
            else:
                if label == 'other':
                    continue
                else:
                    fig.add_trace(go.Bar(x=label_df['target'], y=label_df['count'], name=display_label,
                                         marker={'color': ugc_source_figure_color_dict[label]}))
                    ugc_classes_set = True
        if ugc_classes_set:
            break

    # change layout to descending order
    fig.update_layout(barmode='stack',
                    xaxis={'categoryorder': 'array', 'categoryarray': category_sequence, 'title': None},
                    legend=dict(
                        yanchor="top",
                        y=0.99,
                        xanchor="left",
                        x=1 #0.99
                        ),
                    yaxis_title='count'
                    )
    # fig.update_xaxes(categoryorder='category descending')
    fig.show()

    print('saving figures in different formats..')
    fig.write_html(OUTPUT_HTML)
    plotly.io.write_image(fig, format='png', file=OUTPUT_PNG, width=1900, height=800, engine='auto')
    # fig.write_image(OUTPUT_PNG, )
    # fig.write_image(OUTPUT_SVG )

def plot_ugc_sources_by_year():
    '''
    Analyse the temporal change in data sources used, when did new ones emerge, when did they disappear?
    :return:
    '''
    OUTPUT_HTML = './plots/ugc_sources_by_year_unstacked__.html'
    OUTPUT_PNG = './plots/ugc_sources_by_year_unstacked__.jpg'
    OUTPUT_SVG = './plots/ugc_sources_by_year.svg'

    term_citizen_science = 'citizen science'
    top_sources_in_figure = 5 # + citizen science added seperatly below

    def query_extract(query, source):
        # check if source is from the initial data pool (which was automated) or from the later iterations (webofknowledge) which was manually
        if source == 'webofknowledge':
            return query
        else:
            # used in map function on np array to extract search query of the paper
            patter = r'[A-Z]+\({2}(\({0,1}.+)(?=\) AND \(\(citizen science collective sens\*\))'
            try:
                match = re.match(patter, query).group(1)
            except:
                pass
            return match

    def check_cs(source):
        pattern = r'citizen'
        new_source = []
        for element in source:
            try:
                re.search(pattern, element, re.IGNORECASE).group(0)
                new_source.append(term_citizen_science)
            except Exception as e:
                new_source.append(element)
        return new_source

    rows = query_result_return(conn, "select ugc_source, query, source, date from literature where (decision_r_1 = '1' or decision_r_2 = '1') and subtype != 'Review';")
    rows = np.array([[row[0].lower().replace(' ', '').split(';'), row[1], row[2], row[3]] for row in rows])
    rows = np.array([[list(map(str.strip, row[0])), row[1], row[2], row[3]] for row in rows if row[1] is not None])
    # extract query term which resembles the sdg relevant keyword for which the paper was extracted
    sdg_terms_of_accepted_papers = np.array([[row[0], query_extract(row[1], row[2]), row[3]] for row in rows])
    # if citizen science is mentioned in remark or ugc_source it should be homogenised as 'CS' (citizen science)
    sdg_terms_of_accepted_papers = np.array([[check_cs(row[0]), row[1], row[2].year] for row in sdg_terms_of_accepted_papers if row[2] is not None])
    sdg_terms_of_accepted_papers = fix_different_data_source_names(sdg_terms_of_accepted_papers)
    overall_most_common_ugc_list = [source[0] for source in Counter([item for list_ in sdg_terms_of_accepted_papers for item in list_[0]]).most_common(top_sources_in_figure)]
    # add citizen science if not already among the top x sources
    if term_citizen_science not in overall_most_common_ugc_list:
        overall_most_common_ugc_list += [term_citizen_science]
    # get years
    years = set([item[2] for item in sdg_terms_of_accepted_papers])
    ugc_per_year_dict = {}
    for year in years:
        ugc_per_year = [item for row in sdg_terms_of_accepted_papers for item in row[0] if row[2] == year]
        ugc_per_year_counter = Counter(ugc_per_year).most_common(top_sources_in_figure)
        ugc_per_year_dict[year] = ugc_per_year_counter

    color_pallet = px.colors.qualitative.Pastel
    step = int(len(color_pallet) / 6)
    def get_ugc_color(opacity):
        ugc_color_dict = {
            'twitter': f'rgba(28, 147, 228, {opacity})',
            'flickr': f'rgba(251, 19, 133, {opacity})',
            'facebook': f'rgba(73, 90, 148, {opacity})',
            'instagram': f'rgba(252, 1, 216, {opacity})',
            'youtube': f'rgba(254, 0, 0, {opacity})',
            'sina weibo': f'rgba(242, 145, 48, {opacity})',
            'foursquare': f'rgba(231, 62, 107, {opacity})',
            'citizen science': f'rgba(19, 138, 7, {opacity})',
            'other': f'rgba(173, 173, 173, {opacity})'
        }
        return ugc_color_dict
    ugc_source_figure_color_dict = get_ugc_color(1)

    # ugc_source_figure_color_dict = {
    #     'flickr': color_pallet[0],
    #     'citizen science': color_pallet[step],
    #     'sina weibo': color_pallet[2*step],
    #     'twitter': color_pallet[3*step],
    #     'facebook': color_pallet[4*step],
    #     'other': '#b3b3b3'# color_pallet[5*step]
    # }

    df = pd.DataFrame(columns=['year', 'ugc_sources', 'count', 'color'])
    # append years and their top ugc sources as rows to df
    index = 0
    # years that shall not appear in the figure
    years_to_skip = [2021]
    exclude_years_below = 2005
    for year, ugc_counter in ugc_per_year_dict.items():
        if year in years_to_skip or year < exclude_years_below:
            continue
        other_sources_count = 0
        other_sources_list = []
        for ugc_item in ugc_counter:
            ugc_source = ugc_item[0]
            ugc_count = ugc_item[1]
            if ugc_source in overall_most_common_ugc_list:
                ugc_figure_color = ugc_source_figure_color_dict[ugc_source]
                row = [year, ugc_source, ugc_count, ugc_figure_color]
                df.loc[index] = row
            else:
                other_sources_count += ugc_count
                other_sources_list.append(ugc_source)
            index += 1
        row = [year, 'other', other_sources_count, ugc_source_figure_color_dict['other']]
        df.loc[index] = row
        index += 1
    # excepted will be terms that were manually added during the iteration of found LR
    # the terms therefore do not originate from the initial scopus api search
    # sort df
    df.sort_values(by=['count'], inplace=True, ascending=False)
    figure_colors = df['color']
    # holds individual bars for barchart
    for index, (label, label_df) in enumerate(df.groupby('ugc_sources')):
        # first element create figure, then append
        if index == 0:
            fig = go.Figure(go.Bar(x=label_df['year'], y=label_df['count'], name=label, marker={'color': ugc_source_figure_color_dict[label]}))
        else:
            fig.add_trace(go.Bar(x=label_df['year'], y=label_df['count'], name=label, marker={'color': ugc_source_figure_color_dict[label]}))

    fig.update_layout(xaxis={'categoryorder': 'category descending', 'dtick': 1},
                      legend=dict(
                          yanchor="top",
                          y=0.99,
                          xanchor="left",
                          x=0.01,
                          font_size=14),
                      font_size=30,
                      xaxis_title='year',
                      yaxis_title='count')

    # rotate xaxis labels
    fig.update_xaxes(tickangle=45)

    # barmode='stack',
    # fig.update_xaxes(categoryorder='category descending')
    fig.show()

    print('saving figures in different formats..')
    # fig.write_html(OUTPUT_HTML)
    # plotly.io.write_image(fig, format='png', file=OUTPUT_PNG, width=1900, height=800, engine='auto')
    # fig.write_image(OUTPUT_PNG)
    # fig.write_image(OUTPUT_SVG )

def ugc_titles_per_target(TARGET='3.9'):
    '''
    function used to extract the titles from each paper belonging to one target so that
    Word Clouds can be formed via the webservice www.wordart.com

    :return:
    '''
    def query_extract(query, source):
        # check if source is from the initial data pool (which was automated) or from the later iterations (webofknowledge) which was manually
        if source == 'webofknowledge':
            return query
        else:
            # used in map function on np array to extract search query of the paper
            patter = r'[A-Z]+\({2}(\({0,1}.+)(?=\) AND \(\(citizen science collective sens\*\))'
            try:
                match = re.match(patter, query).group(1)
            except:
                return None
            return match
    PATH = f'./plots/{TARGET.replace(".","_")}_wordcloud.txt'
    target_titles = []
    rows = query_result_return(conn, "select title, query, source from literature where (decision_r_1 = '1' or decision_r_2 = '1') and subtype != 'Review';")
    search_terms_per_target_dict = load_search_terms_per_target()
    sdg_terms_of_accepted_papers = np.array([(row[0], query_extract(row[1], row[2])) for row in rows])
    # papers_per_target = dict.fromkeys(search_terms_per_target_dict.values()
    for (title, term) in sdg_terms_of_accepted_papers:
        try:
            target_str = search_terms_per_target_dict[term]
            if target_str == TARGET:
                target_titles.append(title)
        except Exception as e:
            # excepted will be terms that were manually added during the iteration of found LR
            # the terms therefore do not originate from the initial scopus api search
            print(f'here {term}')

    # save to disk
    with open(PATH, 'wt', encoding='utf-8') as f:
        for title in target_titles:
            f.write(f'{title}\n')

    print(f'[*] Done. {len(target_titles)} titles under target {TARGET} included.')
    print(f'[*] total papers: {len(sdg_terms_of_accepted_papers)}')

def plot_initial_query_treemap():
    # from csv file no_restriciton
    INPUT_FILE = r"C:\Users\mhartman\PycharmProjects\SLR\treemap_data_no_restrictions.csv"

    labels = []
    values = []

    with open(INPUT_FILE, 'rt', encoding='utf-8') as f:
        csv_reader = csv.reader(f, delimiter=';')
        for i, line in enumerate(csv_reader):
            if i == 0:
                labels = line
            elif 'total' in line:
                values = line[1:]

    # add container value
    values = [0] + values
    # add container
    labels = ['query search terms'] + labels[1:]
    parents = ['query search terms'] * len(labels)
    parents[0] = ''

    fig = go.Figure(go.Treemap(
        labels=labels,
        values=values,
        parents=parents,
        root_color="lightblue"
    ))

    fig.update_layout(
        treemapcolorway=["pink", "lightblue"],
        margin=dict(t=50, l=25, r=25, b=25),
        font_size=20
    )

    fig.show()
    fig.write_html(os.path.join(r"C:\Users\mhartman\PycharmProjects\SLR_analysis\final_plots",
                                "treemap_all_terms.html"))


'''
FALSE: SEE reconstruct_initial_corpus_treemap.py for correct treemap of only the final, included terms
'''
def plot_final_query_treemap():
    # from csv file no_restriciton
    INPUT_FILE = r"C:\Users\mhartman\PycharmProjects\SLR\20200820_treemap_adapted_searchterms_no_abs.csv"

    labels = []
    values = []

    with open(INPUT_FILE, 'rt', encoding='utf-8') as f:
        csv_reader = csv.reader(f, delimiter=';')
        for i, line in enumerate(csv_reader):
            if i == 0:
                labels = line
            elif 'total' in line:
                values = line[1:]

    # add container value
    values = [0] + values
    # add container
    labels = ['query search terms'] + labels[1:]
    parents = ['query search terms'] * len(labels)
    parents[0] = ''

    fig = go.Figure(go.Treemap(
        labels=labels,
        values=values,
        parents=parents,
        root_color="lightblue"
    ))

    fig.update_layout(
        treemapcolorway=["pink", "lightblue"],
        margin=dict(t=50, l=25, r=25, b=25),
        font_size=20
    )
    fig.show()
    fig.write_html(os.path.join(r"C:\Users\mhartman\PycharmProjects\SLR_analysis\final_plots",
                                "treemap_only_included_terms_BUT_FALSE.html"))


if __name__ == '__main__':
    conn = connect_db()
    # plot_initial_query_treemap()
    plot_ugc_sources_by_year()
    # plot_ugc_sources_by_topic_revamped_sankeyplot()

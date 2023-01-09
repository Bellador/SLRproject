from db_querier import connect_db
from db_querier import query_result_return
from create_figures import load_search_terms_per_target
from collections import Counter
import matplotlib.pyplot as plt
import numpy as np
import re
import os
import pandas as pd
from collections import defaultdict
# plotting
import plotly.express as px
import plotly.graph_objs as go

'''
DPSIR classification was conducted for all included papers
Study area and author origin was only annotated for included citizen science projects
'''
def DPSIR_per_paper_type_sankeyplot():
    '''
    REVAMPTED:
    - define the top e.g. 5 data sources over all which shall always be displayed in the figure for each target (for legibility)
    - include the class Citizen Science projects

    load the used search terms for ugc terms and sdg target related terms. they are used to be compared with the results to
    evaluate for which targets applicable papers were found and were a possible research gab for UGC exists.
    Additionally, load the ugc sources and plot them with the targets they were used for

    :return:
    '''
    OUTPUT_HTML = './plots/ugc_sources_by_topic.html'
    OUTPUT_PNG = './plots/ugc_sources_by_topic.jpg'
    OUTPUT_SVG = './plots/ugc_sources_by_topic.svg'

    term_citizen_science = 'citizen science'
    top_sources_in_figure = 10  # + citizen science added separately below

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
        pattern = r'(citizen|crowed|crowd)'
        new_source = []
        for element in source:
            try:
                re.search(pattern, element, re.IGNORECASE).group(0)
                new_source.append(term_citizen_science)
            except Exception as e:
                new_source.append(element)
        return new_source

    conn = connect_db()
    rows = query_result_return(conn, "select dpsir, sdg from literature where (decision_r_1 = '1' or decision_r_2 = '1') and subtype != 'Review' and dpsir is not Null;")
    # add in the query above directly the 'sdg' column instead of recreating it here!!

    # convert nparray into df
    df = pd.DataFrame(rows, columns=['dpsir', 'topic'])
    # drop dk dpsir classifications
    df = df.drop(df[df.dpsir == 'dk'].index)
    # make DPSIR upper case
    df['dpsir'] = df['dpsir'].apply(lambda x: x.upper())
    # link target names to topic names
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
        '3.9': 'pollution',
        '3.3': 'diseases',
        '3.8': 'health monitoring',
        '3.5': 'medical abuse'
    }
    # node labels are used by their element's respective index
    node_labels = ['11.3', '3.9', '11.7', '11.5', '3.3', 'D', 'P', 'S', 'I', 'R']
    opacitiy = 0.75
    # convert to rgb tuple
    node_colors = [tuple(int(hex_.lstrip('#')[i:i+2], 16) for i in (0, 2, 4)) for hex_ in px.colors.qualitative.Plotly] # holds exactly 10 elements
    # convert to rgba string
    node_colors = [f'rgba({t[0]}, {t[1]}, {t[2]}, {opacitiy})' for t in node_colors]
    # add specific colours for DPSIR
    node_colors = node_colors[:-5] + [f'rgba(215, 25, 28, {opacitiy})',
                                      f'rgba(253, 174, 97, {opacitiy})',
                                      f'rgba(255, 255, 191, {opacitiy})',
                                      f'rgba(171, 221, 164, {opacitiy})',
                                      f'rgba(43, 131, 186, {opacitiy})',
                                      f'rgba(215, 25, 28, {opacitiy})']
    topics = ['11.5', '3.9', '11.3', '11.7', '3.3']


    # node_colors = [colors[node_index] for node_index, node in enumerate(node_labels)]
    link_colors = []
    link_sources = []
    link_targets = []
    link_values = []
    for topic_index, topic in enumerate(topics):
        for index, (label, label_df) in enumerate(df.groupby('topic')):
            if label == topic:
                label_df_grouped = label_df.groupby('dpsir').count()
                # only include topics with total count of X or greater
                # if label_df_grouped["topic"].sum() >= 20: # redundant since only labels are included that alread fulful this condition (here for legibility)
                #iterate over rows
                for dpsir, row in label_df_grouped.iterrows():
                    link_sources.append(node_labels.index(topic))
                    link_targets.append(node_labels.index(dpsir))
                    link_values.append(row[0])
                    # color based on link source
                    link_colors.append(node_colors[node_labels.index(topic)])

    # adapt node labels for visualisation
    node_labels = [f'{target_name_dict[e]} ({e})' if e in target_name_dict.keys() else e for e in node_labels]

    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=node_labels,
            color=node_colors
        ),
        link=dict(
            source=link_sources,  # indices correspond to labels, eg A1, A2, A1, B1, ...
            target=link_targets,
            value=link_values,
            color=link_colors
        ))])

    fig.update_layout(font_size=20) # title_text="Attribution of top 5 SDG topic related publications by DPSIR framework"
    fig.show()
    fig.write_html(os.path.join(r"C:\Users\mhartman\PycharmProjects\SLR_analysis\dpsir_plots", "dpsir_by_paper_topic_sankeyplot.html"))



def DPSIR_per_paper_type_stackedbar():
    '''
    REVAMPTED:
    - define the top e.g. 5 data sources over all which shall always be displayed in the figure for each target (for legibility)
    - include the class Citizen Science projects

    load the used search terms for ugc terms and sdg target related terms. they are used to be compared with the results to
    evaluate for which targets applicable papers were found and were a possible research gab for UGC exists.
    Additionally, load the ugc sources and plot them with the targets they were used for

    :return:
    '''
    OUTPUT_HTML = './plots/ugc_sources_by_topic.html'
    OUTPUT_PNG = './plots/ugc_sources_by_topic.jpg'
    OUTPUT_SVG = './plots/ugc_sources_by_topic.svg'

    term_citizen_science = 'citizen science'
    top_sources_in_figure = 10  # + citizen science added seperatly below

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

    conn = connect_db()
    rows = query_result_return(conn, "select dpsir, query, source from literature where (decision_r_1 = '1' or decision_r_2 = '1') and subtype != 'Review' and DPSIR is not Null;")

    rows = np.array([[row[0].lower().replace(' ', '').split(';'), row[1], row[2]] for row in rows])
    rows = np.array([[list(map(str.strip, row[0])), row[1], row[2]] for row in rows if row[1] is not None])
    # extract query term which resembles the sdg relevant keyword for which the paper was extracted
    dpsir_with_query = np.array([[row[0][0], query_extract(row[1], row[2])] for row in rows])
    # import sdg target search terms that were used for the SLR
    search_terms_per_target_dict = load_search_terms_per_target()
    dpsir_with_target = np.array([[row[0], search_terms_per_target_dict[row[1]]] for row in dpsir_with_query])

    # convert nparray into df
    df = pd.DataFrame(dpsir_with_target, columns=['dpsir', 'topic'])
    # drop dk dpsir classifications
    df = df.drop(df[df.dpsir == 'dk'].index)
    # make DPSIR upper case
    df['dpsir'] = df['dpsir'].apply(lambda x: x.upper())
    # link target names to topic names
    target_name_dict = {
        '11.1': 'urban inequality',
        '11.2': 'mobility',
        '11.3': 'sustainable urban planning',
        '11.4': 'cultural and natural heritage protection',
        '11.5': 'disasters',
        '11.6': "citie's environmental impact",
        '11.7': 'green and public space',
        '11.a': 'development planning',
        '11.b': 'disaster risk reduction',
        '11.c': 'developing country support',
        '3.4': 'mental health and well-being',
        '3.6': 'traffic accidents',
        '3.9': 'pollution',
        '3.3': 'diseases',
        '3.8': 'health monitoring',
        '3.5': 'medical abuse'
    }
    # df['topic'] = df['topic'].apply(lambda x: target_name_dict[x] + f' ({x})')
    # initialise figure
    fig = go.Figure()
    order = ['11.5', '3.9', '11.3', '11.7', '3.3']
    total_count = 0
    for item in order:
        for index, (label, label_df) in enumerate(df.groupby('topic')):
            if label == item:
                label_df_grouped = label_df.groupby('dpsir').count()
                # only include topics with total count of X or greater
                if label_df_grouped["topic"].sum() >= 20:
                    fig.add_trace(go.Bar(x=label_df_grouped.index.values, y=label_df_grouped['topic'], name=target_name_dict[label] + f' ({label})'))
                    print(f'[*] topic: {label}, count: {label_df_grouped["topic"].sum()}')
                    total_count += label_df_grouped["topic"].sum()
    print(f'[*] total_count: {total_count}')
    # change layout to descending order
    fig.update_layout(barmode='stack',
                      xaxis={'categoryorder': 'array', 'categoryarray': ['D', 'P', 'S', 'I', 'R'], 'title': None},
                      yaxis={'categoryorder': 'category descending'},
                      yaxis_title='count',
                      legend=dict(
                          yanchor="top",
                          y=0.99,
                          xanchor="left",
                          x=0.01
                      ))

    fig.show()
    fig.write_html(os.path.join(r"C:\Users\mhartman\PycharmProjects\SLR_analysis\dpsir_plots", "dpsir_by_paper_topic_stackedbar.html"))


def study_country_with_match_no_match_dist():
    '''
    visualise the distribution of areas that have been covered by scientific research to
    potentially unravel a north-south hemisphere bias

    aggregate by continent:

    :return:
    '''

    # taken from author origin - study origin match code

    query = """select author_country_list, study_area_country  
                                from literature 
                                where study_area_country is not null;"""

    conn = connect_db()
    results = query_result_return(conn, query)

    # take care of UK, if both countries compared are in here, its a match
    UK_list = ['UK', 'England', 'Wales', 'Ireland', 'Scotland']

    EU = ('Europe', ['Europe', 'UK', 'Germany', 'England', 'Wales', 'Ireland', 'Scotland', 'France', 'Denmark', 'Netherlands', 'Belgium', 'Switzerland'])  # Europe
    NA = ('North America', ['USA', 'Canada'])  # North America
    SA = ('South America', ['Peru', 'Brazil', 'Mexico', 'Cuba'])  # South America
    AF = ('Africa', ['Egypt'])  # Africa
    AS = ('Asia', ['India', 'China', 'Hong Kong', 'South Korea'])  # Asia
    OC = ('Oceania', ['Australia', 'New Zealand'])  # Oceania

    continent_lists = [EU, NA, SA, AF, AS, OC]

    storage_ = defaultdict(lambda: {'match': 0, 'no_match': 0})

    def match_continent(study_country):
        matched_continent = False
        for continent_ in continent_lists:
            if study_country in continent_[1]:
                matched_continent = continent_[0]
        if not matched_continent and (study_country == 'Global' or study_country == 'global'):
            matched_continent = 'Global'
        return matched_continent

    for result in results:
        # 1. check if there is a match between author origin and study area
        author_country_s, study_country_s = result
        # check if there are multiple authors and countries
        if ';' in author_country_s:
            author_country_list = author_country_s.split(';')
        else:
            author_country_list = [author_country_s]
        if ';' in study_country_s:
            study_country_list = study_country_s.split(';')
        else:
            study_country_list = [study_country_s]
        # iterate over each element in the author countries and check if they are in the study countr(ies)
        for country in study_country_list:
            if country in author_country_list:
                matched_continent = match_continent(country)
                if matched_continent:
                    storage_[matched_continent]['match'] += 1
            elif country == 'Global' or country == 'global':
                    storage_['Global']['match'] += 1
            # all instances where Europe is the study area also at least one of the authors is from there (manually checked)
            elif country == 'Europe':
                storage_['Europe']['match'] += 1
            elif country in UK_list and (all(x in UK_list for x in study_country_list)):
                storage_['Europe']['match'] += 1
            else:
                matched_continent = match_continent(country)
                if matched_continent:
                    storage_[matched_continent]['no_match'] += 1

    # build dataframe for plotting
    data_array = []
    columns = ['continent', 'match', 'no match', 'total']
    for k, v in storage_.items():
        data_array.append([k, v['match'], v['no_match'], v['match'] + v['no_match']])

    df = pd.DataFrame(data_array, columns=columns)

    fig = px.bar(df.sort_values(by='total', ascending=False), x="continent", y=["match", "no match"]) #title="Study areas by continent with indication if match with author origin exists"

    fig.update_layout(barmode='stack',
                      yaxis_title='count',
                      xaxis_title='continent of study area',
                      font_size=25,
                      legend=dict(
                          title='',
                          yanchor="top",
                          y=0.99,
                          xanchor="right",
                          x=0.99
                      ))
    fig.show()
    # fig.write_html(os.path.join(r"C:\Users\mhartman\PycharmProjects\SLR_analysis\final_plots",
    #                             "continent_w_author_match.html"))
    fig.write_image(os.path.join(r"C:\Users\mhartman\PycharmProjects\SLR_analysis\final_plots",
                                "continent_w_author_match.jpg"))


def author_country_study_country_rel():
    '''
    investigate if the author and affiliation countr(ies) matches the study country for citizen science projects.
    We differentiate between match and no-match. Global studies are considered a match independent of author origin

    study_area_region (lower level of study_area_country) is NOT utilised for now - even though it was annotated

    Countries within the UK are considered a match with authors from any of these countries!

    :return:
    '''

    query = """select author_country_list, study_area_country  
                            from literature 
                            where study_area_country is not null;"""

    conn = connect_db()
    results = query_result_return(conn, query)

    total_match = 0
    total_no_match = 0
    global_ = 0

    # take care of UK, if both countries compared are in here, its a match
    UK_list = ['UK', 'England', 'Wales', 'Ireland', 'Scotland']

    for result in results:
        author_country_s, study_country_s = result
        # check if there are mulitple authors and countries
        if ';' in author_country_s:
            author_country_list = author_country_s.split(';')
        else:
            author_country_list = [author_country_s]
        if ';' in study_country_s:
            study_country_list = study_country_s.split(';')
        else:
            study_country_list = [study_country_s]

        # iterate over each element in the author countries and check if they are in the study countr(ies)
        match = False
        for country in author_country_list:
            if country in study_country_list:
                match = True
            elif 'Global' in study_country_list or 'global' in study_country_list:
                match = True
            # all instances where Europe is the study area also at least one of the authors is from there (manually checked)
            elif 'Europe' in study_country_list:
                match = True
            elif country in UK_list and (all(x in UK_list for x in study_country_list)):
                match = True
            # check for GB countries
        if match:
            total_match += 1
        else:
            total_no_match += 1
            print(f'{author_country_list} : {study_country_list}')

    print(f'[*] match: {total_match}\n no match: {total_no_match}\n     global: {global_}')

    # plot bars in stack manner
    plt.bar(['match', 'no match'], [total_match, total_no_match], color='r')
    plt.ylabel('count')
    plt.show()

def dpsir_twitter_vs_cs():
    '''
    Compare the relative share of DPSIR dimensions found within included publications using either
    Twitter or Citizen Science as underlying data source.
    This data exploration shall allow assumptions if CS contributes different data dimensions compared
    to the most dominant UGC source, Twitter.

    :return:
    '''
    twitter_dpsir_query = """select dpsir 
                                from literature 
                                where (decision_r_1 = '1' or decision_r_2 = '1') 
                                and subtype != 'Review' 
                                and ugc_source ilike '%twitter%'
                                and dpsir is not NULL;"""
    cs_dpsir_query = """select dpsir 
                                from literature 
                                where (decision_r_1 = '1' or decision_r_2 = '1') 
                                and subtype != 'Review' 
                                and (ugc_source ilike '%citizen%' or ugc_source ilike '%crowd%')
                                and dpsir is not NULL;"""
    conn = connect_db()
    twitter_results = query_result_return(conn, twitter_dpsir_query)
    cs_results = query_result_return(conn, cs_dpsir_query)
    twitter_c = Counter(twitter_results.flatten())
    cs_c = Counter(cs_results.flatten())
    # plot stacked bar plot on relative numbers of DPSIR contributions, not considering 'dont know' classifications
    x_labels = ['D', 'P', 'S', 'I', 'R']
    X = np.arange(5)
    twitter_data = []
    cs_data = []
    # sum without 'dk'
    # twitter_sum =
    for element in x_labels:
        # get value and calculate relative share
        twitter_val = round(twitter_c[element] / sum([v for k, v in twitter_c.items() if k != 'dk']) * 100)
        cs_val = round(cs_c[element] / sum([v for k, v in cs_c.items() if k != 'dk']) * 100)
        twitter_data.append(twitter_val)
        cs_data.append(cs_val)
    # print information
    print(f'[*] Absolute Twitter: \n {twitter_c}')
    print(f'[*] Relative Twitter: \n {twitter_data}')
    print(f'[*] Absolute CS: \n {cs_c}')
    print(f'[*] Relative CS: \n {cs_data}')
    fig = plt.figure()
    ax = fig.add_axes([0.1, 0.1, 0.8, 0.8])
    # # plot bars in stack manner
    # plt.bar(x_labels, cs_data, color='r', label='citizen science')
    # plt.bar(x_labels, twitter_data, bottom=cs_data, color='b', label='twitter')
    # plot bar plot in normal manner
    ax.bar(X - 0.2, cs_data, color='r', label='citizen science', width=0.4)
    ax.bar(X + 0.2, twitter_data, color='b', label='twitter', width=0.4)
    ax.legend()
    ax.set_ylabel('%-share')
    plt.xticks(X, x_labels)
    # ax.set_xticklabels(x_labels)
    labels = [item.get_text() for item in ax.get_xticklabels()]
    print(labels)
    plt.show()



if __name__ == '__main__':
    # dpsir_twitter_vs_cs()
    study_country_with_match_no_match_dist()
    # DPSIR_per_paper_type_sankeyplot()
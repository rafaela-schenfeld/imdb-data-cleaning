"""
This script is to filter the data for a specific TV series called Fringe from IMDb data tables.

IMDb datasets used can be found at:
https://developer.imdb.com/non-commercial-datasets/
"""

import os
import pandas as pd
import gzip

# create a new folder to store processed files
output_folder = './processed_fringe_data'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# function to read gzipped TSV files
def read_gzipped_tsv(filename):
    with gzip.open(filename, 'rt', encoding='utf-8') as f:
        return pd.read_csv(f, sep='\t', low_memory=False)

# find Fringe series tconst
title_basics = read_gzipped_tsv('./data/title.basics.tsv.gz')
fringe_series = title_basics[
    (title_basics['primaryTitle'] == 'Fringe') & 
    (title_basics['titleType'] == 'tvSeries')
]['tconst'].values[0]

# get episodes with titles and runtime
title_episode = read_gzipped_tsv('./data/title.episode.tsv.gz')
fringe_episodes = title_episode[title_episode['parentTconst'] == fringe_series]
fringe_episodes = pd.merge(fringe_episodes, 
                           title_basics[['tconst', 'primaryTitle', 'originalTitle', 'runtimeMinutes']], 
                           on='tconst')
fringe_episodes.rename(columns={'primaryTitle': 'episodeTitle'}, inplace=True)

# convert seasonNumber and episodeNumber to integers for sorting
fringe_episodes['seasonNumber'] = pd.to_numeric(fringe_episodes['seasonNumber'])
fringe_episodes['episodeNumber'] = pd.to_numeric(fringe_episodes['episodeNumber'])
fringe_episodes = fringe_episodes.sort_values(['seasonNumber', 'episodeNumber'])

# add episodes index
fringe_episodes['episodeIndex'] = range(1, len(fringe_episodes) + 1)
fringe_episodes.to_csv(f'{output_folder}/fringe_episodes.csv', index=False)

# get ratings 
title_ratings = read_gzipped_tsv('./data/title.ratings.tsv.gz')
fringe_ratings = title_ratings[title_ratings['tconst'].isin(fringe_episodes['tconst'])]
fringe_ratings.to_csv(f'{output_folder}/fringe_ratings.csv', index=False)

# get crew information 
title_crew = read_gzipped_tsv('./data/title.crew.tsv.gz')
fringe_crew = title_crew[title_crew['tconst'].isin(fringe_episodes['tconst'])]
fringe_crew.to_csv(f'{output_folder}/fringe_crew.csv', index=False)

# get character information 
title_principals = read_gzipped_tsv('./data/title.principals.tsv.gz')
fringe_characters = title_principals[
    (title_principals['tconst'].isin(fringe_episodes['tconst'])) & 
    (title_principals['category'].isin(['actor', 'actress']))
]
fringe_characters = fringe_characters.drop(columns=['job'])
fringe_characters.to_csv(f'{output_folder}/fringe_characters.csv', index=False)

# create a lookup table for names
unique_nconsts = set(fringe_crew['directors'].str.split(',').sum() +
                     fringe_crew['writers'].str.split(',').sum() +
                     fringe_characters['nconst'].tolist())

name_basics = read_gzipped_tsv('./data/name.basics.tsv.gz')
name_lookup = name_basics[name_basics['nconst'].isin(unique_nconsts)][['nconst', 'primaryName']]
name_lookup.to_csv(f'{output_folder}/name_lookup.csv', index=False)

print(f"Data processing complete. Files saved to {output_folder}.")
import pandas as pd
import logging


def transform_grammys(data):
    """Defines the columns database data 'Grammy' dataset to be used in the analysis."""
    try:
        columns_map = ['category', 'nominee', 'winner', 'year']
        data = data[columns_map]
        data = data.dropna()
    except Exception as e:
        logging.error(f'Error: {str(e)}')
    
    return data


def transform_spotify(data):
    """Defines the columns csv data 'Spotify' dataset to be used in the analysis."""
    columns_map = [
        'Unnamed: 0',
        'artists',
        'track_name',
        'popularity',
        'duration_ms',
        'explicit',
        'danceability',
        'energy',
        'loudness',
        'speechiness',
        'acousticness',
        'instrumentalness',
        'liveness',
        'valence',
        'tempo',
        'track_genre'
    ]

    genere_map = {
        "rock": "Rock and similars",
        "alt-rock": "Rock and similars",
        "alternative": "Rock and similars",
        "grunge": "Rock and similars",
        "punk": "Rock and similars",
        "rockabilly": "Rock and similars",
        "punk-rock": "Rock and similars",
        "hard-rock": "Rock and similars",
        "rock-n-roll": "Rock and similars",
        "psych-rock": "Rock and similars",
        "pop-film": "Pop and similars",
        "power-pop": "Pop and similars",
        "pop": "Pop and similars",
        "pop-rock": "Pop and similars",
        "synth-pop": "Pop and similars",
        "mandopop": "Pop and similars",
        "electronic": "Electronic and similars",
        "techno": "Electronic and similars",
        "detroit-techno": "Electronic and similars",
        "trance": "Electronic and similars",
        "minimal-techno": "Electronic and similars",
        "electro": "Electronic and similars",
        "deep-house": "Electronic and similars",
        "dubstep": "Electronic and similars",
        "house": "Electronic and similars",
        "progressive-house": "Electronic and similars",
        "hip-hop": "HipHop and Rap",
        "afrobeat": "HipHop and Rap",
        "rap": "HipHop and Rap",
        "trip-hop": "HipHop and Rap",   
        "reggae": "Reggae and Dancehall",
        "dancehall": "Reggae and Dancehall",
        "metal": "Metal and similars",
        "black-metal": "Metal and similars",
        "death-metal": "Metal and similars",
        "heavy-metal": "Metal and similars",
        "metalcore": "Metal and similars",
        "jazz": "Jazz and Blues",
        "blues": "Jazz and Blues",
        "classical": "Classic Music",
        "opera": "Classic Music",
        "tango": "Classic Music",
        "acoustic": "Classic Music",
        "piano": "Classic Music",
        "guitar": "Classic Music",
        "latino": "Latin Music",
        "salsa": "Latin Music",
        "samba": "Latin Music",
        "reggaeton": "Latin Music",
        "spanish": "Latin Music",
        "brazil": "Latin Music",
        "ambient": "Others",
        "comedy": "Others",
        "country": "Others",
        "disco": "Others",
        "funk": "Others",
        "gospel": "Others",
        "indie": "Others",
        "indie-pop": "Others",
        "new-age": "Others",
        "soul": "Others",
        "songwriter": "Others",
        "drum-and-bass": "Others",
        "bluegrass": "Others",
        "breakbeat": "Others",
        "cantopop": "Others",
        "world-music": "Others",
        "chicago-house": "Others",
        "anime": "Special Categories",
        "children": "Special Categories",
        "disney": "Special Categories",
        "kids": "Special Categories",
        "party": "Special Categories",
        "sad": "Special Categories",
        "sleep": "Special Categories",
        "study": "Special Categories",
        "singer-songwriter": "Special Categories",
        "ska": "Special Categories",
        "show-tunes": "Special Categories",
        "sertanejo": "Special Categories",
        "r-n-b": "Special Categories",
        "pagode": "Special Categories",
        "malay": "Special Categories",
        "mpb": "Special Categories",
        "industrial": "Special Categories",
        "idm": "Special Categories", 
        "honky-tonk": "Special Categories",
        "hardstyle": "Special Categories",
        "hardcore": "Special Categories",
        "happy": "Special Categories",
        "groove": "Special Categories",
        "goth": "Special Categories",
        "grindcore": "Special Categories",
        "garage": "Special Categories",
        "emo": "Special Categories",
        "dub": "Special Categories",
        "edm": "Special Categories",
        "forro": "Special Categories",
        "romance": "Special Categories",
        "club": "Special Categories",
        "dance": "Special Categories",
        "folk": "Special Categories",
        "chill": "Special Categories",
        "swedish": "European Music",
        "french": "European Music",
        "turkish": "European Music",
        "iranian": "European Music",
        "german": "European Music",
        "british": "European Music",
        "j-dance":"Oriental Music",
        "j-idol":"Oriental Music",
        "j-pop":"Oriental Music",
        "j-rock":"Oriental Music",
        "k-pop":"Oriental Music",
        "indian":"Oriental Music",
    }
    try:
        data = data[columns_map]
        data['duration_min'] = round(data['duration_ms'] / 60000, 2)
        data = data.drop(columns=['duration_ms'])
        data['track_genre_grouped'] = data['track_genre'].replace(genere_map)
        data['duration_grouped'] = pd.cut(
            x=data['duration_min'], bins=[0, 2, 3, 5, 10, 90],
            labels=['0-2 min', '2-3 min', '3-5 min', '5-10 min', '10+ min'],
            right=False
        )
        data['popularity_grouped'] = pd.cut(
            x=data['popularity'], bins=[0, 20, 40, 60, 80, 100],
            labels=['0-20', '20-40', '40-60', '60-80', '80-100'],
            right=False
        )
        data['valence_grouped'] = pd.cut(
            x=data['valence'], bins=[0, 0.2, 0.4, 0.6, 0.8, 1],
            labels=['0-0.2', '0.2-0.4', '0.4-0.6', '0.6-0.8', '0.8-1'],
            right=False
        )
        data['danceability_grouped'] = pd.cut(
            x=data['danceability'], bins=[0, 0.2, 0.4, 0.6, 0.8, 1],
            labels=['0-0.2', '0.2-0.4', '0.4-0.6', '0.6-0.8', '0.8-1'],
            right=False
        )
        data['energy_grouped'] = pd.cut(
            x=data['energy'], bins=[0, 0.2, 0.4, 0.6, 0.8, 1],
            labels=['0-0.2', '0.2-0.4', '0.4-0.6', '0.6-0.8', '0.8-1'],
            right=False
        )
        data['speechiness_grouped'] = pd.cut(
            x=data['speechiness'], bins=[0, 0.2, 0.4, 0.6, 0.8, 1],
            labels=['0-0.2', '0.2-0.4', '0.4-0.6', '0.6-0.8', '0.8-1'],
            right=False
        )
        data['loudness_grouped'] = pd.cut(
            x=data['loudness'], bins=[-50, -40, -30, -20, -10, 0, 10],
            labels=['-50 - -40', '-40 - -30', '-30 - -20', '-20 - -10', '-10 - 0', '0 - 10'],
            right=False
        )
        data['acousticness_grouped'] = pd.cut(
            x=data['acousticness'], bins=[0, 0.2, 0.4, 0.6, 0.8, 1],
            labels=['0-0.2', '0.2-0.4', '0.4-0.6', '0.6-0.8', '0.8-1'],
            right=False
        )
        data['instrumentalness_grouped'] = pd.cut(
            x=data['instrumentalness'], bins=[0, 0.2, 0.4, 0.6, 0.8, 1],
            labels=['0-0.2', '0.2-0.4', '0.4-0.6', '0.6-0.8', '0.8-1'],
            right=False
        )
        data['liveness_grouped'] = pd.cut(
            x=data['liveness'], bins=[0, 0.2, 0.4, 0.6, 0.8, 1],
            labels=['0-0.2', '0.2-0.4', '0.4-0.6', '0.6-0.8', '0.8-1'],
            right=False
        )
        data = data[data['tempo'] != 0]
        data['tempo_grouped'] = pd.cut(
            x=data['tempo'], bins=[30, 50, 100, 150, 200, 250],
            labels=['30 - 50', '50 - 100', '100 - 150', '150 - 200', '200 - 250'],
            right=False
        )
        data = data.dropna()
    except Exception as e:
        logging.error(f'Error: {str(e)}')

    return data
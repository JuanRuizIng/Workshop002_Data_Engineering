# Workshop #2: Data Engineer <img src="https://github.com/user-attachments/assets/e50b269a-fd97-4ec3-a1e9-e9629cef94ae" alt="Data Icon" width="30px"/>

Realized by **Juan Andrés Ruiz Muñoz** ([@JuanRuizIng](https://github.com/JuanRuizIng)).

## Overview ✨

In this workshop we use Spotify and Grammys data. With this data we run loading, cleaning and transformation processes to find interesting insights using the following tools:

* Python ➜ [Download site](https://www.python.org/downloads/)
* Jupyter Notebook ➜ [VS Code tool for using notebooks](https://youtu.be/ZYat1is07VI?si=BMHUgk7XrJQksTkt)
* PostgreSQL ➜ [Download site](https://www.postgresql.org/download/)
* Power BI (Desktop version) ➜ [Download site](https://www.microsoft.com/es-es/power-platform/products/power-bi/desktop)
* Apache Airflow [Download site](https://github.com/apache/airflow)

The libraries needed for Python are

* Pandas
* Matplotlib
* Seaborn
* SQLAlchemy
* Python-dotenv

These libraries are included in the *requirements.txt* file.

## Dataset Information <img src="https://github.com/user-attachments/assets/5fa5298c-e359-4ef1-976d-b6132e8bda9a" alt="Dataset" width="30px"/>


The first dataset used, *[Spotify Dataset](https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset)*, has 114000 rows and 21 columns of Spotify tracks over a range of 125 different genres. Each track has some audio features associated with it.
This dataset is further transformed to be better processed by the visual analysis tool.
Initially, the column names of the dataset and their respective Dtype are:

* "Unnamed: 0" ➜ int
* "track_id" ➜ object
* "artists" ➜ object
* "album_name" ➜ object
* "track_name" ➜ object
* "popularity" ➜ int
* "duration_ms" ➜ int
* "explicit" ➜ bool
* "danceability" ➜ float
* "energy" ➜ float
* "key" ➜ int
* "loudness"➜ float
* "mode" ➜ int
* "speechiness" ➜ float
* "acousticness" ➜ float
* "instrumentalness" ➜ float
* "liveness" ➜ float
* "valence" ➜ float
* "tempo" ➜ float
* "time_signature" ➜ int
* "track_genre" ➜ object

The second dataset used, *[Grammy Awards](https://www.kaggle.com/datasets/unanimad/grammy-awards/data)*, has 4800 rows and 10 columns of Grammy, is an award presented by The Recording Academy to recognize achievements in the music industry. The trophy depicts a gilded gramophone. The annual presentation ceremony features performances by prominent artists, and the presentation of those awards that have a more popular interest. The Grammys are the second of the Big Three major music awards held annually (between the American Music Awards in the Fall, and the Billboard Music Awards in the Summer).
This dataset will be transformed and enabled for a merge into the spotify dataset.
Initially, the column names of the dataset and their respective Dtype are:

* "year" ➜ int
* "title" ➜ object
* "published_at" ➜ object
* "updated_at" ➜ object
* "category" ➜ object
* "nominee" ➜ object
* "artist" ➜ object
* "workers" ➜ object
* "img" ➜ object
* "winner" ➜ bool

### Clone the repository

Execute the following command to clone the repository:

```bash
  https://github.com/JuanRuizIng/Workshop002_Data_Engineering.git
```

### Enviromental variables

> From now on, the steps will be done in VS Code.

To establish the connection to the database, we use a module called *database.py*. In this Python script we call a file where our environment variables are stored, this is how we will create this file:

1. We create a directory named ***auth*** inside ***src*** folder.

2. There we create a file called ***.env***.

3. In that file we declare 6 enviromental variables. Remember that the variables in this case go without double quotes, i.e. the string notation (`"`):
   ```python
    PG_HOST = # host address, e.g. localhost or 127.0.0.1
    PG_PORT = # PostgreSQL port, e.g. 5432

    PG_USER = # your PostgreSQL user
    PG_PASSWORD = # your user password
    
    PG_DRIVER = postgresql+psycopg2
    PG_DATABASE = # your database name, e.g. postgres
   ```

4. *(Optional)* If you want the file to be saved on the drive, in ***auth*** you will need to save your google services key and edit the store.py file. [For more information here](https://youtu.be/tamT_iGoZDQ?si=KIhvL3jQFgn9GhAJ).

### Set the csvs

Place spotify_dataset.csv and the_grammys_awards.csv inside the src/data folder

![image](https://github.com/user-attachments/assets/a4a287ee-8c26-48e9-9ad4-41fd4dfdbee9)

### Airflow configuration

In your airflow.cfg, you need configure to run the dags:

```python
dags_folder = /home/[USER]/Workshop002_Data_Engineering/dags
```

### Libraries

you will need to access your virtual environment and secure and execute in the terminal

```bash
pip install -r requirements.txt
```

### Running the code

1. Execute loadRaw.py in src/loadRaw folder

2. In the terminal, we execute "airflow standalone" and login in airflow

3. Execute the ETL process named:

![image](https://github.com/user-attachments/assets/7d83fe0a-515f-409d-a838-a3f959103d4d)

4. Check if everything is in order in logs or by viewing the executed tasks in the dashboard

![image](https://github.com/user-attachments/assets/26b66585-e50d-4775-bd48-3556e499d941)

5. your graphs should look like this:

![image](https://github.com/user-attachments/assets/ebca96f5-3d6d-4241-9a47-5c3dce087016)

6. To view the EDA process, we execute the 2 notebooks following whatever order. You can run it just pressing the "Execute All" button. Remember the notebooks in notebooks folder:

   1. *EDA_grammys.ipynb*
   2. *EDA_spotify.ipynb*
  
Remember to choose **the appropriate Python kernel** when running the notebook and **install the *ipykernel*** to support Jupyter notebooks in VS Code with the venv virtual environment.

### Connecting the database with Power BI

1. Open Power BI Desktop and create a new dashboard. Select the *Get data* option - be sure you choose the "PostgreSQL Database" option.

![Power BI](https://github.com/user-attachments/assets/a53ef992-d5b9-468e-b227-94e72179a591)


2. Insert the PostgreSQL Server and Database Name.

![image](https://github.com/user-attachments/assets/ebe02754-44e8-498c-891f-e1a0038d351d)


3. Fill in the following fields with your credentials.

![image](https://github.com/user-attachments/assets/18748b7f-7d5c-4c21-891a-70e77dd21d69)


4. If you manage to connect to the database the following tables will appear:

![image](https://github.com/user-attachments/assets/88e4a3f3-de9e-404b-a360-fabc40269759)


5. Choose the merged_data_spotify_grammys table and start making your own visualizations!

![image](https://github.com/user-attachments/assets/82ab916c-08d4-45ab-aa8a-59b67de5f7a1)


## Thank you! 💩🐍

Law 7, The 48 Laws of Power - Robert Greene

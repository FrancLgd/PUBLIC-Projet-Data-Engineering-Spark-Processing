#!/usr/bin/env python
# coding: utf-8

#### ---- LIBRAIRIES

from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as func
from pyspark.sql.window import Window
import argparse
from datetime import datetime, timedelta
import pandas as pd

#### ---- PARAMETRES

# Command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument('--DATE_START', help = "Date de départ", type = str, default = "2019-10-01 00:00:00")
parser.add_argument('--DATE_END', help = "Date de fin", type = str, default = "2019-10-05 23:00:00")
parser.add_argument('--DESTINATION', help = "Destination file path", type = str, default = "gs://client_event_brutes/extracted_{}_{}.csv")

args = parser.parse_args() 

DATE_START = datetime.strptime(args.DATE_START,"%Y-%m-%d %H:%M:%S")
DATE_END =  datetime.strptime(args.DATE_END,"%Y-%m-%d %H:%M:%S")
DESTINATION = args.DESTINATION.format(DATE_START.strftime("%Y%m%d"),DATE_END.strftime("%Y%m%d"))

# Initialisation du sparkContext
spark = SparkSession \
    .builder \
    .appName("PySpark") \
    .getOrCreate()

sc = spark.sparkContext

sql_c = SQLContext(spark.sparkContext)

# Pour éviter le bug de conversion de date
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

#### ---- TRAITEMENTS

# Checker si DATE_START < DATE_END
if DATE_START >= DATE_END:
    raise ValueError("DATE_END doit être plus ancien que DATE_START.")


# Lire le(s) fichier(s) csv

# ... lister les mois concernes
months_list = []
current_date = DATE_START
while current_date <= DATE_END:
    months_list.append(current_date.strftime("%Y-%b"))
    current_date = (current_date + timedelta(days=31)).replace(day=1)

# ... en deduire les fichiers
    

files_list = ["gs://client_event_brutes/blent-learning-user-ressources.s3.eu-west-3.amazonaws.com/projects/9c15cb/{}.csv".format(month) for month in months_list]

print(files_list)

# files_list = ["gs://client_event_brutes/blent-learning-user-ressources.s3.eu-west-3.amazonaws.com/projects/9c15cb/sample.csv"]


# ... charger tous les fichiers dans une seule table
data = sql_c.read.option("header", True).csv(files_list[0])
if(len(files_list)>1):
    for file in files_list[1:]:
        data = data.union(sql_c.read.option("header", True).csv(file))

data.show()

# Convertir les variables dans le bon type 
data.printSchema()

data = data \
    .withColumn("event_time", func.to_timestamp("event_time","yyyy-MM-dd HH:mm:ss")) \
    .withColumn("product_id", func.col("product_id").cast("int")) \
    .withColumn("price", func.col("price").cast("double")) \
    .withColumn("user_id", func.col("user_id").cast("int"))

data.printSchema()

# Filtrer les lignes entre DATE_START et DATE_END
data = data \
    .filter((func.col("event_time") >= DATE_START) & (func.col("event_time") <= DATE_END))

# Agréger à la maille (session x article x utilisateur)
session_product_data = data \
    .groupby("user_session","product_id","user_id") \
    .agg(
        
        # ... calculer le prix moyen de cet article pendant la session
        func.mean(func.col("price")).alias("price"),

        # ... obtenir la catégorie de cet article (on suppose logiquement que c'est la même durant toute la session)
        func.first("category_code").alias("category_code"),
                   
        # ... obtenir la  marque de cet article (on suppose logiquement que c'est la même durant toute la session)
        func.first("brand").alias("brand"),
        
        # ... compter le nombre de vues de cet article dans la session
        func.sum(func.when(func.col("event_type") == "view", 1).otherwise(0)).alias("num_views_product"),

        # ... regarder si 'purchase' est apparu au moins une fois pour cet article dans les événements de la session
        func.when(func.sum(func.when(func.col("event_type") == "purchase", 1).otherwise(0))>=1,True).otherwise(False).alias("purchased"),
        
    )

session_product_data.show()

# Agréger à la maille (session x utilisateur)
session_data = data \
    .groupby("user_session","user_id") \
    .agg(
        
        # ... obtenir la date du premier événement de la session
        func.min("event_time").alias("start_time"),

        # ... obtenir la date du dernier événement de la session
        func.max("event_time").alias("end_time"),

        # ... compter le nombre d'articles différents vus dans la session
        func.countDistinct("product_id").alias("num_views_session")
        
    ) \
    .withColumn("duration", 

                # ... calculer la durée de la session en secondes
                (func.unix_timestamp("end_time")-func.unix_timestamp("start_time")).cast("int")
                
               )\
    .withColumn("start_hour",
                
                # ... heure de début de session
                func.hour("start_time")
                
               )\
    .withColumn("start_minute",
                
                # ... minute de début de session
                func.minute("start_time")
                
               )\
    .withColumn("start_dayofweek",
                
                # ... jour de début de session
                func.dayofweek("start_time")
                
               )

session_data.show()

# Créez une fenêtre temporelle basée sur la colonne event_time
windowSpec = Window.partitionBy("user_id").orderBy("start_time")

session_data = session_data \
    .withColumn("sessions_precedentes",
                # ... nombre de sessions precedentes
                func.row_number().over(windowSpec) - 1
               )

session_data.show()


# Joindre les tables (session x article x utilisateur) et (session x utilisateur)
session_product_data = session_product_data \
    .join(session_data,on = ["user_id","user_session"])


session_product_data.show()

# Calculer le nombre de fois où l'article a été déjà vu dans des sessions précédentes
windowSpec = Window.partitionBy("user_id","product_id").orderBy("start_time")

session_product_data = session_product_data \
    .withColumn("num_prev_and_current_product_views",
                
                # ... nombre de fois où l'article a été vu dans les sessions précédentes (session courante inclue)
                func.sum("num_views_product").over(windowSpec)
               ) \
    .withColumn("num_prev_product_views",

                # ... nombre de fois où l'article a été déjà vu dans des sessions précédentes (session courante exclue)
                func.col("num_prev_and_current_product_views") - func.col("num_views_product")
               )


session_product_data.show()

# Reduire le nombre de partitions
session_product_data = session_product_data.repartition(1)

# Ecrire dans le fichier de destination
session_product_data.write.option("header", True).csv(DESTINATION)

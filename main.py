
import nltk
from pyspark.sql import functions as F, types as T


def download_nltk_data():
    nltk.download('all')

def join_pet_data():
    train_data = spark.read.format('csv').load("/data/train_data")
    color_labels_data = spark.read.format('csv').load("/data/color_labels")
    breed_labels_data = spark.read.format('csv').load("/data/breed_labels")
    
    pdf = (train_data
           .withColumn("AdoptionSpeed", F.col("AdoptionSpeed").cast(T.IntegerType()))
           .merge(breed_labels_data, left_on=["Breed1", "Type"], right_on=["BreedID", "Type"])
           .merge(color_labels_data, left_on="Color1", right_on="ColorID")
           ).toPandas()
    pdf["AdoptionSpeed"] = pdf["AdoptionSpeed"].astype("int")
    pdf["Fee"] = pdf["Fee"].astype("int")
    pdf["Age"] = pdf["Age"].astype("int")
    pdf["Name"] = pdf["Name"].fillna("Unknown")
    pdf.isna().sum()
    return pdf

if __name__ == '__main__':
    joined_data = join_pet_data()


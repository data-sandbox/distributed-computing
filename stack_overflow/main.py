import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import udf


def read_csv(questions_path, answers_path, tags_path):
    """
    Read csv files and return Spark dataframes.
    """
    df_questions = (spark.read.options(encoding="ISO-8859-1",
                                       header=True, multiLine=False,
                                       mode="DROPMALFORMED")
                    .csv(questions_path)
                    .limit(10_000)
                    )
    df_answers = (spark.read.options(encoding="ISO-8859-1",
                                     header=True, multiLine=False,
                                     mode="DROPMALFORMED")
                  .csv(answers_path)
                  .limit(10_000)
                  )
    df_tags = (spark.read.options(encoding="ISO-8859-1",
                                  header=True, multiLine=False,
                                  mode="DROPMALFORMED")
               .csv(tags_path)
               .limit(10_000)
               )
    return df_questions, df_answers, df_tags


def preprocess(df_questions, df_answers, df_tags):
    """
    Clean and filter dataframes
    """
    df_quest_filt = (df_questions
                     .withColumn('Id', df_questions['Id'].cast(IntegerType()))
                     .withColumn('OwnerUserId', df_questions['OwnerUserId'].cast(IntegerType()))
                     .withColumn('CreationDate', F.regexp_replace('CreationDate', 'T', ' '))
                     .withColumn('CreationDate', F.regexp_replace('CreationDate', 'Z', ''))
                     .withColumn('CreationTime', F.unix_timestamp('CreationDate', 'y-M-d HH:mm:ss').cast(LongType()))
                     .withColumn('ClosedDate', F.regexp_replace('ClosedDate', 'T', ' '))
                     .withColumn('ClosedDate', F.regexp_replace('ClosedDate', 'Z', ''))
                     .withColumn('ClosedTime', F.unix_timestamp('ClosedDate', 'y-M-d HH:mm:ss').cast(LongType()))
                     .withColumn('Score', df_questions['Score'].cast(IntegerType()))
                     ).na.drop(subset=["Id", "Body", "CreationTime"])

    df_answers_filt = (df_answers
                       .withColumn('Id', df_answers['Id'].cast(IntegerType()))
                       .withColumn('OwnerUserId', df_answers['OwnerUserId'].cast(IntegerType()))
                       .withColumn('ParentId', df_answers['ParentId'].cast(IntegerType()))
                       .withColumn('Score', df_answers['Score'].cast(IntegerType()))
                       .withColumn('CreationDate', F.regexp_replace('CreationDate', 'T', ' '))
                       .withColumn('CreationDate', F.regexp_replace('CreationDate', 'Z', ''))
                       .withColumn('CreationTime', F.unix_timestamp('CreationDate', 'y-M-d HH:mm:ss').cast(LongType()))
                       ).na.drop()

    df_tags_filt = (df_tags
                    .withColumn('Id', df_tags['Id'].cast(IntegerType()))
                    ).na.drop()

    return df_quest_filt, df_answers_filt, df_tags_filt


def parse_body(body):
    """
    Barebones udf function for debugging
    """
    return body


def join(questions_path, answers_path, tags_path):
    """
    Join dataframes, save example results, and attempt to run
    udf function. Udf function throws an error with very little
    documentation online, leading to issues in debugging. However,
    this same udf function workflow has previously worked on a
    different project on a different machine.
    """
    read = read_csv(questions_path, answers_path, tags_path)
    df_quest_filt, df_answers_filt, df_tags_filt = preprocess(
        read[0], read[1], read[2])

    # Groupby id and collect all tags into list
    df_id_tags = (df_tags_filt
                  .groupBy('Id')
                  .agg(F.collect_list('Tag')
                       .alias('Tags'))
                  .sort('Id', ascending=True)
                  )

    # Join dataframes
    df_body_tags = (df_quest_filt
                    .join(df_id_tags,
                          df_quest_filt['Id'] == df_id_tags['Id'],
                          'inner')
                    .select(df_quest_filt['Id'], 'Tags', 'Score', 'Body')
                    )

    # Write example results to csv
    (df_body_tags
        .withColumn('Tags', F.col('Tags').cast('string'))
        .write.option("header", "true")
        .mode("overwrite")
        .csv("output")
     )

    parse = udf(parse_body, StringType())

    df_test = (df_body_tags
               .withColumn('ParsedBody', parse('Body'))
               )

    # Write results to csv
    (df_test
        .withColumn('Tags', F.col('Tags').cast('string'))
        .write.option("header", "true")
        .mode("overwrite")
        .csv("output2")
     )

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--questions',
        help="Path to questions CSV.")
    parser.add_argument(
        '--answers',
        help="Path to answers CSV.")
    parser.add_argument(
        '--tags',
        help="Path to tags CSV.")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("StackOverflow").getOrCreate()

    join(args.questions, args.answers, args.tags)

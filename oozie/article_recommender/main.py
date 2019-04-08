import argparse
from datetime import datetime
import logging

from article_recommender import recommend

from pyspark.sql import SparkSession


def get_cmd_options():
    """Return command line options passed to the script.

    Returns:
        object: Arguments passed to the script.

    """
    parser = argparse.ArgumentParser(
        description='Generates article normalized scores.')
    parser.add_argument('end_date',
                        help='End date in the yyyymmdd format.',
                        type=lambda x: datetime.strptime(x, "%Y%m%d").date())
    parser.add_argument('--spark_app_name',
                        help='Name of the spark application, e.g.' +
                        ' "article-recommender"')
    parser.add_argument('--language_pairs_file',
                        help='Location of the list of language pairs to train.')
    parser.add_argument('--wikidata_dir',
                        help='Location of Wikidata dumps in HDFS.')
    parser.add_argument('--topsites_file',
                        help='Location of top Wikipedias by edit count.')
    parser.add_argument('--output_dir',
                        help='Output location in HDFS.')
    parser.add_argument('--tmp_dir',
                        help='Location for saving temporary files in HDFS.')
    return parser.parse_args()


def validate_cmd_options(options):
    """Validate command line options passed by the user.

    Returns:
        bool: In case of error, False is returned. Otherwise, True.

    """
    if options.end_date > datetime.today().date():
        logging.error('End date cannot be later than today: %s.' %
                      options.end_date)
        return False
    return True


def get_language_pairs_to_train(spark, file):
    return spark.read.load(file,
                           format="csv", sep="\t",
                           inferSchema="true", header="true").collect()


def main():
    options = get_cmd_options()
    if validate_cmd_options(options):
        spark = SparkSession\
            .builder\
            .appName(options.spark_app_name)\
            .enableHiveSupport()\
            .getOrCreate()
        for source_language, target_language in\
                get_language_pairs_to_train(spark, options.language_pairs_file):
            print('Started trainig %s-%s with ending date %s.' %
                  (source_language, target_language, options.end_date))
            normalized_scores = recommend.NormalizedScores(
                spark,
                source_language,
                target_language,
                options.end_date,
                options.wikidata_dir,
                options.topsites_file,
                options.output_dir,
                options.tmp_dir
            )
            normalized_scores.train()


if __name__ == '__main__':
    main()

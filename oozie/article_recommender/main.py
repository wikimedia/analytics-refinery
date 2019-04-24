import argparse
from datetime import datetime

from article_recommender import normalizedscores as ns, util


def get_cmd_options():
    """Return command line options passed to the script.

    Returns:
        object: Arguments passed to the script.

    """
    parser = argparse.ArgumentParser(
        description='Generates article normalized scores.')
    parser.add_argument('language_pairs',
                        help='Comma separated list of source and target '
                        'language codes, e.g. ru-uz,en-ko,ko-uz.',
                        type=lambda pairs: [
                            x.split('-') for x in pairs.split(',')
                        ])
    parser.add_argument('end_date',
                        help='End date in the yyyymmdd format.',
                        type=lambda x: datetime.strptime(x, "%Y%m%d").date())
    parser.add_argument('--spark_app_name',
                        help='Name of the spark application, e.g.' +
                        ' "article-recommender"')
    parser.add_argument('--topsites_file',
                        help='Location of top Wikipedias by edit count.')
    parser.add_argument('--dblist_file',
                        help='Location of list of Wikipedias.')
    parser.add_argument('--wikidata_dir',
                        help='Location of Wikidata dumps in HDFS.')
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
        print('End date cannot be later than today: %s.' %
              options.end_date)
        return False
    return True


def main():
    options = get_cmd_options()
    if validate_cmd_options(options):
        spark = util.get_spark_session(options.spark_app_name)
        normalized_scores = ns.NormalizedScores(
            spark,
            options.language_pairs,
            options.end_date,
            options.wikidata_dir,
            options.topsites_file,
            options.output_dir,
            options.tmp_dir
        )
        normalized_scores.train()


if __name__ == '__main__':
    main()

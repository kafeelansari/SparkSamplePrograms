"""This program is created to explain the functionality of a spark RDD."""
from pyspark import SparkConf,SparkContext


def mappartitionfunct(iterator):
    """prints and returns a tuple of a (word,1) for each partition passed as an argument."""
    for x in iterator:
        print(f"({x},1)")
        yield (x,1)
    print("="*20)


def printpartitionsdata(iterator):
    """prints the partition wise data with a separator '=' """
    for x in iterator:
        print(x)
    print("="*15)


def main():
    text = """StackOverflow’s annual developer survey concluded earlier this year, and they have graciously published the (anonymized) 2019 results for analysis. They’re a rich view into the experience of software developers around the world — what’s their favorite editor? how many years of experience? tabs or spaces? and crucially, salary. Software engineers’ salaries are good, and sometimes both eye-watering and news-worthy.
The tech industry is also painfully aware that it does not always live up to its purported meritocratic ideals. Pay isn’t a pure function of merit, and story after story tells us that factors like name-brand school, age, race, and gender have an effect on outcomes like salary.Can machine learning do more than predict things? Can it explain salaries and so highlight cases where these factors might be undesirably causing pay differences? This example will sketch how standard models can be augmented with SHAP (SHapley Additive exPlanations) to detect individual instances whose predictions may be concerning, and then dig deeper into the specific reasons the data leads to those predictions.
"""

    "A conf for standalone spark with 2 cores allocated to the application."
    conf = SparkConf().setMaster('local[2]')

    "sc is the SparkContext created using above spark configuration."
    sc = SparkContext(conf= conf)

    "creating a RDD using the collection 'text' variable data."
    text_rdd = sc.parallelize(text.split(" "),10)

    "Verifying the number of partitions created in above step."
    print(text_rdd.getNumPartitions())

    "printing the data for each partition. foreachParitition takes iterator as an argument."
    text_rdd.foreachPartition(lambda x : printpartitionsdata(x))

    "Mapping the data of each partition as (word,1). mapPartitions function takes iterator as an argument."
    mapped_text_rdd = text_rdd.mapPartitions(lambda x: mappartitionfunct(x))

    "Reducing the data by Key"
    reduced_text_rdd = mapped_text_rdd.reduceByKey(lambda x,y: x+y)

    "Printing the result of the records from previous command on console."
    print(reduced_text_rdd.collect())

    "Stopping the Spark Context"
    sc.stop()


if __name__ == '__main__':
    main()

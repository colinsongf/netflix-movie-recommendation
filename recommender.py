import time, sys, os
from pyspark.mllib.recommendation import ALS
from pyspark import SparkContext, SparkConf


# calculates average rating from movieid/rating tuple
# ret: movieid[nr/rating, avg/rating]
def get_counts_and_averages(movieid_rating):
    nr_ratings = len(movieid_rating[1])
    return movieid_rating[0], (nr_ratings, float(sum(x for x in movieid_rating[1]))/nr_ratings)

# This is the logic behind the ratings for the netflix dataset.
class NetflixRecommender:

    # count and average all ratings of ratings dataset
    def count_ratings_and_average(self):
        movieids_ratings = self.ratings.map(lambda x: (x[0], x[2])).groupByKey()
        average_movieid_rating = movieids_ratings.map(get_counts_and_averages)
        self.movie_rating_count = average_movieid_rating.map(lambda x: (x[0], x[1][0]))

    # train the model and write to self
    def train(self):
        # rank, seed, iter, lamda/regularization param
        # TODO: tweak these values!
        self.model = ALS.train(self.ratings, 10, seed=10L, iterations=10, lambda_=0.1)

    # predict top n movies for userid
    def predict_n_ratings(self, usr_id, n_count, threshold):
        # Get pairs of (userID, movieID) for user_id unrated movies
        unrated = self.movies.filter(lambda rating: not rating[1]==usr_id).map(lambda x: (usr_id, x[0]))

        # Get predicted ratings
        ratings = self.predict_full(unrated).filter(lambda r: r[2]>=threshold).takeOrdered(n_count, key=lambda x: -x[1])
        return ratings

    # creates full prediction for userid, movieid
    # ret: formatted movietitle, rating, number of ratings
    def predict_full(self, usr_movie):
        pr_model = self.model.predictAll(usr_movie).map(lambda x: (x.product, x.rating))
        # join movie + title + rating count, map
        return pr_model.join(self.movies_wtitle).join(self.movie_rating_count).map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))

    # ctor.
    def __init__(self, sc):
        dataset = os.path.join('.', 'dataset')
        self.sc = sc

        print("\n\n ** loading ratings... **\n\n")
        self.ratings = self.sc.textFile(os.path.join(dataset, 'nf_10000')).map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()

        print("\n\n ** loading movie data **\n\n")
        # movieID, year, title
        self.movies = self.sc.textFile(os.path.join(dataset, 'movie_titles')).map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),tokens[1],tokens[2])).cache()

        # I need id & title...
        self.movies_wtitle = self.movies.map(lambda x: (int(x[0]),x[2])).cache()

        print("\n\n ** loading complete ** \n\n")
        # Pre-calculate movies ratings counts
        self.count_ratings_and_average()

        print("\n\n ** train model ** \n\n")
        # Train the model
        self.train()

        print("\n\n ** result ** \n\n")

        user_id = 6
        number_recommendations = 15
        #the number of ratings a movie must have before it's concidered...
        rating_threshold = 25
        predicted = self.predict_n_ratings(user_id, number_recommendations, rating_threshold)
        print ("recommendet ratings for user id 6:\n%s" % "\n".join(map(str, predicted)))


if __name__ == "__main__":
    conf = SparkConf().setAppName("netflix-recommender")
    NetflixRecommender(SparkContext(conf=conf))
    print("\n\n ** DONE. **")

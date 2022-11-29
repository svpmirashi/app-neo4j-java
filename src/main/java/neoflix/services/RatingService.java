package neoflix.services;

import neoflix.AppUtils;
import neoflix.Params;
import neoflix.ValidationException;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.NoSuchRecordException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RatingService {
    private final Driver driver;
    private final List<Map<String, Object>> ratings;
    private final Map<String, Object> pulpfiction;

    /**
     * The constructor expects an instance of the Neo4j Driver, which will be
     * used to interact with Neo4j.
     */
    public RatingService(Driver driver) {
        this.driver = driver;
        this.ratings = AppUtils.loadFixtureList("ratings");
        this.pulpfiction = AppUtils.loadFixtureSingle("pulpfiction");
    }

    /**
     * Return a paginated list of reviews for a Movie.
     *
     * Results should be ordered by the `sort` parameter, and in the direction specified
     * in the `order` parameter.
     * Results should be limited to the number passed as `limit`.
     * The `skip` variable should be used to skip a certain number of rows.
     *
     * @param {string} id       The tmdbId for the movie
     * @param {string} sort  The field to order the results by
     * @param {string} order    The direction of the order (ASC/DESC)
     * @param {number} limit    The total number of records to return
     * @param {number} skip     The number of records to skip
     * @returns {Promise<Record<string, any>>}
     */
    // tag::forMovie[]
    public List<Map<String,Object>> forMovie(String id, Params params) {
        // TODO: Get ratings for a Movie

        return AppUtils.process(ratings,params);
    }
    // end::forMovie[]


    /**
     * Add a relationship between a User and Movie with a `rating` property.
     * The `rating` parameter should be converted to a Neo4j Integer.
     *
     * If the User or Movie cannot be found, a NotFoundError should be thrown
     *
     * @param {string} userId   the userId for the user
     * @param {string} movieId  The tmdbId for the Movie
     * @param {number} rating   An integer representing the rating from 1-5
     * @returns {Promise<Record<string, any>>}  A movie object with a rating property appended
     */
    // tag::add[]
    public Map<String,Object> add(String userId, String movieId, int rating) {
        // TODO: Convert the native integer into a Neo4j Integer
        // TODO: Save the rating in the database
        // TODO: Return movie details and a rating
        String query = "MATCH (u:User {userId: $userId}) MATCH (m:Movie {tmdbId: $movieId}) MERGE (u) -[r:RATED]-> (m) SET r.rating = $rating, r.timestamp = datetime() RETURN m { .*, rating: r.rating } AS movie";
        Map<String, Object> movieMap = null;
        try(Session session = driver.session()){
            movieMap = session.executeWrite(tx -> {
                var result = tx.run(query, Values.parameters("userId", userId, "movieId", movieId, "rating", rating));
                var res=result.single().get("movie").asMap();
                return res;
            });
        } catch(NoSuchRecordException e) {
            throw new ValidationException("Movie or user not found to add rating", Map.of("movie", movieId, "user", userId));
        }

        return movieMap;

//        var copy = new HashMap<>(pulpfiction);
//        copy.put("rating",rating);
//        return copy;
    }
    // end::add[]
}
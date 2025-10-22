# Big Data Lab Notes

This file contains all lab notes, code, and instructions for analyzing movie ratings datasets using Hadoop MapReduce.

---

## Question 1: Basic MapReduce - Calculate the average and total rating for movies


## Objective
- Read two input files:
  1. `ratings_*.txt` containing user ratings
     ```
     userId,movieId,rating,timestamp
     1,1193,5,978300760
     1,661,3,978302109
     ```
  2. `movies.txt` containing movie information
     ```
     movieId,title,genres
     1,Toy Story (1995),Animation|Children|Comedy
     2,Jumanji (1995),Adventure|Children|Fantasy
     ```
- Compute the **average rating** for each movie.
- Track the **highest-rated movie** with at least 5 ratings.

---

## MapReduce Flow

### 1. Mapper Phase

#### `RatingMapper`
- Input: `ratings_*.txt`
- Output Key: `movieId`
- Output Value: `Rate:<rating>`
- Logic:
  1. Skip empty or invalid lines.
  2. Parse `movieId` and `rating`.
  3. Emit `(movieId, "Rate:<rating>")`.

#### `MovieMapper`
- Input: `movies.txt`
- Output Key: `movieId`
- Output Value: `Movie:<title>`
- Logic:
  1. Skip empty or invalid lines.
  2. Parse `movieId` and `title`.
  3. Emit `(movieId, "Movie:<title>")`.

---

### 2. Reducer Phase

#### `RatingReducer`
- Input Key: `movieId`
- Input Values: List of `Rate:<rating>` and `Movie:<title>`
- Logic:
  1. Separate ratings and movie title.
  2. Compute the **average rating**.
  3. Track the **highest-rated movie** with at least 5 ratings.
- Cleanup:
  - Emit a **summary line** with the highest-rated movie.


# Question 2: Average Ratings by Genre

---

## Objective

- Join **ratings** and **movies** datasets by `movieId`.
- Compute **average rating per genre**, handling movies with multiple genres.
- Understand MapReduce job chaining (Job1 → Job2).

---

## Input Data

  1. `ratings_*.txt` containing user ratings
     ```
     userId,movieId,rating,timestamp
     1,1193,5,978300760
     1,661,3,978302109
     ```
  2. `movies.txt` containing movie information
     ```
     movieId,title,genres
     1,Toy Story (1995),Animation|Children|Comedy
     2,Jumanji (1995),Adventure|Children|Fantasy
     ```
---

## MapReduce Workflow

### Job 1: Join Movies and Ratings → `(genre, rating)` pairs

#### Mappers
- **RatingMapper**
  - Input: `ratings_*.txt`
  - Output Key: `movieId`
  - Output Value: `Rate:<rating>`
- **MovieMapper**
  - Input: `movies.txt`
  - Output Key: `movieId`
  - Output Value: `Genres:<genre1|genre2|...>`

#### Reducer: `JoinReducer`
- Input Key: `movieId`
- Input Values: list of `Rate:<rating>` and `Genres:<genre1|genre2|...>`
- Logic:
  1. Collect all ratings for this movie.
  2. Extract genres.
  3. Emit `(genre, rating)` for each genre.
- Example Output (Job1):

---

### Job 2: Aggregate Ratings by Genre

#### Mapper: `GenreMapper`
- Input: lines from Job1 output: `Genre<TAB>Rating`
- Output Key: `genre`
- Output Value: `DoubleWritable(rating)`
- Logic: Convert string rating to `DoubleWritable`.

#### Reducer: `GenreReducer`
- Input Key: `genre`
- Input Values: list of `DoubleWritable(rating)`
- Logic:
  1. Sum ratings for each genre.
  2. Count number of ratings.
  3. Compute average rating.
- Example Output (Job2):

---

## Key Concepts

- **MapReduce Join:** Combining two datasets using a common key (`movieId`).
- **Chained Jobs:** Output of Job1 is used as input for Job2.
- **Multiple Genres Handling:** Each movie rating contributes to all its genres.
- **Temporary Output:** Job1 writes temporary output files for Job2.
- **Robust Parsing:** Handles invalid or missing ratings.
- **Hadoop FileSystem Management:** Automatically deletes temporary and final output directories if they exist.

---

## Execution Flow

1. **Job1:** Join ratings and movies → generate `(genre, rating)` pairs.
2. **Job2:** Aggregate ratings by genre → compute averages and counts.
3. **Cleanup:** Temporary output folder is deleted after Job2 finishes.

# Question 3: Movie Ratings by Gender

---

## Objective

- Join **ratings** and **users** datasets to compute **average movie ratings by gender**.
- Use **MapReduce job chaining** (Job1 → Job2) and **distributed cache** for movie titles.
- Output: Each movie's average rating for **male** and **female** users.

---

## Input Data

  1. `ratings_*.txt` containing user ratings
     ```
     userId,movieId,rating,timestamp
     1,1193,5,978300760
     1,661,3,978302109
     ```
  2. `movies.txt` containing movie information
     ```
     movieId,title,genres
     1,Toy Story (1995),Animation|Children|Comedy
     2,Jumanji (1995),Adventure|Children|Fantasy
     ```
  1. `users.txt` containing user ratings
     ```
     userId,gender,other columns
     1,F,5,978300760
     1,M,3,978302109
     ```
---

## MapReduce Workflow

### Job 1: Join Ratings with Users

#### Mappers
- **RatingMapper**
  - Input: `ratings_*.txt`
  - Output Key: `userId`
  - Output Value: `Rate:<movieId>:<rating>`
- **UsersMapper**
  - Input: `users.txt`
  - Output Key: `userId`
  - Output Value: `Gender:<gender>`

#### Reducer: `JoinReducer`
- Input Key: `userId`
- Input Values: list of `Rate:<movieId>:<rating>` and `Gender:<gender>`
- Logic:
  1. Extract user's gender.
  2. Collect all ratings by this user.
  3. Emit `(movieId, <gender>:<rating>)` for each movie the user rated.
- Example Output (Job1):

---

### Job 2: Aggregate Ratings by Gender with Movie Titles

#### Mapper: `MovieGenderMapper`
- Input: lines from Job1 output: `movieId<TAB>gender:rating`
- Output Key: `movieId`
- Output Value: `gender:rating`

#### Reducer: `MovieGenderReducer`
- Input Key: `movieId`
- Input Values: list of `gender:rating`
- Logic:
  1. Use **distributed cache** to load `movies.txt` into memory for `movieId → title` mapping.
  2. Separate ratings by gender.
  3. Compute **average rating for male and female users**.
  4. Emit `movieTitle → Male: <avg>, Female: <avg>`.
- Example Output (Job2):

---

## Key Concepts

- **MapReduce Join:** Join ratings and users by `userId`.
- **Chained Jobs:** Job1 generates intermediate output used by Job2.
- **Distributed Cache:** Movie titles are loaded into memory in Job2 to map `movieId → title`.
- **Gender-based Aggregation:** Separate ratings into male/female buckets and compute averages.
- **Temporary Output Handling:** Job1 writes temporary output for Job2 and is deleted after completion.
- **Robust Parsing:** Handles missing or invalid ratings and genders.
- **Hadoop FileSystem Management:** Deletes temp or output folders if they exist.

---

## Execution Flow

1. **Job1:** Join ratings with users → produce `(movieId, gender:rating)` pairs.
2. **Job2:** Aggregate ratings by gender → include movie title from distributed cache.
3. **Cleanup:** Temporary output folder is deleted after Job2 finishes.

---

## Summary

- `bai3` demonstrates **MapReduce joins**, **job chaining**, and **distributed cache usage**.
- Output shows each movie's **average ratings separated by gender**.
- Can handle large datasets efficiently on Hadoop HDFS.

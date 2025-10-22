package com.lab1.bigdata;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai1 {

    // mapper for rating_*.txt
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Text movieIdKey = new Text();
        private Text ratingValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();

            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split(",");

            if (parts.length < 4) {
                return;
            }

            try {
                String movieId = parts[1].trim();
                double rating = Double.parseDouble(parts[2].trim());

                movieIdKey.set(movieId);
                ratingValue.set(String.format("Rate: %.2f", rating));

                context.write(movieIdKey, ratingValue);

            } catch (NumberFormatException e) {
                // ignoring all irrelevant records
            }
        }
    }

    // mapper for movies.txt
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        private Text movieIdKey = new Text();
        private Text movieNameValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();

            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split(",");

            if (parts.length < 3) {
                return;
            }

            try {
                String movieId = parts[0].trim();
                String movieName = parts[1].trim();

                movieIdKey.set(movieId);
                movieNameValue.set(String.format("Movie: %s", movieName));

                context.write(movieIdKey, movieNameValue);

            } catch (NumberFormatException e) {
                // ignoring all irrelevant records
            }
        }
    }

    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {
        // private Text outputKey = new Text();
        // private Text outputValue = new Text();

        
        private String maxMovie = "";
        private double maxRating = Double.MIN_VALUE;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;
            String movieName = "";

            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("Rate: ")) {
                    String rate = value.replace("Rate: ", "");
                    count += 1;
                    sum += Double.parseDouble(rate);
                } else {
                    movieName = value.replace("Movie: ", "");
                }
            }

            double avg = sum / count;

            // outputKey.set(String.format("%s: ", movieName));
            // outputValue.set(String.format("\tAverageRating: %.2f (Total ratings: %d)", avg, count));
            // context.write(outputKey, outputValue);

            // Only consider movies with at least 5 ratings for max tracking
            if (count >= 5 && avg > maxRating) {
                maxRating = avg;
                maxMovie = movieName;
            }            
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text(""), new Text("---------------------------------------------------------"));
            if (!maxMovie.isEmpty()) {
                context.write(
                    new Text("Summary:"),
                    new Text(String.format(
                        "%s is the highest rated movie with an average rating of %.2f among movies with at least 5 ratings.",
                        maxMovie, maxRating))
                );
            } else {
                context.write(
                    new Text("Summary:"),
                    new Text("No movie has at least 5 ratings.")
                );
            }
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Movie Rating Analysis");

        job.setJarByClass(bai1.class);
        job.setMapperClass(RatingMapper.class);
        job.setMapperClass(MovieMapper.class);
        job.setReducerClass(RatingReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(
                job,
                new Path(args[0]),
                TextInputFormat.class,
                RatingMapper.class);
        MultipleInputs.addInputPath(
                job,
                new Path(args[1]),
                TextInputFormat.class,
                MovieMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
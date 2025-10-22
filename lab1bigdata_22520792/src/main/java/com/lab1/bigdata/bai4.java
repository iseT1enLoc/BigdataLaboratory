package com.lab1.bigdata;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai4 {

    // Mapper for Ratings file
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("userId") || line.startsWith("UserID") || line.startsWith("user")) return;
            
            String[] parts = line.split(",");
            if (parts.length >= 3) {
                String userId = parts[0].trim();
                String movieId = parts[1].trim();
                String rating = parts[2].trim();
                context.write(new Text(userId), new Text("R:" + movieId + ":" + rating));
            }
        }
    }

    // Mapper for Users file
    public static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("userId") || line.startsWith("UserID") || line.startsWith("user")) return;
            
            String[] parts = line.split(",");
            if (parts.length >= 3) {
                String userId = parts[0].trim();
                String age = parts[2].trim();
                context.write(new Text(userId), new Text("U:" + age));
            }
        }
    }

    // Mapper for Movies file
    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("movieId") || line.startsWith("MovieID") || line.startsWith("movie")) return;
            
            String[] parts = line.split(",", 2);
            if (parts.length >= 2) {
                String movieId = parts[0].trim();
                String title = parts[1].trim();
                context.write(new Text(movieId), new Text("M:" + title));
            }
        }
    }

    // Reducer Job1: Join Users with Ratings
    public static class JoinUserRatingReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String age = null;
            List<String> ratings = new ArrayList<>();

            for (Text val : values) {
                String s = val.toString();
                if (s.startsWith("U:")) {
                    age = s.substring(2);
                } else if (s.startsWith("R:")) {
                    ratings.add(s.substring(2));
                }
            }

            if (age != null) {
                for (String rating : ratings) {
                    String[] parts = rating.split(":");
                    if (parts.length >= 2) {
                        String movieId = parts[0];
                        String ratingValue = parts[1];
                        context.write(new Text(movieId), new Text(ratingValue + ":" + age));
                    }
                }
            }
        }
    }

    // Mapper for Temp file
    public static class TempMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length >= 2) {
                context.write(new Text(parts[0].trim()), new Text(parts[1].trim()));
            }
        }
    }

    // Reducer Job2: Join with Movies and Aggregate
    public static class AggregateReducer extends Reducer<Text, Text, Text, Text> {
        
        private int getAgeGroup(int age) {
            if (age < 18) return 0;
            if (age >= 18 && age < 35) return 1;
            if (age >= 35 && age < 50) return 2;
            return 3;
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String movieTitle = null;
            double[] sumRatings = new double[4];
            int[] countRatings = new int[4];

            for (Text val : values) {
                String s = val.toString();
                if (s.startsWith("M:")) {
                    movieTitle = s.substring(2);
                } else {
                    String[] parts = s.split(":");
                    if (parts.length >= 2) {
                        try {
                            double rating = Double.parseDouble(parts[0]);
                            int age = Integer.parseInt(parts[1]);
                            int group = getAgeGroup(age);
                            sumRatings[group] += rating;
                            countRatings[group]++;
                        } catch (NumberFormatException e) {
                            // Skip invalid data
                        }
                    }
                }
            }

            if (movieTitle != null) {
                String[] groupNames = {"0-18", "18-35", "35-50", "50+"};
                StringBuilder result = new StringBuilder();
                
                for (int i = 0; i < 4; i++) {
                    if (i > 0) result.append(", ");
                    double avg = countRatings[i] > 0 ? sumRatings[i] / countRatings[i] : 0.0;
                    result.append(groupNames[i]).append(": ").append(String.format("%.2f", avg));
                }
                
                context.write(new Text(movieTitle), new Text(result.toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: bai4 <ratings_input> <movies_input> <users_input> <output>");
            System.exit(2);
        }

        String ratingsPath = args[0];
        String moviesPath = args[1];
        String usersPath = args[2];
        String outputPath = args[3];
        String tempPath = "temp_bai4";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Clean up existing directories
        if (fs.exists(new Path(tempPath))) fs.delete(new Path(tempPath), true);
        if (fs.exists(new Path(outputPath))) fs.delete(new Path(outputPath), true);

        // Job 1: Join Ratings with Users
        Job job1 = Job.getInstance(conf, "Join Ratings with Users");
        job1.setJarByClass(bai4.class);

        MultipleInputs.addInputPath(job1, new Path(ratingsPath), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job1, new Path(usersPath), TextInputFormat.class, UserMapper.class);

        job1.setReducerClass(JoinUserRatingReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job1, new Path(tempPath));

        if (!job1.waitForCompletion(true)) {
            System.err.println("Job 1 failed!");
            System.exit(1);
        }

        // Job 2: Join with Movies and Aggregate by Age Group
        Job job2 = Job.getInstance(conf, "Aggregate Ratings by Age Group");
        job2.setJarByClass(bai4.class);

        MultipleInputs.addInputPath(job2, new Path(tempPath), TextInputFormat.class, TempMapper.class);
        MultipleInputs.addInputPath(job2, new Path(moviesPath), TextInputFormat.class, MovieMapper.class);

        job2.setReducerClass(AggregateReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job2, new Path(outputPath));

        if (!job2.waitForCompletion(true)) {
            System.err.println("Job 2 failed!");
            System.exit(1);
        }

        // Clean up temp directory
        fs.delete(new Path(tempPath), true);
        
        System.out.println("Job completed successfully!");
        System.exit(0);
    }
}
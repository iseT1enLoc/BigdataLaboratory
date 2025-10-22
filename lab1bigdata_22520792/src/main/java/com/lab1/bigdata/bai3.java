package com.lab1.bigdata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai3 {

    /* ------------------ Mappers for Job1 ------------------ */
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().trim().split(",");
            if (parts.length < 3) return;

            String userId = parts[0].trim();
            String movieId = parts[1].trim();
            String rating = parts[2].trim();

            outKey.set(userId);
            outVal.set("Rate:" + movieId + ":" + rating);
            context.write(outKey, outVal);
        }
    }

    public static class UsersMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().trim().split(",");
            if (parts.length < 2) return;

            String userId = parts[0].trim();
            String gender = parts[1].trim();

            outKey.set(userId);
            outVal.set("Gender:" + gender);
            context.write(outKey, outVal);
        }
    }

    /* ------------------ Reducer for Job1 ------------------ */
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String gender = null;
            List<String[]> ratings = new ArrayList<>(); // each: [movieId, rating]

            for (Text t : values) {
                String s = t.toString();
                if (s.startsWith("Gender:")) {
                    gender = s.substring("Gender:".length());
                } else if (s.startsWith("Rate:")) {
                    String[] parts = s.substring("Rate:".length()).split(":");
                    if (parts.length == 2) ratings.add(parts);
                }
            }

            if (gender == null || ratings.isEmpty()) return;

            for (String[] r : ratings) {
                String movieId = r[0];
                String rating = r[1];
                outKey.set(movieId);
                outVal.set(gender + ":" + rating);
                context.write(outKey, outVal);
            }
        }
    }

    /* ------------------ Mapper for Job2 ------------------ */
    public static class MovieGenderMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\\t");
            if (parts.length < 2) return;

            String movieId = parts[0];
            String genderRating = parts[1];

            outKey.set(movieId);
            outVal.set(genderRating);
            context.write(outKey, outVal);
        }
    }

    /* ------------------ Reducer for Job2 with MovieTitle ------------------ */
    public static class MovieGenderReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outVal = new Text();
        private HashMap<String, String> movieTitles = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            // Read cached movie files
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI cacheFile : cacheFiles) {
                    Path path = new Path(cacheFile.getPath());
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            String[] parts = line.split(",", 3);
                            if (parts.length >= 2) {
                                String movieId = parts[0].trim();
                                String movieTitle = parts[1].trim();
                                movieTitles.put(movieId, movieTitle);
                            }
                        }
                    }
                }
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double maleSum = 0, femaleSum = 0;
            int maleCount = 0, femaleCount = 0;

            for (Text t : values) {
                String[] parts = t.toString().split(":");
                if (parts.length != 2) continue;
                String gender = parts[0];
                double rating = Double.parseDouble(parts[1]);
                if (gender.equalsIgnoreCase("M")) {
                    maleSum += rating;
                    maleCount++;
                } else if (gender.equalsIgnoreCase("F")) {
                    femaleSum += rating;
                    femaleCount++;
                }
            }

            String title = movieTitles.getOrDefault(key.toString(), key.toString());
            String result = String.format("Male: %.2f, Female: %.2f",
                    maleCount > 0 ? maleSum / maleCount : 0,
                    femaleCount > 0 ? femaleSum / femaleCount : 0);

            outVal.set(result);
            context.write(new Text(title), outVal);
        }
    }

    /* ------------------ Main ------------------ */
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: bai3 <ratings_folder> <users_folder> <movies_folder> <output>");
            System.exit(2);
        }

        String ratingsInput = args[0];
        String usersInput = args[1];
        String moviesInput = args[2];
        String finalOutput = args[3];

        String tempOutput = "temp_user_gender";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path tempOutputPath = new Path(tempOutput);
        if (fs.exists(tempOutputPath)) fs.delete(tempOutputPath, true);

        Path finalOutputPath = new Path(finalOutput);
        if (fs.exists(finalOutputPath)) fs.delete(finalOutputPath, true);

        // ---------- Job1 ----------
        Job job1 = Job.getInstance(conf, "Join Ratings with Users by Gender");
        job1.setJarByClass(bai3.class);

        MultipleInputs.addInputPath(job1, new Path(ratingsInput), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job1, new Path(usersInput), TextInputFormat.class, UsersMapper.class);

        job1.setReducerClass(JoinReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job1, tempOutputPath);

        if (!job1.waitForCompletion(true)) {
            System.err.println("Job1 failed.");
            System.exit(1);
        }

        // ---------- Job2 ----------
        Job job2 = Job.getInstance(conf, "Aggregate Ratings by Gender");
        job2.setJarByClass(bai3.class);

        FileInputFormat.addInputPath(job2, tempOutputPath);
        FileOutputFormat.setOutputPath(job2, finalOutputPath);

        job2.setMapperClass(MovieGenderMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setReducerClass(MovieGenderReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);


        FileSystem movieFS = FileSystem.get(new URI(moviesInput), conf);
        for (org.apache.hadoop.fs.FileStatus fileStatus : movieFS.listStatus(new Path(moviesInput))) {
            if (!fileStatus.isFile()) continue;
            job2.addCacheFile(fileStatus.getPath().toUri());
        }


        if (!job2.waitForCompletion(true)) {
            System.err.println("Job2 failed.");
            System.exit(1);
        }

        // delete temp folder
        if (fs.exists(tempOutputPath)) fs.delete(tempOutputPath, true);

        System.exit(0);
    }
}

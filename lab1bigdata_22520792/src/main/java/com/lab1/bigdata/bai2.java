package com.lab1.bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai2 {

    /* ------------------ Mappers for job1 ------------------ */
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Expecting: userId, movieId, rating, timestamp
            String[] parts = line.split(",");
            if (parts.length < 3) return;

            String movieId = parts[1].trim();
            String ratingStr = parts[2].trim();
            try {
                double rating = Double.parseDouble(ratingStr);
                outKey.set(movieId);
                outVal.set("Rate:" + rating);
                context.write(outKey, outVal);
            } catch (NumberFormatException e) {
                // skip invalid rating
            }
        }
    }

    // Mapper for movies file: output key = movieId, value = "Genres:<genre1|genre2|...>"
    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",", 3); // split into at most 3 parts
            if (parts.length < 3) return;

            String movieId = parts[0].trim();
            String genres = parts[2].trim(); // e.g. "Animation|Children|Comedy"
            if (genres.isEmpty()) return;

            outKey.set(movieId);
            outVal.set("Genres:" + genres);
            context.write(outKey, outVal);
        }
    }

    /* ------------------ Reducer (Job1 join) ------------------ */

    // For each movieId, emit (genre, rating) for every rating found for that movie.
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text(); // rating as string

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String genresStr = null;
            List<Double> ratings = new ArrayList<>();

            for (Text t : values) {
                String s = t.toString();
                if (s.startsWith("Genres:")) {
                    genresStr = s.substring("Genres:".length());
                } else if (s.startsWith("Rate:")) {
                    String r = s.substring("Rate:".length());
                    try {
                        ratings.add(Double.parseDouble(r));
                    } catch (NumberFormatException e) {
                        // skip
                    }
                }
            }

            // If no genres or no ratings => nothing to emit
            if (genresStr == null || ratings.isEmpty()) return;

            String[] genres = genresStr.split("\\|");
            for (String g : genres) {
                String genre = g.trim();
                if (genre.isEmpty()) continue;
                outKey.set(genre);
                // emit one line per rating for this genre
                for (Double rating : ratings) {
                    outVal.set(String.valueOf(rating));
                    context.write(outKey, outVal);
                }
            }
        }
    }

    /* ------------------ Mapper & Reducer for Job2 ------------------ */

    // Job2 mapper: input is text lines like "Genre\tRating"
    public static class GenreMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final Text outKey = new Text();
        private final DoubleWritable outVal = new DoubleWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            // split on tab or multiple spaces
            String[] parts = line.split("\\t+");
            if (parts.length < 2) {
                // sometimes Hadoop's TextOutputFormat uses "\t", but if not try a whitespace split
                parts = line.split("\\s+");
                if (parts.length < 2) return;
            }
            String genre = parts[0].trim();
            String ratingStr = parts[1].trim();
            try {
                double rating = Double.parseDouble(ratingStr);
                outKey.set(genre);
                outVal.set(rating);
                context.write(outKey, outVal);
            } catch (NumberFormatException e) {
                // skip
            }
        }
    }

    // Job2 reducer: compute average rating per genre
    public static class GenreReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        private final Text outVal = new Text();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            long count = 0;
            for (DoubleWritable d : values) {
                sum += d.get();
                count++;
            }
            if (count == 0) return;
            double avg = sum / count;
            outVal.set(String.format("Avg: %.2f (Count: %d)", avg, count));
            context.write(key, outVal);
        }
    }

    /* ------------------ main: run job1 then job2 ------------------ */

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: bai2 <ratings_input_path> <movies_input_path> <final_output_path>");
            System.exit(2);
        }

        String ratingsInput = args[0];
        String moviesInput = args[1];
        String finalOutput = args[2];

        // temp path for job1 output
        String tempOutput = "temp_genre_ratings";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // delete temp or final output if exists (helpful during testing)
        Path tempOutputPath = new Path(tempOutput);
        if (fs.exists(tempOutputPath)) {
            fs.delete(tempOutputPath, true);
        }
        Path finalOutputPath = new Path(finalOutput);
        if (fs.exists(finalOutputPath)) {
            fs.delete(finalOutputPath, true);
        }

        // ---------- Job 1 ----------
        Job job1 = Job.getInstance(conf, "Join Movies and Ratings -> genre,rating");
        job1.setJarByClass(bai2.class);

        // Multiple inputs: ratings and movies
        MultipleInputs.addInputPath(job1, new Path(ratingsInput), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job1, new Path(moviesInput), TextInputFormat.class, MovieMapper.class);

        job1.setReducerClass(JoinReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        // Output of job1: TextOutputFormat default -> lines "genre\t<rating>"
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job1, tempOutputPath);

        boolean ok = job1.waitForCompletion(true);
        if (!ok) {
            System.err.println("Job1 failed.");
            System.exit(1);
        }

        // ---------- Job 2 ----------
        Job job2 = Job.getInstance(conf, "Aggregate ratings by genre");
        job2.setJarByClass(bai2.class);

        // Input is the tempOutput text files
        FileInputFormat.addInputPath(job2, tempOutputPath);
        FileOutputFormat.setOutputPath(job2, finalOutputPath);

        job2.setMapperClass(GenreMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(DoubleWritable.class);

        job2.setReducerClass(GenreReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        ok = job2.waitForCompletion(true);
        if (!ok) {
            System.err.println("Job2 failed.");
            System.exit(1);
        }

        // optionally delete temp
        if (fs.exists(tempOutputPath)) {
            fs.delete(tempOutputPath, true);
        }

        System.exit(0);
    }
}

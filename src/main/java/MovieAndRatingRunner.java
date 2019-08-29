import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieAndRatingRunner {

    public static void main(String[] args) {

        try {
            Job job = Job.getInstance();
            job.setJobName("Movie Rating Job");

            job.setJarByClass(MovieAndRatingRunner.class);

            MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingsMapper.class);

            job.setMapOutputValueClass(Text.class);
            job.setMapOutputKeyClass(IntWritable.class);

            job.setOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);

            job.setReducerClass(MovieAndRatingReducer.class);
//            job.setCombinerClass(MovieAndRatingReducer.class);

            FileOutputFormat.setOutputPath(job, new Path(args[2]));

            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

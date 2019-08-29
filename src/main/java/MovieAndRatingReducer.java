import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MovieAndRatingReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String movieName = "";
        int movieCount = 0;

        for (Text value : values) {

            String[] vals = value.toString().split("\t");

            if (vals[0].equals("movie")) {

                movieName = vals[1];
            } else {
                movieCount++;
            }
        }

        if (movieCount > 0) {
            context.write(
                    new Text(movieName), new IntWritable(movieCount)
            );
        }
    }
}

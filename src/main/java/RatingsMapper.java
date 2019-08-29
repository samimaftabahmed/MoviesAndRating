import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class RatingsMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer stringTokenizer = new StringTokenizer(value.toString(), "");

        // for getting the value of what is being logged in this method use file operations to save log
//        StringBuilder stringBuilder = new StringBuilder();

        boolean stat = true;

        while (stringTokenizer.hasMoreTokens()) {

            if (stat) {
                stat = false;
                continue;
            }

            String token = stringTokenizer.nextToken();

            String[] ratingData = token.split(",");

            context.write(
                    new IntWritable(Integer.parseInt(ratingData[1])),
                    new Text("rating\t" + ratingData[2])
            );

//            boolean stat = ratingData[0].equals("userId") || ratingData[1].equals("movieId") ||
//                    ratingData[2].equals("rating") || ratingData[3].equals("timestamp");


        }
    }
}

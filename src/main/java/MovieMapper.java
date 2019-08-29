import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MovieMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer tokenizer = new StringTokenizer(value.toString(), "");

        while (tokenizer.hasMoreElements()) {

            String[] movieData = tokenizer.nextToken().split(",");

            context.write(
                    new IntWritable(Integer.parseInt(movieData[0])),
                    new Text("movie\t" + movieData[1])
            );
        }
    }
}

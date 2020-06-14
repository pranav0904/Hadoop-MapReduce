import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import sun.awt.SunHints;

import java.io.IOException;

public class MapperClass extends Mapper <LongWritable, Text, Text, IntWritable>{

    Text word = new Text();
    IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String [] Tokens = line.split(" ");

        for (String val: Tokens) {
            word.set(val);
            context.write(word,one);
        }

    }
}

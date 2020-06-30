import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<Text,Text,Text,Text> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        Text firstName = new Text(value.toString().split("\t")[0]);
        context.write(key, firstName);
    }
}

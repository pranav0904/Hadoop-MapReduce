import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DriverClass
{
    public static void main( String[] args ) throws Exception
    {

        if (args.length != 2) {
            System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
            System.exit(-1);
        }
        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);

        // Create configuration
        Configuration conf = new Configuration(true);

        // Create job
        Job job = new Job(conf, "Yearly Average ");
        job.setJarByClass(DriverClass.class);

        job.setPartitionerClass(NaturalKeyPartitioner.class);

        // Setup MapReduce
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setNumReduceTasks(1);


        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class); //keyValueTextInputFormat

        // Output
        FileOutputFormat.setOutputPath(job, outputDir);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputDir))
            fs.delete(outputDir, true);

        // Execute job
        job.waitForCompletion(true);
    }
}
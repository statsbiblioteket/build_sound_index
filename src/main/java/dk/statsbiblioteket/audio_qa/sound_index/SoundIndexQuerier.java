package dk.statsbiblioteket.audio_qa.sound_index;

import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SoundIndexQuerier extends Configured implements Tool {

    protected static final String QUERY_FILE_NAME = "ismir.queryfile";

    static public class SoundIndexQuerierMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void map(LongWritable lineNo, Text databaseFile, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            FileSystem fs = FileSystem.get(URI.create("file:///"),context.getConfiguration());
            String queryFile = conf.get(SoundIndexQuerier.QUERY_FILE_NAME);

            try {

                Text res = executeQuery(databaseFile.toString(), fs, queryFile);

                if (res.toString().length() > 0) {
                    context.write(new LongWritable(lineNo.get() % 200), res);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

        private Text executeQuery(String databaseFile, FileSystem fs, String queryFile) throws IOException {

            String[] queryCommand = new String[]{
                    "/home/scape/bin/ismir_query",
                    "-d",
                    databaseFile,
                    "-q",
                    queryFile,
            };

            final Text output = new Text();

            int exitCode = CLIToolRunner.runCLItool(queryCommand, null, fs, null, output);
            if (exitCode != 0) {
                throw new IOException(output.toString());
            }

            return output;
        }

    }

    // static public class SoundIndexQuerierReducer extends Reducer<LongWritable, Text, LongWritable, Text> {


    // 	@Override
    // 	protected void reduce(LongWritable r, Iterable<Text> databaseFiles, Context context) throws IOException, InterruptedException {
    // 	    // execute queries and concatenate output.
    // 	    Text ret = new Text();
    // 	    FileSystem fs = FileSystem.get(URI.create("file:///"),context.getConfiguration());
    // 	    for (Text dbFile : databaseFiles) {
    // 		Text output = executeQuery(dbFile.toString(), fs, context.getConfiguration().get("wav-file"));
    // 		ret = new Text(output.toString() + output.toString());
    // 	    }

    // 	    // Write gathered output from queries.
    // 	    context.write(new LongWritable(1), ret);
    // 	}

    // }

    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = getConf();

        conf.set(NLineInputFormat.LINES_PER_MAP, "1");
        conf.set("mapred.line.input.format.linespermap", "1");

        int n = args.length;

        if (n != 3) {
            return 1;
        }

        String queryFile = args[0];
        conf.set(SoundIndexQuerier.QUERY_FILE_NAME, queryFile);

        // remember to add file:// on commandline
        Job job = Job.getInstance(conf);
        job.setJarByClass(SoundIndexQuerier.class);

        NLineInputFormat.addInputPath(job, new Path(args[1]));
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setNumReduceTasks(1);

        job.setMapperClass(SoundIndexQuerierMapper.class);
        //job.setReducerClass(SoundIndexQuerierReducer.class);

        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //job.setOutputKeyClass(LongWritable.class);
        //job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : -1;

    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SoundIndexQuerier(), args));
    }

}

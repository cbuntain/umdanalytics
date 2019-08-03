/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umd.analytics.mr.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.text.SimpleDateFormat;

/**
 * Simple word count demo.
 */
public class IpTimeline extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(IpTimeline.class);

  // Mapper: emits (token, 1) for every word occurrence.
  public static final class MyMapper extends Mapper<Object, Text, LongWritable, IntWritable> {
    private static final SimpleDateFormat dateParser = new SimpleDateFormat("dd/MMM/yyyy HH:mm:ss");
    private Map<Long, Integer> counts;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      counts = new HashMap<>();
    }

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

      Reader in = new StringReader(value.toString());
      CSVParser parser = new CSVParser(in, CSVFormat.EXCEL);
      List<CSVRecord> list = parser.getRecords();

      for ( CSVRecord record : list ) {

        try {
          Date d = dateParser.parse(record.get(0));
          d.setSeconds(0);

          long t = d.getTime();
          if (counts.containsKey(t)) {
            counts.put(t, counts.get(t)+1);
          } else {
            counts.put(t, 1);
          }
        } catch (Exception e) {
          // Pass
        }

      }
    }

    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable cnt = new IntWritable();
      LongWritable time = new LongWritable();

      for (Map.Entry<Long, Integer> entry : counts.entrySet()) {
        time.set(entry.getKey());
        cnt.set(entry.getValue());
        context.write(time, cnt);
      }
    }
  }

  // Reducer: sums up all the counts.
  public static final class MyReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
    // Reuse objects.
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private IpTimeline() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + IpTimeline.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(IpTimeline.class.getSimpleName());
    job.setJarByClass(IpTimeline.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    
    // job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new IpTimeline(), args);
  }
}

package io.bfscan;

import io.bfscan.data.WarcRecord;
import io.bfscan.mapreduce.ClueWeb09InputFormat;
import io.bfscan.mapreduce.ClueWeb12InputFormat;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class CountWarcRecords extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(CountWarcRecords.class);

  private static enum Records { TOTAL, PAGES };

  private static class MyMapper
      extends Mapper<LongWritable, WarcRecord, NullWritable, NullWritable> {
    @Override
    public void map(LongWritable key, WarcRecord doc, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Records.TOTAL).increment(1);

      String docid = doc.getDocid();
      if (docid != null) {
        context.getCounter(Records.PAGES).increment(1);
      }
    }
  }

  public CountWarcRecords() {}

  public static final String INPUT_OPTION = "input";
  public static final String CLUEWEB09 = "clueweb09";
  public static final String CLUEWEB12 = "clueweb12";

  /**
   * Runs this tool.
   */
  @SuppressWarnings("static-access")
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT_OPTION));

    options.addOption(CLUEWEB09, false, "clueweb09");
    options.addOption(CLUEWEB12, false, "clueweb12");

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    if (!cmdline.hasOption(CLUEWEB09) && !cmdline.hasOption(CLUEWEB12)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.err.println("Must specify -clueweb09 or -clueweb12!");
      return -1;
    }

    String input = cmdline.getOptionValue(INPUT_OPTION);

    LOG.info("Tool name: " + CountWarcRecords.class.getSimpleName());
    LOG.info(" - input: " + input);

    Job job = Job.getInstance(getConf());
    job.setJobName(CountWarcRecords.class.getSimpleName() + ":" + input);
    job.setJarByClass(CountWarcRecords.class);
    job.setNumReduceTasks(0);

    FileInputFormat.addInputPaths(job, input);

    if (cmdline.hasOption(CLUEWEB09)) {
      LOG.info(" - collection: clueweb09");
      job.setInputFormatClass(ClueWeb09InputFormat.class);
    } else {
      LOG.info(" - collection: clueweb12");
      job.setInputFormatClass(ClueWeb12InputFormat.class);
    }

    job.setOutputFormatClass(NullOutputFormat.class);
    job.setMapperClass(MyMapper.class);

    job.waitForCompletion(true);

    Counters counters = job.getCounters();
    int numDocs = (int) counters.findCounter(Records.PAGES).getValue();
    LOG.info("Read " + numDocs + " docs.");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Running " + CountWarcRecords.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    ToolRunner.run(new CountWarcRecords(), args);
  }
}

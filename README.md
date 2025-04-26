// MaxLoginTimeMapper.java
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxLoginTimeMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text user = new Text();
    private SimpleDateFormat format = new SimpleDateFormat("HH:mm");
    private String currentUser = null;
    private Date loginTime = null;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\s+");
        if (parts.length != 3) {
            return;
        }

        String userId = parts[0];
        String action = parts[1];
        String timeStr = parts[2];

        try {
            Date time = format.parse(timeStr);

            if (action.equals("login")) {
                currentUser = userId;
                loginTime = time;
            } else if (action.equals("logout") && currentUser != null && currentUser.equals(userId)) {
                long duration = (time.getTime() - loginTime.getTime()) / (60 * 1000); // in minutes
                user.set(userId);
                context.write(user, new IntWritable((int) duration));
                currentUser = null; // reset after logout
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


// MaxLoginTimeReducer.java
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxLoginTimeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Map<String, Integer> userTimes = new HashMap<>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int total = 0;
        for (IntWritable val : values) {
            total += val.get();
        }
        userTimes.put(key.toString(), total);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        int maxTime = 0;
        for (Integer time : userTimes.values()) {
            if (time > maxTime) {
                maxTime = time;
            }
        }

        for (Map.Entry<String, Integer> entry : userTimes.entrySet()) {
            if (entry.getValue() == maxTime) {
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
            }
        }
    }
}


// MaxLoginTimeDriver.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxLoginTimeDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max Login Time");

        job.setJarByClass(MaxLoginTimeDriver.class);
        job.setMapperClass(MaxLoginTimeMapper.class);
        job.setReducerClass(MaxLoginTimeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // input path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

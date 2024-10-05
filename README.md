import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesAnalysis {

    public static class YearMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text year = new Text();
        private IntWritable value = new IntWritable();

        @Override
        protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
            String[] fields = line.toString().split("\\s+");
            if (fields.length > 1) {
                year.set(fields[0]); // Lấy năm từ trường đầu tiên
                try {
                    value.set(Integer.parseInt(fields[fields.length - 1])); // Lấy giá trị từ trường cuối cùng
                    context.write(year, value);
                } catch (NumberFormatException e) {
                    // Xử lý lỗi nếu không thể chuyển đổi giá trị
                    System.err.println("Error parsing value: " + fields[fields.length - 1]);
                }
            }
        }
    }

    public static class TransactionReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable totalValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if (sum >= 30) { // Chỉ xuất ra nếu tổng lớn hơn hoặc bằng 30
                totalValue.set(sum);
                context.write(key, totalValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SalesAnalysis <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(SalesAnalysis.class);
        job.setJobName("Sales Analysis");

        job.setMapperClass(YearMapper.class);
        job.setReducerClass(TransactionReducer.class);
        job.setCombinerClass(TransactionReducer.class); // Sử dụng reducer làm combiner

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

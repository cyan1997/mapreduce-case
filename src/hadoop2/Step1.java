package hadoop2;
 
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
 
public class Step1 {
	
	
	/***
	 * input:  /input/useraction.txt
	 * userID,itemID,score
	 * 	A,1,1
		C,3,5
		B,2,3
		B,5,3
		B,6,5
		A,2,10
		C,3,10
		C,4,5
		C,1,5
		A,1,1
		A,6,5
		A,4,3		
	 * output:
	 * (itemID,userID_score)
	 *	("1","A_1")
		("3","C_5")
		("2","B_3")
		("5","B_3")
		("6","B_5")
		("2","A_10")
		("3","C_10")
		("4","C_5")
		("1","C_5")
		("1","A_1")
		("6","A_5")
		("4","A_3")
	 * 
	 * 即map操作是将（用户ID,物品ID,行为分值)转为（物品ID，用户ID,行为分值)
	 * @author chenjie
	 *
	 */
	public static class Mapper1 extends Mapper<LongWritable,Text,Text,Text>
	{
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//System.out.println("map,key=" + key + ",value=" + value.toString());
			String values[] = value.toString().split(",");
			String userID = values[0];
			String itemID = values[1];
			String score  = values[2];
			outKey.set(itemID);
			outValue.set(userID + "_" + score);
			context.write(outKey, outValue);
			System.out.println("(\"" + itemID + "\",\"" + userID + "_" + score + "\")");
		}
	}
	
	/***
	 * input:
	 * itemID [userID_socre...]
	 * 	("1",["A_1","C_5","A_1"])
		("2",["A_10","B_3"])
		("3",["C_10","C_5"])
		("4",["A_3","C_5"])
		("5",["B_3"])
		("6",["A_5","B_5"])
		
		output:
		itemID [userID_sumScore...]
		1	A_2,C_5
		2	A_10,B_3
		3	C_15
		4	A_3,C_5
		5	B_3
		6	A_5,B_5
		
		即reduce操作是将（物品ID，用户ID,行为分值)中对于物品ID和用户ID相同的行为分值进行累加
		如	("1",["A_1","C_5","A_1"])中对于1号物品，A号用户，1+1=2
		那么将1号物品，A号用户，总分2分存在map中，（1,“A_2”）
		同理将1号物品，C号用户，总分5分存在map中，（1,“C_5”）
		...
		然后将1号物品的所有信息输出  key:1	value:A_2,C_5
		同理将2号物品的所有信息输出  key:2  value:A_10,B_3
		...
	 * @author chenjie
	 *
	 */
	public static class Reducer1 extends Reducer<Text,Text,Text,Text>
	{
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String itemID = key.toString();
			StringBuilder log = new StringBuilder();
			log.append("(\"" + itemID + "\",[");
			Map<String,Integer> map = new HashMap<String,Integer>();
			for(Text value : values)
			{
				log.append("\"" + value + "\",");
				String userID = value.toString().split("_")[0];
				String score = value.toString().split("_")[1];
				if(map.get(userID) == null)
				{
					map.put(userID, Integer.valueOf(score));
				}
				else
				{
					Integer preScore = map.get(userID);
					map.put(userID, preScore + Integer.valueOf(score));
				}
			}
			if(log.toString().endsWith(","))
				log.deleteCharAt(log.length()-1);
			log.append("])");
			System.out.println(log);
			StringBuilder sb = new StringBuilder();
			for(Map.Entry<String, Integer> entry : map.entrySet())
			{
				String userID = entry.getKey();
				String score = String.valueOf(entry.getValue());
				sb.append(userID + "_" + score + ",");
			}
			String line = null;
			if(sb.toString().endsWith(","))
			{
				line = sb.substring(0, sb.length()-1);
			}
			outKey.set(itemID);
			outValue.set(line);
			context.write(outKey, outValue);
		}
		
	}
	
	private static final String INPATH = "/input/useraction.txt";//输入文件路径
	private static final String OUTPATH = "/output/tuijian1";//输出文件路径
	private static final String HDFS = "hdfs://192.168.58.129:9000";//HDFS路径
	
	public int run() throws IOException, ClassNotFoundException, InterruptedException {
		 Configuration conf = new Configuration();
		 conf.set("fs.defaultFS",HDFS);
		    //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		    //String[] otherArgs = {"hdfs://pc1:9000/input/chenjie.txt","hdfs://pc1:9000/output/out4"};
		    String[] otherArgs = {INPATH,OUTPATH};
		    //这里需要配置参数即输入和输出的HDFS的文件路径
		    if (otherArgs.length != 2) {
		      System.err.println("Usage: wordcount <in> <out>");
		      System.exit(2);
		    }
		    //conf.set("fs.defaultFS",HDFS);
		   // JobConf conf1 = new JobConf(WordCount.class);
		    @SuppressWarnings("deprecation")
			Job job = new Job(conf, "step1");//Job(Configuration conf, String jobName) 设置job名称和
		    job.setJarByClass(Step1.class);
		    job.setMapperClass(Mapper1.class); //为job设置Mapper类 
		    //job.setCombinerClass(IntSumReducer.class); //为job设置Combiner类  
		    job.setReducerClass(Reducer1.class); //为job设置Reduce类 
 
		    job.setMapOutputKeyClass(Text.class);  
		    job.setMapOutputValueClass(Text.class); 
 
		    job.setOutputKeyClass(Text.class);        //设置输出key的类型
		    job.setOutputValueClass(Text.class);//  设置输出value的类型
 
		    //TODO
		    job.setOutputFormatClass(TextOutputFormat.class);
		    FileInputFormat.addInputPath(job, new Path(otherArgs[0])); //为map-reduce任务设置InputFormat实现类   设置输入路径
 
		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//为map-reduce任务设置OutputFormat实现类  设置输出路径
		   
		    FileSystem fs = FileSystem.get(conf);
			Path outPath = new Path(OUTPATH);
			if(fs.exists(outPath))
			{
				fs.delete(outPath, true);
			}
		    
		
		    return job.waitForCompletion(true) ? 1 : -1;
		
		/*Configuration conf = new Configuration();
		conf.set("fs.defaultFS",HDFS);
		Job job = Job.getInstance(conf,"step1");
		job.setJarByClass(Step1.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileSystem fs = FileSystem.get(conf);
		Path inPath = new Path(INPATH);
		if(fs.exists(inPath))
		{
			//FileInputFormat.addInputPath(conf, inPath);
		}
		Path outPath = new Path(OUTPATH);
		if(fs.exists(outPath))
		{
			fs.delete(outPath, true);
		}*/
		
	}
	
	public static void main(String[] args)
	{
		try {
			new Step1().run();
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
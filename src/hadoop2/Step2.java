package hadoop2;
 
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
 
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
 
 
/***
 * 
 *����ÿ���е����ƶȣ������γ�һ�����ƶȾ���
 * 
 * input
 * itemID [userID_sumScore...]
		1	A_2,C_5
		2	A_10,B_3
		3	C_15
		4	A_3,C_5
		5	B_3
		6	A_5,B_5
	ͬʱ��input����һ�ݵ�����cache�У�Ȼ���input��cache��ÿһ�о�����ֵ
	output:
	itemID	[itemID_cos...]
	1	1_1.00,2_0.36,3_0.93,4_0.99,6_0.26
	2	1_0.36,2_1.00,4_0.49,5_0.29,6_0.88
	3	4_0.86,3_1.00,1_0.93
	4	1_0.99,4_1.00,6_0.36,3_0.86,2_0.49
	5	2_0.29,5_1.00,6_0.71
	6	1_0.26,5_0.71,6_1.00,2_0.88,4_0.36
 * @author chenjie
 *
 */
public class Step2 {
	
	/***
	 * input:
	 * itemID [userID_sumScore...]
		1	A_2,C_5
		2	A_10,B_3
		3	C_15
		4	A_3,C_5
		5	B_3
		6	A_5,B_5
		cache : = input
		output:
		1	1_1.00
		1	2_0.36
		1	3_0.93
		1	4_0.99
		1	6_0.26
		2	1_0.36
		2	2_1.00
		2	4_0.49
		2	5_0.29
		2	6_0.88
		3	1_0.93
		3	3_1.00
		3	4_0.86
		4	1_0.99
		4	2_0.49
		4	3_0.86
		4	4_1.00
		4	6_0.36
		5	2_0.29
		5	5_1.00
		5	6_0.71
		6	1_0.26
		6	2_0.88
		6	4_0.36
		6	5_0.71
		6	6_1.00
	 * @author chenjie
	 *
	 */
	public static class Mapper2 extends Mapper<LongWritable,Text,Text,Text>
	{
		private Text outKey = new Text();
		private Text outValue = new Text();
		private List<String> cacheList = new ArrayList<String>();
		
		private DecimalFormat df = new DecimalFormat("0.00");
		
		/***
		 * 	���ļ����浽�ڴ��У�ÿһ��Ϊһ���ַ������������й���list
		 */
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			FileReader fr = new FileReader("itemUserScore1");
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			while((line = br.readLine()) != null)
			{
				cacheList.add(line);
			}
			fr.close();
			br.close();
		}
		
		/***
		 * 	��
		 * 	value ��1	A_2,C_5
			cacheList : 2	A_10,B_3
			Ϊ��
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println("map,key=" + key + ",value=" + value.toString());
			String[] rowAndline = value.toString().split("\t");
			//����к�
			//rowAndline : 1	A_2,C_5
			String row_matrix1 = rowAndline[0];
			//row_matrix1 ��1
			String[] column_value_array_matrix1 = rowAndline[1].split(",");
			//��ø���
			//rowAndline[1] �� A_2,C_5
			//column_value_array_matrix1 : [A_2,C_5]
			
			//|x|=sqrt(x1^2+x2^2+...)
			double denominator1 = 0;
			//��������1��ģ
			for(String colunm : column_value_array_matrix1)//��������1��ÿһ������
			{
				String score = colunm.split("_")[1];
				denominator1 +=  Double.valueOf(score) * Double.valueOf(score);
				//���������ƽ�����ۼӵ�ģ
			}
			denominator1 = Math.sqrt(denominator1);//�����ŵõ�ģ
			
			
			for(String line : cacheList)// ��line 2	A_10,B_3  Ϊ��
			{
				String[] rowAndline2 = line.toString().split("\t");
				//rowAndline2 : 2	A_10,B_3
				String row_matrix2 = rowAndline2[0];
				//row_matrix2 :2
				String[] column_value_array_matrix2 = rowAndline2[1].split(",");
				//column_value_array_matrix2 : A_10,B_3
				
				double denominator2 = 0;//������2��ģ
				for(String colunm : column_value_array_matrix2)
				{
					String score = colunm.split("_")[1];
					denominator2 +=  Double.valueOf(score) * Double.valueOf(score);
				}
				denominator2 = Math.sqrt(denominator2);
				
				
				int numerator = 0;
				//����ɼ��ۼӽ��
				for(String column_value_matrix1 : column_value_array_matrix1)//��������1��ÿһ��(����) A_2,C_5
				{
					String column_maxtrix1 = column_value_matrix1.split("_")[0];
					//����û�ID
					String value_matrix1 = column_value_matrix1.split("_")[1];
					//��÷���
					
					for(String column_value_matrix2 : column_value_array_matrix2)//��������2��ÿһ��(����) A_10,B_3
					{
						String column_maxtrix2 = column_value_matrix2.split("_")[0];
						//����û�ID
						String value_matrix2 = column_value_matrix2.split("_")[1];
						//��÷���
						
						//�����ͬһ������
						if(column_maxtrix2.equals(column_maxtrix1))//����Ҳ������ΪʲôҪ�����кţ�ֻ���к���ȷ����ȣ���֤����ͬһ��λ�õķ���
						{
							numerator += Integer.valueOf(value_matrix1) * Integer.valueOf(value_matrix2);
							//numerator += 2��10
						}
					}
				}
				
				double cos = numerator / (denominator1 * denominator2);
				//������
				if(cos == 0)
					continue;
				outKey.set(row_matrix1);//�����keyֵ����Ϊ��������к�
				outValue.set(row_matrix2 + "_" + df.format(cos));//�����valueֵ����Ϊ�Ҳ�ת�þ�����к�(ʵ�ʾ�����к�)_��λ�õ�ֵ
				context.write(outKey, outValue);
				System.out.println(outKey + "\t" + outValue);
			}
		}
	}
	
	/***
	 * input:
	 *  ("1",["1_1.00","2_0.36","3_0.93","4_0.99","6_0.26"])
		("2",["1_0.36","2_1.00","4_0.49","5_0.29","6_0.88"])
		("3",["4_0.86","3_1.00","1_0.93"])
		("4",["1_0.99","4_1.00","6_0.36","3_0.86","2_0.49"])
		("5",["2_0.29","5_1.00","6_0.71"])
		("6",["1_0.26","5_0.71","6_1.00","2_0.88","4_0.36"])
		
		output:
		1	1_1.00,2_0.36,3_0.93,4_0.99,6_0.26
		2	1_0.36,2_1.00,4_0.49,5_0.29,6_0.88
		3	4_0.86,3_1.00,1_0.93
		4	1_0.99,4_1.00,6_0.36,3_0.86,2_0.49
		5	2_0.29,5_1.00,6_0.71
		6	1_0.26,5_0.71,6_1.00,2_0.88,4_0.36
		��������������
		�õ����յ����ƶȾ���
	 * 
	 * @author chenjie
	 *
	 */
	public static class Reducer2 extends Reducer<Text,Text,Text,Text>
	{
		private Text outKey = new Text();
		private Text outValue = new Text();
		
	
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//System.out.println(ReduceUtils.getReduceInpt(key, values));
			//ֻ�ܱ���һ�Σ�
			StringBuilder sb = new StringBuilder();
			for(Text text : values)
			{
				sb.append(text + ",");
			}
			String line = "";
			if(sb.toString().endsWith(","))
			{
				line = sb.substring(0,sb.length()-1);
			}
			outKey.set(key);
			outValue.set(line);
			context.write(outKey, outValue);
		}
		
	}
	
	
	//private static final String INPATH = "/input/itemUserScore1.txt";
	private static final String INPATH = "/output/tuijian1/part-r-00000";
	private static final String OUTPATH = "/output/tuijian2";
	
	//private static final String CACHE = "/input/itemUserScore1.txt";
	private static final String CACHE = "/output/tuijian1/part-r-00000";
	private static final String HDFS = "hdfs://192.168.58.129:9000";
	
	public int run() throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		 Configuration conf = new Configuration();
		 conf.set("fs.defaultFS",HDFS);
		    //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		    //String[] otherArgs = {"hdfs://pc1:9000/input/chenjie.txt","hdfs://pc1:9000/output/out4"};
		    String[] otherArgs = {INPATH,OUTPATH};
		    //������Ҫ���ò���������������HDFS���ļ�·��
		    if (otherArgs.length != 2) {
		      System.err.println("Usage: wordcount <in> <out>");
		      System.exit(2);
		    }
		    //conf.set("fs.defaultFS",HDFS);
		   // JobConf conf1 = new JobConf(WordCount.class);
		    @SuppressWarnings("deprecation")
			Job job = new Job(conf, "step2");//Job(Configuration conf, String jobName) ����job���ƺ�
		    job.setJarByClass(Step2.class);
		    job.setMapperClass(Mapper2.class); //Ϊjob����Mapper�� 
		    //job.setCombinerClass(IntSumReducer.class); //Ϊjob����Combiner��  
		    job.setReducerClass(Reducer2.class); //Ϊjob����Reduce�� 
 
		    job.addCacheArchive(new URI(CACHE + "#itemUserScore1"));
		    
		    job.setMapOutputKeyClass(Text.class);  
		    job.setMapOutputValueClass(Text.class); 
 
		    job.setOutputKeyClass(Text.class);        //�������key������
		    job.setOutputValueClass(Text.class);//  �������value������
 
		    //TODO  
		    //job.setOutputFormatClass(SequenceFileOutputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		    FileInputFormat.addInputPath(job, new Path(otherArgs[0])); //Ϊmap-reduce��������InputFormatʵ����   ��������·��
		    	
		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//Ϊmap-reduce��������OutputFormatʵ����  �������·��
		  
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
			new Step2().run();
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}


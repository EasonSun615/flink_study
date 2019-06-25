package myflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketWindowWordCount {
	public static void main(String[] args) throws Exception{
		StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> text = env.socketTextStream("localhost",9000,"\n");
		DataStream<Tuple2<String, Integer>> wordCounts = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>(){
			@Override
			public void flatMap(String value, Collector<Tuple2<String, Integer>> out){
				for (String word : value.split("\\s")){
					out.collect(Tuple2.of(word,1));
				}
			}
		});
		DataStream<Tuple2<String, Integer>> windowCounts = wordCounts
			.keyBy(0)
			.timeWindow(Time.seconds(5))
			.sum(1);
		windowCounts.print().setParallelism(1);
		env.execute("Socket window WordCount");


	}
}

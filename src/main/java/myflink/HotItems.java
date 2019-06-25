package myflink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;
import java.io.File;
import java.net.URL;

public class HotItems {
	public static void main(String[] args) throws Exception{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
		Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
		PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
		String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
		PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);


		DataStream<UserBehavior> dataSource = env.createInput(csvInput, pojoType);
		DataStream<UserBehavior> timedData = dataSource
			.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
				@Override
				public long extractAscendingTimestamp(UserBehavior userBehavior) {
					return userBehavior.timestamp * 1000;
				}
			});
		DataStream<UserBehavior> pvData = timedData
			.filter(new FilterFunction<UserBehavior>() {
				@Override
				public boolean filter(UserBehavior userBehavior) throws Exception {
					return userBehavior.behavior.equals("pv");
				}
			});
		DataStream<UserBehavior> windowData = pvData
			.keyBy("itemId")
			.timeWindow(Time.minutes(60), Time.minutes(5))
			.aggregate(new CountAgg(), new WindowResultFunction());




	}

	public static class  CountAgg implements AggregateFunction<UserBehavior, Long, Long>{

		@Override
		public Long createAccumulator(){
			return 0L;
		}

		@Override
		public Long add(UserBehavior userBehavior, Long acc){
			return acc + 1;
		}

		@Override
		public Long getResult(Long acc){
			return acc;
		}

		@Override
		public Long merge(Long aLong, Long acc1) {
			return aLong + acc1;
		}
	}

	public static class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
		@Override
		public void apply(
			Tuple key,
			TimeWindow window,
			Iterable<Long> aggregateResult,
			Collector<ItemViewCount> collector
		) throws Exception {
			Long itemId =((Tuple1<Long>) key).f0;
			Long count = aggregateResult.iterator().next();
			collector.collect(ItemViewCount.of(itemId, window.getEnd(), count));
		}
	}

	public static class ItemViewCount{
		public long itemId;
		public long windowEnd;
		public long viewCount;

		public static ItemViewCount of(long itemId, long windowEnd, long viewCount){
			ItemViewCount result = new ItemViewCount();
			result.itemId = itemId;
			result.windowEnd = windowEnd;
			result.viewCount = viewCount;
			return result;
		}

	}


	public static class UserBehavior {
		public long userId;
		public long itemId;
		public int categoryId;
		public String behavior;
		public long timestamp;
	}
}

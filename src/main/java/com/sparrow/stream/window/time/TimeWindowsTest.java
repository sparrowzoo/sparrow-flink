//package com.sprucetec.recommend.stream;
//
//import com.alibaba.fastjson.JSON;
//import com.sprucetec.recommend.stream.constant.CONSTANT;
//import com.sprucetec.recommend.stream.support.filter.AggregationFilter;
//import com.sprucetec.recommend.stream.support.filter.FilterResult;
//import com.sprucetec.recommend.stream.support.filter.impl.ByteBufferAggregationFilter;
//import com.sprucetec.recommend.stream.support.redis.mapper.HotOfAreaRedisMapper;
//import com.sprucetec.recommend.stream.support.monitor.ProcessingTimeTriggerProxy;
//import com.sprucetec.recommend.stream.manager.order.TopNWindowAggregationOrderBased;
//import com.sprucetec.recommend.stream.pojo.bo.hotn.order.AggregationOfSkuInAreaBO;
//import com.sprucetec.recommend.stream.pojo.bo.hotn.order.OrderDetailEventBO;
//import com.sprucetec.recommend.stream.pojo.dto.OrderItemDTO;
//import com.sprucetec.recommend.stream.pojo.dto.OrderWrapDTO;
//import com.sprucetec.recommend.stream.support.utils.redis.AddressAssemble;
//import com.sprucetec.recommend.stream.support.utils.redis.TtlClusterRedisSink;
//import org.apache.flink.api.common.functions.ReduceFunction;
//import org.apache.flink.api.common.restartstrategy.RestartStrategies;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.runtime.state.filesystem.FsStateBackend;
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.ProcessFunction;
//import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
//import org.apache.flink.util.Collector;
//import org.apache.rocketmq.flink.RocketMQConfig;
//import org.apache.rocketmq.flink.RocketMQSource;
//import org.apache.rocketmq.flink.common.serialization.SimpleKeyValueDeserializationSchema;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.util.Map;
//import java.util.Properties;
//
//public class RealTimeTopAggregationMain {
//    private static Logger logger = LoggerFactory.getLogger(CONSTANT.FLINK_DEBUG_LOGGER);
//
//    private static AggregationFilter aggregationFilter = new ByteBufferAggregationFilter();
//
//    public static void main(String[] args) throws IOException {
//        Properties properties = new Properties();
//        properties.load(RealTimeTopAggregationMain.class.getResourceAsStream("/top_n_of_area_config.properties"));
//        String checkpointDataUri = properties.getProperty("checkpoint_data_uri");
//        String rocketmqNameServerAddr = properties.getProperty("rocketmq_name_server_addr");
//        String rocketmqConsumerGroup = properties.getProperty("rocketmq_consumer_group");
//        String rocketmqConsumerTopic = properties.getProperty("rocketmq_consumer_topic");
//        String redisClusterAddress = properties.getProperty("redis_cluster_address");
//        Integer topN = Integer.valueOf(properties.getProperty("top_n", "80"));
//        Integer slidingProcessingTimeWindowSizeMinutes = Integer.valueOf(properties.getProperty("sliding_processing_time_window_size_minutes", "1"));
//        Integer slidingProcessingTimeWindowSlideSeconds = Integer.valueOf(properties.getProperty("sliding_processing_time_window_slide_seconds", "2"));
//        Integer parallelism = Integer.valueOf(properties.getProperty("parallelism", "4"));
//        Integer tumblingProcessingTimeWindowSeconds=Integer.valueOf(properties.getProperty("tumbling_processing_time_window_seconds","5"));
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        // enable checkpoint
//        env.enableCheckpointing(60000);
//        // make sure 500 ms of progress happen between checkpoints
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
//        env.setStateBackend(new FsStateBackend(checkpointDataUri, true));
//        //env.setStateBackend(new RocksDBStateBackend(checkpointDataUri,true);
//        env.setRestartStrategy(RestartStrategies.fallBackRestart());
//        // advanced options:
//        // set mode to exactly-once (this is the default)
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//
//        //checkpoints have to complete within one minute, or are discarded ms
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        // allow only one checkpoint to be in progress at the same time
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        // enable externalized checkpoints which are retained after job cancellation
//        env.setStateBackend(new FsStateBackend(checkpointDataUri));
//
//        Properties consumerProps = new Properties();
//        consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, rocketmqNameServerAddr);
//        consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, rocketmqConsumerGroup);
//        consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, rocketmqConsumerTopic);
//
//        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
//        //FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
//
//        FlinkJedisClusterConfig flinkJedisPoolConfig = new FlinkJedisClusterConfig.Builder()
//                .setMaxIdle(30000)
//                .setMinIdle(3)
//                .setTimeout(5000)
//                .setMaxRedirections(3)
//                .setMaxTotal(8)
//                .setNodes(AddressAssemble.assemble(redisClusterAddress)).build();
//
//
//        env.addSource(new RocketMQSource(new SimpleKeyValueDeserializationSchema(), consumerProps))
//                .name("rocketmq-source")
//                .setParallelism(parallelism)
//                .process(new ProcessFunction<Map, AggregationOfSkuInAreaBO>() {
//                    @Override
//                    public void processElement(Map in, Context ctx, Collector<AggregationOfSkuInAreaBO> out) {
//                        String value = (String) in.get("value");
//                        OrderWrapDTO orderWrap = JSON.parseObject(value, OrderWrapDTO.class);
//                        if (orderWrap.getOrder_item() == null) {
//                            return;
//                        }
//                        for (OrderItemDTO orderItem : orderWrap.getOrder_item()) {
//                            OrderDetailEventBO orderDetailEvent = new OrderDetailEventBO();
//                            orderDetailEvent.setAreaId(orderWrap.getOrder().getSale_area_id());
//                            orderDetailEvent.setCompanyId(orderWrap.getOrder().getCompany_id());
//                            orderDetailEvent.setSkuId(orderItem.getSku_id());
//                            orderDetailEvent.setTimestamp(orderWrap.getOrder().getCreate_time());
//
//                            AggregationOfSkuInAreaBO aggregationOfSkuInArea = new AggregationOfSkuInAreaBO(orderDetailEvent,
//                                    aggregationFilter.convert(orderDetailEvent.getCompanyId()));
//                            out.collect(aggregationOfSkuInArea);
//                        }
//                    }
//                })
//                .name("map-processor")
//                .setParallelism(parallelism)
//                .keyBy(new KeySelector<AggregationOfSkuInAreaBO, String>() {
//                    @Override
//                    public String getKey(AggregationOfSkuInAreaBO o) throws Exception {
//                        return o.getAreaId() + "-" + o.getSkuId();
//                    }
//                })
//                .window(SlidingProcessingTimeWindows.of(Time.minutes(slidingProcessingTimeWindowSizeMinutes), Time.seconds(slidingProcessingTimeWindowSlideSeconds)))
//                .trigger(new ProcessingTimeTriggerProxy("SlidingProcessingTimeWindow"))
//                .reduce(new ReduceFunction<AggregationOfSkuInAreaBO>() {
//                    @Override
//                    public AggregationOfSkuInAreaBO reduce(AggregationOfSkuInAreaBO origin, AggregationOfSkuInAreaBO compareTo) throws Exception {
//                        FilterResult filterResult=aggregationFilter.or(origin.getCompanyIdBytes(),compareTo.getCompanyIdBytes());
//                        AggregationOfSkuInAreaBO aggregationOfSkuInArea = new AggregationOfSkuInAreaBO(
//                                origin.getAreaId(),
//                                origin.getSkuId(),
//                                (origin.getCount() + compareTo.getCount()),
//                                filterResult.getBytes(),
//                                filterResult.getCount());
//                        return aggregationOfSkuInArea;
//                    }
//                }).name("user-sku-sum-reduce").setParallelism(parallelism)
//                .keyBy(new KeySelector<AggregationOfSkuInAreaBO, Integer>() {
//                    @Override
//                    public Integer getKey(AggregationOfSkuInAreaBO o) throws Exception {
//                        return o.getAreaId();
//                    }
//                })
//                //上一个窗口fire后进入该窗口
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(tumblingProcessingTimeWindowSeconds)))
//                .trigger(new ProcessingTimeTriggerProxy("ProcessingTimeSessionWindows"))
//                .process(new TopNWindowAggregationOrderBased(topN)).setParallelism(parallelism)
//                .addSink(new TtlClusterRedisSink(flinkJedisPoolConfig, new HotOfAreaRedisMapper(),60*60*48)).setParallelism(parallelism).name("top-n-sink");
//
//        try {
//            env.execute("realtime-top-aggregation");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
//

package com.hcb.flink.window.flow.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.omg.CORBA.INTERNAL;

/**
 * @author ChengBing Han
 * @date 20:34  2018/11/19
 * @description flink, 滑动窗口计算，每隔1秒对最近2秒内的单词计数
 */
public class WindowWordCountUseFlink {
    public static final String delimiter = "\n";
    public static final String WORD = "word"; //和WordCount类中的的属性名称相同
    public static final String COUNT = "count";//和WordCount类中的的属性名称相同
    public static final String DEFAULT_HOST = "centos7-1";//和WordCount类中的的属性名称相同
    public static final Integer DEFAULT_PORT = 10101;//和WordCount类中的的属性名称相同


    public static void main(String[] args) throws Exception {
        //获取host,port
        String host;
        int port;

        try {
            final ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
            host = parameterTool.get("host");
        } catch (Exception e) {
            System.out.println("Use default host or port host/1001");
            port = DEFAULT_PORT;
            host = DEFAULT_HOST;
        }

        //获取flink的运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //连接sorcket获取数据
        final DataStreamSource<String> text = env.socketTextStream(host, port, delimiter);

        //2、算子计算
        //2.1 把每行字符串转换<word, count>类型数据
        DataStream<WordWithCount> windowCount = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            public void flatMap(String value, Collector<WordWithCount> collector) throws Exception {
                final String[] splits = value.split("\\s"); //截取所有空格
                for (String word : splits) {
                    collector.collect(new WordWithCount(word, 1L));

                }
            }
        })
                //2.2 把word 相同的数据分为1组
                .keyBy(WORD)
                //2.3指定数据窗口（最近n秒）大小和滑动窗口（每隔几秒）大小
                .timeWindow(Time.seconds(2), Time.seconds(1))
                //2.4 累计（类似mapreduce中的reduce阶段），有两个方法都可以
                .sum(COUNT);
              /*  .reduce(new ReduceFunction<WordWithCount>() {
                    public WordWithCount reduce(WordWithCount wordWithCount1, WordWithCount wordWithCount2) throws Exception {

                        return new WordWithCount(wordWithCount1.word, wordWithCount1.count + wordWithCount2.count);
                    }
                })*/


        //3 将结果打印到控制台
        windowCount.print().setParallelism(1);//设置并行度

        //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("use flik 滑动窗口 word count ");


    }
}

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import java.io.File

fun main(args: Array<String>) {
    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.config.globalJobParameters = params

    val input = "/home/niit/Documents/dataflair/input.text";
    val text = env.readTextFile(input)

    text.flatMap(Tokenizer)
            .keyBy(0)
            .sum(1)
            .writeAsText("/home/niit/Documents/dataflair/output.text")

    env.execute("Streaming WordCount")
}

object Tokenizer : FlatMapFunction<String, Tuple2<String, Int>> {
    override fun flatMap(value: String, out: Collector<Tuple2<String, Int>>) {
        value.split("\\W+".toRegex())
                .asSequence()
                .filter { it.isNotEmpty() }
                .map { it.toLowerCase() }
                .forEach { out.collect(Tuple2(it, 1)) }
    }
}
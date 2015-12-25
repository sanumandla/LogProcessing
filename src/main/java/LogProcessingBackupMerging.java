import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: sridhar.anumandla
 * Date: 5/16/13
 * Time: 9:44 AM
 * To change this template use File | Settings | File Templates.
 */
public class LogProcessingBackupMerging {
    static boolean first = false;
    public static StringBuilder stringBuilder = new StringBuilder();

    public static void main(String[] args) {
//        String inputPath = "data/input1.txt";
        String inputPath = "data/api_logs_0513.txt";
        String outputPath = "data/testoutput2";

//        try {
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, LogProcessingBackupMerging.class);

        Tap inTap = new Hfs(new cascading.scheme.hadoop.TextDelimited(Fields.ALL, false, " ", null, null, false), inputPath);
        Tap outTap = new Hfs(new cascading.scheme.hadoop.TextDelimited(Fields.ALL, false, " ", null, null, false), outputPath);

        Pipe startPipe = new Pipe("Begin");
        Pipe endPipe = new Each(startPipe, new LogTweaker());

        FlowDef flowDef = FlowDef.flowDef();
        flowDef.addSource(startPipe, inTap);
        flowDef.addTailSink(endPipe, outTap);

        FlowConnector flowConnector = new HadoopFlowConnector(properties);
        Flow flow = flowConnector.connect(flowDef);
        flow.complete();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    public static class LogTweaker extends BaseOperation implements Function {

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            try {
                TupleEntry arguments = functionCall.getArguments();
                String tuple = arguments.getTuple().toString();

                String result = "";
                String tmpTuple = arguments.getTuple().toString().replaceAll("\\s", "");
                if (tmpTuple.startsWith("EndAction")) {
                    result = stringBuilder.toString() + " " + tuple;
                    stringBuilder.delete(0, stringBuilder.length());
                    first = false;
                } else if (!first && !tmpTuple.isEmpty()) {
                    stringBuilder.append(tuple);
                    first = true;
                } else {
                    stringBuilder.delete(0, stringBuilder.length());
                    if (!tmpTuple.equals("null") && !tmpTuple.isEmpty()) {
                        stringBuilder.append(tuple);
                        first = true;
                    } else {
                        first = false;
                    }

                }


                System.out.println(result);

                if (!result.isEmpty()) {
                    Tuple output = new Tuple();
                    output.add(result);
                    functionCall.getOutputCollector().add(output);
                }
            } catch (Exception e) {

            }
        }
    }

}

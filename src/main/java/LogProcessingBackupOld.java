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
import cascading.tuple.TupleEntry;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: sridhar.anumandla
 * Date: 5/16/13
 * Time: 9:44 AM
 * To change this template use File | Settings | File Templates.
 */
public class LogProcessingBackupOld {
    public static void main(String[] args) {
        String inputPath = "data/input1.txt";
        String outputPath = "data/testoutput2";

//        try {
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, LogProcessingBackupMerging.class);

        Tap inTap = new Hfs(new cascading.scheme.hadoop.TextDelimited(Fields.ALL, false, " ", null, null, false), inputPath);
        Tap outTap = new Hfs(new cascading.scheme.hadoop.TextDelimited(Fields.ALL, false, ",", null, null, false), outputPath);

        Pipe startPipe = new Pipe("Begin");
        Pipe endPipe = new Each(startPipe, new LogTweaker(), Fields.ALL);

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
            TupleEntry arguments = functionCall.getArguments();
            String tuple = arguments.getTuple().toString().replaceAll("\\s", "");
            System.out.println(tuple);
//            End Action - POST /1.0/permission Time: 125ms
//            tuple = "End Action - POST /1.0/permission Time: 125ms";

//      *************** Important
//            String dateRegex = "^((19|20)\\d\\d)-(0?[1-9]|1[012])-(0?[1-9]|[12][0-9]|3[01])\\s(?:(?:([01]?\\d|2[0-3]):)?([0-5]?\\d):)?([0-5]?\\d),([0-9]*)";
//            String continutedRegex = dateRegex + "\\s-\\s\\[\\w+\\]\\s-[\\s\\w]*[\\w+-_.]*\\s([\\[\\w+@.\\]]*)\\s([\\[0-9\\]]*)\\s([\\[\\w+-\\]]*)\\sEnd\\sAction\\s-\\s\\w+\\s(/[a-zA-Z0-9+&@#/%?=~_|!:,.;]*)\\sTime:\\s([0-9]*)ms";

//            String regex = "End Action - GET|POST (/1.0/[0-9a-zA-Z/&_=-?]*) Time: ([0-9]*)ms";
            String regex = "EndAction-GET|POST(/1.0/[0-9a-zA-Z\\/&_=-?]*)Time:([0-9]*)ms";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(tuple);
            if (matcher.find()) {
                System.out.println("Success ==> " + matcher.group(2) + "======" + matcher.group(1));
                System.out.println(matcher.group(0));
            } else {
                System.out.println("Pattern mismatch !!!");
                System.out.println(tuple);
            }

        }
    }

}

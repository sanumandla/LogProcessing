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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: sridhar.anumandla
 * Date: 5/16/13
 * Time: 9:44 AM
 * To change this template use File | Settings | File Templates.
 */
public class ExtractData {
    static boolean first = false;
    public static StringBuilder stringBuilder = new StringBuilder();

    public static void main(String[] args) {
        String inputPath = "data/merged_files/part-00000";
//        String inputPath = "data/input2.txt";
        String outputPath = "data/testoutput2";

//        try {
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, LogProcessingBackupMerging.class);

        Tap inTap = new Hfs(new cascading.scheme.hadoop.TextDelimited(Fields.ALL, false, " ", null, null, false), inputPath);
        Tap outTap = new Hfs(new cascading.scheme.hadoop.TextDelimited(Fields.ALL, false, ",", null, null, false), outputPath);

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

//              String tupleRegex = dateRegex + "\\s+-\\s+\\[\\w+\\]\\s+-[\\s+\\w+]*[\\w+-_.]*\\s+([\\[\\w+@.\\]]*)\\s+([\\[0-9\\]]*)\\s+([\\[\\w+-\\]]*)\\s+End\\s+Action\\s+-\\s+\\w+\\s+(/[a-zA-Z0-9+&@#/%?=~_-|!:,.;]*)\\s+Time:\\s+([0-9]*)ms";

                String dateRegex = "^(((19|20)\\d\\d)-(0?[1-9]|1[012])-(0?[1-9]|[12][0-9]|3[01])\\s(?:(?:([01]?\\d|2[0-3]):)?([0-5]?\\d):)?([0-5]?\\d),([0-9]*))";
                String tupleRegex = dateRegex + "\\s+-\\s+\\[\\w+\\]\\s+-[\\s+\\w+]*[\\w+-_.]*\\s+([\\[\\w+@.\\]]*)\\s+([\\[0-9\\]]*)\\s+([\\[\\w+-\\]]*)\\s+End\\s+Action\\s+-\\s+\\w+\\s+(/[a-zA-Z0-9+&@#/%?=~_-|!:,.;]*)\\s+Time:\\s+([0-9]*)ms";

                String date, userId, tenantId, sessionId, responseTime, url;

                Pattern pattern = Pattern.compile(tupleRegex);
                Matcher matcher = pattern.matcher(tuple);
                if (matcher.find()) {
                    date = matcher.group(1).replace(",", ".");
                    userId = matcher.group(10).replace("[", "").replace("]", "");
                    tenantId = matcher.group(11).replace("[", "").replace("]", "");
                    sessionId = matcher.group(12).replace("[", "").replace("]", "");
                    url = matcher.group(13);
                    if (url.contains("?")) {
                        url = url.substring(0, url.indexOf("?"));
                    }
                    responseTime = matcher.group(14);
                } else {
                    String tokens[] = tuple.split("\t");
                    date = tokens[0] + "\t" + tokens[1].replace(",", ".");
                    userId = tokens[9].replace("[", "").replace("]", "");
                    tenantId = tokens[10].replace("[", "").replace("]", "");
                    sessionId = tokens[11].replace("[", "").replace("]", "");
                    url = tokens[16];
                    if (url.contains("?")) {
                        url = url.substring(0, url.indexOf("?"));
                    }
                    responseTime = tokens[18].substring(0, tokens[18].indexOf("ms"));
//                    System.out.println(tuple);
                }

                Tuple output = new Tuple();
                output.add(date);
                output.add(userId);
                output.add(tenantId);
                output.add(sessionId);
                output.add(url);
                output.add(responseTime);

                functionCall.getOutputCollector().add(output);

            } catch (Exception e) {
            }
        }
    }

}

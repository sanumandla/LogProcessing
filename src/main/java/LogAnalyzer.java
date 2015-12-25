import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.io.File;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: sridhar.anumandla
 * Date: 5/16/13
 * Time: 9:44 AM
 * To change this template use File | Settings | File Templates.
 */
public class LogAnalyzer {
    static boolean first = false;
    public static StringBuilder stringBuilder = new StringBuilder();
    public static List<String> locations = new ArrayList<String>();

    public static String product, feature;

    public static void main(String[] args) {
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, LogProcessingBackupMerging.class);

//        String inputPath = "data/input1.txt";
//        String inputPath = "data/api_logs_0513.txt";
//        String outputPath = "data/productfeatureout123";
//        String inputPath = "/Users/sridhar.anymandla/Downloads/prod_slave_may/prod_slave_01.log";

        String inputPath = args[0];
        String outputPath = args[1];

        getLocations(inputPath);

//        try {
        List<Pipe> pipes = new ArrayList<Pipe>();
        Map<Pipe, Tap> pipeTapMap = new HashMap<Pipe, Tap>();

        int index = 0;
        for (String location : locations) {
            Pipe pipe = new Pipe("pipe_" + index);
            Tap inTap = new Hfs(new cascading.scheme.hadoop.TextDelimited(Fields.ALL, false, " ", null, null, false), location);
            pipeTapMap.put(pipe, inTap);
            pipes.add(pipe);
            index++;
        }

        Pipe mergePipe = new Merge(pipes.toArray(new Pipe[pipes.size()]));
        Tap outTap = new Hfs(new cascading.scheme.hadoop.TextDelimited(Fields.ALL, false, ",", null, null, false), outputPath);

        Pipe endPipe = new Each(mergePipe, new LogTweaker());

        FlowDef flowDef = FlowDef.flowDef();

        Set<Pipe> pipeSet = pipeTapMap.keySet();
        for (Pipe pipe : pipeSet) {
            flowDef.addSource(pipe, pipeTapMap.get(pipe));
        }
        flowDef.addTailSink(endPipe, outTap);

        FlowConnector flowConnector = new HadoopFlowConnector(properties);
        Flow flow = flowConnector.connect(flowDef);
        flow.complete();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    public static void getLocations(String inputPath) {
        String[] dirPaths = inputPath.split(",");
        for (String dirPath : dirPaths) {
            File directory = new File(dirPath);
            if (directory.isFile()) {
                locations.add(directory.getPath());
            }
            File[] files = directory.listFiles();
            if (files == null || files.length == 0) {
                continue;
            }
            for (File file : files) {
                if (file.isDirectory()) {
                    getLocations(file.getPath());
                } else {
                    locations.add(file.getPath());
                }
            }
        }
    }

    public static class LogTweaker extends BaseOperation implements Function {

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            try {
                TupleEntry arguments = functionCall.getArguments();
                String tuple = arguments.getTuple().toString();

                String mergedTuple = "";
                String tmpTuple = tuple.replaceAll("\\s", "");
                if (tmpTuple.startsWith("EndAction")) {
                    mergedTuple = stringBuilder.toString() + " " + tuple;
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

                if (!mergedTuple.isEmpty()) {
                    String dateRegex = "^(((19|20)\\d\\d)-(0?[1-9]|1[012])-(0?[1-9]|[12][0-9]|3[01])\\s(?:(?:([01]?\\d|2[0-3]):)?([0-5]?\\d):)?([0-5]?\\d),([0-9]*))";
                    String tupleRegex = dateRegex + "\\s+-\\s+\\[\\w+\\]\\s+-[\\s+\\w+]*[\\w+-_.]*\\s+([\\[\\w+@.\\]]*)\\s+([\\[0-9\\]]*)\\s+([\\[\\w+-\\]]*)\\s+End\\s+Action\\s+-\\s+(\\w+)\\s+(/[a-zA-Z0-9+&@#/%?=~_-|!:,.;]*)\\s+Time:\\s+([0-9]*)ms";

                    String date, userId, tenantId, sessionId, responseTime, url, type;

                    Pattern pattern = Pattern.compile(tupleRegex);
                    Matcher matcher = pattern.matcher(mergedTuple);
                    if (matcher.find()) {
                        date = matcher.group(1).replace(",", ".");
                        userId = matcher.group(10).replace("[", "").replace("]", "");
                        tenantId = matcher.group(11).replace("[", "").replace("]", "");
                        sessionId = matcher.group(12).replace("[", "").replace("]", "");
                        type = matcher.group(13);
                        url = matcher.group(14);
                        if (url.contains("?")) {
                            url = url.substring(0, url.indexOf("?"));
                        }
                        responseTime = matcher.group(15);
                    } else {
                        String tokens[] = mergedTuple.split("\t");
                        date = tokens[0] + "\t" + tokens[1].replace(",", ".");
                        userId = tokens[9].replace("[", "").replace("]", "");
                        tenantId = tokens[10].replace("[", "").replace("]", "");
                        sessionId = tokens[11].substring(tokens[11].indexOf("[") + 1, tokens[11].indexOf("]"));
                        type = tokens[14];
                        url = tokens[15];
                        if (url.contains("?")) {
                            url = url.substring(0, url.indexOf("?"));
                        }
                        responseTime = tokens[17].substring(0, tokens[17].indexOf("ms"));
                    }

                    product = null;
                    feature = null;
                    getProductFeature(url);

                    Tuple output = new Tuple();
                    output.add(date);
                    output.add(userId);
                    output.add(tenantId);
                    output.add(sessionId);
                    output.add(url);
                    output.add(responseTime);
                    output.add(type);
                    output.add(product);
                    output.add(feature);

                    functionCall.getOutputCollector().add(output);
                }
            } catch (Exception e) {
                System.out.println(e.toString());
            }
        }
    }

    public static void getProductFeature(String url) {
        url = url.trim();
        if (url.startsWith("/1.0/alerts")) {
            product = "Alerts";
            feature = "";
        } else if (url.startsWith("/1.0/alerts/blocks")) {
            product = "Alerts";
            feature = "Setting";
        } else if (url.startsWith("/1.0/alerts/instances")) {
            product = "Alerts";
            feature = "View Alert";
        }  else if (url.startsWith("/1.0/dashboard") || url.startsWith("/1.0/widget")) {
            product = "Dashboards";
            feature = "";
        } else if (url.startsWith("/v0/validateSession")) {
            product = "Actions/Metrics";
            feature = "";
        } else if (url.startsWith("/1.0/tenant") && url.contains("product") && url.contains("recommendations") && url.contains("crosssell")) {
            product = "OpenAPI";
            feature = "ProductRec";
        } else if (url.startsWith("/1.0/tenant") && url.contains("user") && url.contains("recommendations") && url.contains("product")) {
            product = "OpenAPI";
            feature = "UserRec";
        } else if (url.startsWith("/1.0/tenant") && url.contains("/product/details/")) {
            product = "OpenAPI";
            feature = "ProductRecDetails";
        } else if (url.startsWith("/1.0/tenant") && url.contains("/salesevent/details/")) {
            product = "OpenAPI";
            feature = "ProductSalesDetails";
        } else if (url.startsWith("/1.0/user") && url.contains("recommendations")) {
            product = "OpenAPI";
            feature = "UserRec";
        } else if (url.startsWith("/1.0/tenant") && url.contains("upload")) {
            product = "OpenAPI";
            feature = "Upload";
        } else if (url.startsWith("/1.0/accessKey")) {
            product = "OpenAPI";
            feature = "Settings";
        } else if (url.startsWith("/1.0/tenant") && (url.contains("analytics/model/smartmail-engage/downsample") ||
                url.contains("analytics/model/smartmail-unsub/downsample") || url.contains("analytics/model/smartmail-engage/coefficients") ||
                        url.contains("analytics/model/smartmail-unsub/coefficients")) ) {
            product = "EMO";
            feature = "RDownsample";
        } else if (url.startsWith("/1.0/tenant") && ( url.contains("analytics/churn") || url.contains("analytics/churn/factors"))) {
            product = "Pathway";
            feature = "Churn";
        } else if (url.startsWith("/1.0/tenant") && url.contains("analytics/churn/targetchurn")) {
            product = "Pathway";
            feature = "ChurnInteraction";
        } else if (url.startsWith("/1.0/tenant") && url.contains("analytics/settings/features")) {
            product = "Pathway";
            feature = "ChurnSettings";
        } else if (url.startsWith("/analytics/emoplusplus/") || url.startsWith("/config")  || url.startsWith("/home")
                || url.startsWith("/login") || url.startsWith("/tenant") || url.startsWith("/tenantvalues") || url.startsWith("/uploadloginfile") || url.startsWith("/users")
                || url.startsWith("/workermessages") || url.startsWith("/permission") || url.startsWith("/roles") || url.startsWith("/products") || url.startsWith("/quartzjobs") || url.startsWith("/widget")
                || url.startsWith("/queries") || url.startsWith("/query") || url.startsWith("/requestPasswordReset") || url.startsWith("/1.0/permission") ||
                url.equals("/")) {
            product = "BackendUI";
            feature = "";
        } else if (url.startsWith("/dqe/dedupe/")) {
            product = "DQE";
            feature = "";
        } else if (url.startsWith("/1.0/webtag")) {
            product = "WebTag";
            feature = "";
        } else if (url.startsWith("/resetPassword")) {
            product = "UI";
            feature = "ResetPassword";
        } else if (url.startsWith("/v1/login")) {
            product = "UI";
            feature = "Login";
        } else if (url.startsWith("/v1/switchTenant")) {
            product = "UI";
            feature = "SwitchTenant";
        } else if (url.startsWith("/v1/logout")) {
            product = "UI";
            feature = "Logout";
        }
    }

}
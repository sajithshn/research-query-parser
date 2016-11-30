import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by sajith on 11/13/16.
 */
public class Parser {

    // Filter
    // Passed into a function
    // Imported Exported function

    // Statefull Funtion
    private String executionPlan;
    // Check for state
    public static void main(String[] args){
        String executionPlan = "define stream inputEmailsStream (iij_timestamp long, fromAddress string, toAddresses string, ccAddresses string, bccAddresses string, subject string, body string, regexstr string);"
                + "from TempStream [(roomNo >= 100 and toAddresses < 110) and temp > 40 ] select avg(ccAddresses)"
                + "from TempStream [(roomNo >= 101 and roomNo < 110) and temp > 40 ]"
                + "having fromAddress == '30' "
                + "partition with ( subject == '30' as 'scp' and body == 'ss' of TempStream )";
        Parser parser = new Parser(executionPlan);
        parser.parse(executionPlan);
    }

    public Parser(String executionPlan){
        this.executionPlan = executionPlan;
    }

    public void parse(String executionPlan){
        checkForCompression(executionPlan);

    }

    public void checkForStatefulness(String executionPlan){
        boolean isStateful = isWindowUsed(executionPlan);
        if (isStateful){
            System.out.println("This query is stateful as it uses windows. Therefor can't be data switched.");
            return;
        }

        isStateful = isPatternUsed(executionPlan);
        if (isStateful){
            System.out.println("This query is stateful as it uses patterns. Therefor can't be data switched.");
            return;
        }

        isStateful = isSequenceUsed(executionPlan);
        if (isStateful){
            System.out.println("This query is stateful as it uses sequence. Therefor can't be data switched.");
            return;
        }

        System.out.println("This query is stateless. Therefore, can be query switched.");
    }

    private boolean isWindowUsed(String executionPlan){
        return executionPlan.contains("#window");
    }

    private boolean isPatternUsed(String executionPlan){
        return executionPlan.contains("->");
    }

    private boolean isSequenceUsed(String executionPlan){
        Matcher match = Pattern.compile("(?=(" + "from\\s+every))").matcher(executionPlan);
        while(match.find()) {
            return true;
        }
        return false;
    }

    public void checkForCompression(String executionPlan){
        Set<String> stringFields = getStringFields(executionPlan);
        for (String field : stringFields){
            boolean testPassed = isNotUsedInFilter(field);
            if (!testPassed){
                System.out.println(field + " is used in a filter. Therefore Can't compress");
            }

            testPassed = isNotPassedIntoAFunction(field);
            if (!testPassed){
                System.out.println(field + " is passed into a function. Therefore Can't compress");
            }

            testPassed = isNotUsedInHaving(field);
            if (!testPassed){
                System.out.println(field + " is used in having. Therefore Can't compress");
            }

            testPassed = isNotUsedInRangePartition(field);
            if (!testPassed){
                System.out.println(field + " is used in range partition. Therefore Can't compress");
            }


            if (testPassed){
                System.out.println(field + " can be compressed");
            }


        }
    }

    private Set<String> getStringFields(String streamDefinition){
        Set<String> returnValues = new HashSet<String>();
        String[] fields = streamDefinition.substring(streamDefinition.indexOf("("), streamDefinition.indexOf(")")).split(",");

        for (String field : fields){
            String[] fieldDefinition = field.trim().split(" ");

            if (fieldDefinition[1].toLowerCase().equals("string")){
                returnValues.add(fieldDefinition[0]);
            }
        }

        return returnValues;
    }

    private boolean isNotUsedInFilter(String fieldName){
        Matcher match = Pattern.compile("(?=(" + "\\[(.*?)\\]" + "))").matcher(executionPlan);
        while(match.find()) {
            if (match.group(1).contains(fieldName)){
                return false;
            }
        }
        return true;
    }

    private boolean isNotPassedIntoAFunction(String fieldName){
        Matcher match = Pattern.compile("(?=(" + "\\(\\s*" + fieldName + "\\s*\\)" + "))").matcher(executionPlan);
        while(match.find()) {
            if (match.group(1).contains(fieldName)){
                return false;
            }
        }
        return true;
    }

    private boolean isNotUsedInHaving(String fieldName){
        Matcher match = Pattern.compile("(?=(" + "having\\s+" + fieldName + "))").matcher(executionPlan);
        while(match.find()) {
                return false;
        }
        return true;
    }

    private boolean isNotUsedInRangePartition(String fieldName){
        Matcher match = Pattern.compile("(?=(" + ".*partition\\s+with\\s*\\(.*"+ fieldName +"\\s*==.+\\)" + "))").matcher(executionPlan);
        while(match.find()) {
            return false;
        }
        return true;
    }
}
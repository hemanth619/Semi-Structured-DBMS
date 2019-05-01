package xmldb;

//originally from : joins.C

import diskmgr.PCounter;
import iterator.*;
import heap.*;
import global.*;
import java.io.*;
import java.util.*;
import java.lang.*;

import iterator.Iterator;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import static tests.TestDriver.FAIL;
import static tests.TestDriver.OK;

class Task4QueryPlan2 implements GlobalConst {

    private static boolean OK = true;
    private static boolean FAIL = false;
    private Vector xmlTuples;
    static XMLParser xmlParser = new XMLParser();
    static HashMap<String, Integer> tagIndex = new HashMap<>();
    static HashSet<String> globalResults = new HashSet<>();

    private static int colLength = 0;

//	ArrayList<ArrayList<String>> sortedRules;

    static int currentTagIndex = 1; // Used in the hashmap
    static int currentProjCount = 6;
    static int currentInstanceIndex = 1;

    //    ArrayList<SortMerge> sortMergeInstanceList = new ArrayList<>();
    static ArrayList<NestedLoopsJoins> nestedLoopInstanceList = new ArrayList<>();

    /** Constructor
     */
    public Task4QueryPlan2() {

        //build XMLTuple table
        xmlTuples  = new Vector();


        boolean status = OK;
        int numTuples = xmlParser.listOfXMLObjects.size();
        int numTuplesAttrs = 2;

        for (int i =0; (i < numTuples); i++) {
            //fixed length record
            XMLInputTreeNode node = xmlParser.listOfXMLObjects.get(i);
            xmlTuples.addElement(new XMLTuple(node.interval.start, node.interval.end, node.interval.level, node.tagName));
        }



        String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb";
        String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

        String remove_cmd = "/bin/rm -rf ";
        String remove_logcmd = remove_cmd + logpath;
        String remove_dbcmd = remove_cmd + dbpath;
        String remove_joincmd = remove_cmd + dbpath;

        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
            Runtime.getRuntime().exec(remove_joincmd);
        }
        catch (IOException e) {
            System.err.println (""+e);
        }


        SystemDefs sysdef = new SystemDefs( dbpath, 10000, 10000, "Clock" );

        // creating the sailors relation
        AttrType [] Stypes = new AttrType[numTuplesAttrs];
        Stypes[0] = new AttrType (AttrType.attrInterval);
        Stypes[1] = new AttrType (AttrType.attrString);

        //SOS
        short [] Ssizes = new short [1];
        Ssizes[0] = 10; //first elt. is 30

        Tuple t = new Tuple();
        try {
            t.setHdr((short) numTuplesAttrs, Stypes, Ssizes);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // inserting the tuple into file "sailors"
        RID             rid;
        Heapfile        f = null;
        try {
            f = new Heapfile("test.in");
        }
        catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) numTuplesAttrs, Stypes, Ssizes);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        for (int i=0; i<numTuples; i++) {
            try {

                t.setIntervalFld(1, ((XMLTuple)xmlTuples.elementAt(i)).interval);
//                System.out.println(((XMLTuple)xmlTuples.elementAt(i)).tagName + " " + i);
                t.setStrFld(2, ((XMLTuple)xmlTuples.elementAt(i)).tagName);

            }
            catch (Exception e) {
                System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            }
            catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() ***");
                status = FAIL;
                e.printStackTrace();
            }


        }
        // System.out.println(xmlParser.listOfXMLObjects.size());

        if (status != OK) {
            //bail out
            System.err.println ("*** Error creating relation for sailors");
            Runtime.getRuntime().exit(1);
        }

    }

    public static ArrayList<ArrayList<String>> getSortedRules(ArrayList<String> tags, ArrayList<ArrayList<String>> rules){
        ArrayList<Integer> isRuleVisited = new ArrayList<>();
        ArrayList<Integer> isTagVisited = new ArrayList<>();
        ArrayList<ArrayList<String>> sortedRules = new ArrayList<ArrayList<String>>();
        Queue<String> q = new LinkedList<>();

        //Mark all rules as unvisited first
        for(int i=0; i<rules.size(); i++){
            isRuleVisited.add(0);
        }
        // isRuleVisited.set(0,1);
        for(int i=0; i<tags.size(); i++){
            isTagVisited.add(0);
        }

        ((LinkedList<String>) q).add(rules.get(0).get(0));
        ((LinkedList<String>) q).add(rules.get(0).get(1));
        isTagVisited.set(tags.indexOf(rules.get(0).get(0)), 1);
        // isTagVisited.set(tags.indexOf(rules.get(0).get(1)), 1);


        while(q.size() != 0){
            String tag = q.remove();
            isTagVisited.set(tags.indexOf(tag),1);
            for(int i = 0; i< rules.size(); i++){
                if(isRuleVisited.get(i) == 0){

                    if(rules.get(i).get(0).equals(tag)){
                        isRuleVisited.set(i, 1);
                        sortedRules.add(rules.get(i));
                        if(isTagVisited.get(tags.indexOf(tag)) == 0){
                            q.add(tag);

                        }
                        if(isTagVisited.get(tags.indexOf(rules.get(i).get(1))) == 0){
                            q.add(rules.get(i).get(1));
                        }

                    }
                    if(rules.get(i).get(1).equals(tag)){
                        isRuleVisited.set(i, 1);
                        sortedRules.add(rules.get(i));
                        if(isTagVisited.get(tags.indexOf(tag)) == 0){
                            q.add(tag);
                        }
                        if(isTagVisited.get(tags.indexOf(rules.get(i).get(0))) == 0){
                            q.add(rules.get(i).get(0));
                        }

                    }
                }
            }
        }



        return sortedRules;
    }

//    public ArrayList<ArrayList<String>> wrapperForSortedRules(String filename) {
//    	// assume this reads the query file, and produces a list of tag names
//    	try{
//            File file = new File(filename+".txt");
//            Scanner scan =new Scanner(file);
//
//            ArrayList<String> tags = new ArrayList<>();
//            ArrayList<ArrayList<String>> rules = new ArrayList<ArrayList<String>>();
//
//
//            //Scan numberoftags, tags and rules from file
//            int numberOfTags = scan.nextInt();
//            for(int i=0; i<numberOfTags; i++){
//                String tag = scan.next();
//                if(tag.length() > 5)
//                    tag = tag.substring(0,5);
//                tags.add(tag);
//                //  System.out.println(tags.get(i));
//            }
//            int j = 0;
//            while(scan.hasNext()){
//                ArrayList<String> temp = new ArrayList<>();
//                int leftTag = scan.nextInt();
//                int rightTag = scan.nextInt();
//                String relation = scan.next();
//                //System.out.println(leftTag + " " + rightTag + " " + relation);
//                temp.add(tags.get(leftTag-1));
//                temp.add(tags.get(rightTag-1));
//                temp.add(relation);
//                //   System.out.println(temp.get(0) + " " + temp.get(1) + " " + temp.get(2));
//                rules.add(temp);
//
//            }
//            ArrayList<String> reversedtags = new ArrayList<>();
//            for(int i= tags.size()-1; i >=0; i--){
//                reversedtags.add(tags.get(i));
//            }
//
//            ArrayList<ArrayList<String>> reversedRules = new ArrayList<ArrayList<String>>();
//            for(int i = rules.size()-1; i >= 0; i--) {
//                reversedRules.add(rules.get(i));
//            }
//
//            ArrayList<ArrayList<String>> sortedRules = XMLQueryParsing.getSortedRules(tags, rules);
//            return sortedRules;
//    	} catch(Exception e) {
//    		e.printStackTrace();
//    	}
//    	return new ArrayList<ArrayList<String>>();
//    }

    public static SortMerge2 processQuery(ArrayList<ArrayList<String>> sortedRules, ArrayList<String> tags) {
        ArrayList<NestedLoopsJoins> instances = nestedLoopInstanceList;
        NestedLoopsJoins sm1, sm2;

        int joinColumnIndex = 1; // incremented each time a new "tag" is added, because a tag will bring 2 columns

        boolean status = OK;

        AttrType [] Stypes = new AttrType[4];
        Stypes[0] = new AttrType (AttrType.attrInterval);
        Stypes[1] = new AttrType (AttrType.attrString);
        Stypes[2] = new AttrType (AttrType.attrInterval);
        Stypes[3] = new AttrType (AttrType.attrString);

        short [] Ssizes = new short[2];
        Ssizes[0] = 10;
        Ssizes[1] = 10;


//        AttrType [] Stypes2 = new AttrType[2];
//        Stypes2[0] = new AttrType (AttrType.attrInterval);
//        Stypes2[1] = new AttrType (AttrType.attrString);

        FldSpec [] projectionList = new FldSpec[6];
        projectionList[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projectionList[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        projectionList[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        projectionList[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
        projectionList[4] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
        projectionList[5] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);



        TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
        SortMerge2 tempInstance =null;

        if (!tagIndex.containsKey(sortedRules.get(0).get(0))) {
            tagIndex.put(sortedRules.get(0).get(0), currentTagIndex);
            currentTagIndex+= 2;
        }

        if (!tagIndex.containsKey(sortedRules.get(0).get(1))) {
            tagIndex.put(sortedRules.get(0).get(1), currentTagIndex);
            currentTagIndex+= 2;
        }

        joinColumnIndex = tagIndex.get(sortedRules.get(currentInstanceIndex).get(0));

        CondExpr[] expr = new CondExpr[2];
        expr[0] = new CondExpr();
        expr[1] = null;
        expr[0].next  = null;

        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);

        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),joinColumnIndex);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        expr[0].flag = 1;

        if (!tagIndex.containsKey(sortedRules.get(currentInstanceIndex).get(1))) {
            tagIndex.put(sortedRules.get(currentInstanceIndex).get(1), currentTagIndex);
            currentTagIndex+= 2;
        }

        sm1 = nestedLoopInstanceList.get(0);
        sm2 = nestedLoopInstanceList.get(currentInstanceIndex);

        try {
            tempInstance = new SortMerge2(Stypes, 4, Ssizes, Stypes, 4, Ssizes, joinColumnIndex, 12, 1, 12, 10, sm1, sm2, false, false, ascending, expr, projectionList, 6);
        }
        catch (Exception e) {
            System.err.println("*** join error in SortMerge constructor ***");
            status = FAIL;
            System.err.println (""+e);
            e.printStackTrace();
        }


        currentInstanceIndex++;
        CondExpr[] condExpr;
        SortMerge2 sortMerge2Instance;

        int columnCount, tempVal;
        boolean flag = false;
        AttrType[] Stypes2;
        colLength = 6;

        SortMerge2 tempInstance2 = null;
        for (int index = currentInstanceIndex; index < nestedLoopInstanceList.size(); index++) {
//    		sortMerge2Instance = tempInstance;
//    		sm2 = sortMergeInstanceList.get(index);

            Stypes2 = new AttrType[4];
            Stypes2[0] = new AttrType (AttrType.attrInterval);
            Stypes2[1] = new AttrType (AttrType.attrString);
            Stypes2[2] = new AttrType (AttrType.attrInterval);
            Stypes2[3] = new AttrType (AttrType.attrString);

            AttrType[] Stypes1 = new AttrType[tagIndex.size()*2];
            for (int currentColumn = 0; currentColumn < tagIndex.size()*2; currentColumn++) {
                if (currentColumn%2 == 0) {
                    Stypes1[currentColumn] = new AttrType(AttrType.attrInterval);
                } else {
                    Stypes1[currentColumn] = new AttrType(AttrType.attrString);
                }
            }

            short[] Ssizes1 = new short[tagIndex.size()];
            for (int i=0; i<tagIndex.size(); i++) {
                Ssizes1[i] = 10;
            }

            short [] Ssizes2 = new short[2];
            Ssizes2[0] = 10;
            Ssizes2[1] = 10;


            joinColumnIndex = tagIndex.get(sortedRules.get(index).get(0));

            if (!tagIndex.containsKey(sortedRules.get(index).get(1))) {
                tagIndex.put(sortedRules.get(index).get(1), currentTagIndex);
                currentTagIndex+= 2;
                currentProjCount+= 2;
                flag = true;
            }

            tempVal = flag ? currentProjCount : currentProjCount-2;
            FldSpec [] projectionList2 = new FldSpec[tempVal];

            colLength = tempVal;
            for (int i=0; i<tempVal; i++) {
                projectionList2[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
            }

            if (flag) {
                projectionList2[tempVal-2] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
                projectionList2[tempVal-1] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);
            }

            // TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);

            tempInstance2 = tempInstance;
            sm2 = nestedLoopInstanceList.get(index);
//            System.out.println("Column length: " + projectionList2.length);
            try {
                tempInstance = new SortMerge2(Stypes1, Stypes1.length, Ssizes1, Stypes2, Stypes2.length, Ssizes2, joinColumnIndex, 12, 1, 12, 10, tempInstance2, sm2, false, false, ascending, expr, projectionList2, tempVal);
            }
            catch (Exception e) {
                System.err.println("*** join error in SortMerge constructor ***");
                status = FAIL;
                System.err.println (""+e);
                e.printStackTrace();
            }

            flag = false;
        }

//    	boolean done = false;
//        Tuple t = null;
//        HashSet<String> tupleSet = new HashSet<>();
//        try{
//            while(!done){
//                t = tempInstance.get_next();
//                if(t == null) {
//                    done = true;
//                    break;
//                }
//
////
//               String res = "";
//                for (int k=1; k<=colLength; k++) {
//                	if(k%2 != 0) {
//                		res += " | Start = " + t.getIntervalFld(k).getStart() + " End = " + t.getIntervalFld(k).getEnd() + " Level = " + t.getIntervalFld(k).getLevel();
////                		System.out.print(" | Start = " + t.getIntervalFld(k).getStart() + " End = " + t.getIntervalFld(k).getEnd() + " Level = " + t.getIntervalFld(k).getLevel());
//                	}else {
//                		res += " TagName = " + t.getStrFld(k);
////                		System.out.print(" TagName = " + t.getStrFld(k) );
//                	}
//
//                }
////                System.out.println();
//                globalResults.add(res);
//            }
//
//        } catch(Exception e){
//            e.printStackTrace();
//        }
        tagIndex = new HashMap<>();
        globalResults = new HashSet<>();

        colLength = 0;

//    	ArrayList<ArrayList<String>> sortedRules;

        currentTagIndex = 1; // Used in the hashmap
        currentProjCount = 6;
        currentInstanceIndex = 1;
        nestedLoopInstanceList = new ArrayList<>();
        return tempInstance;
    }

    public static void wrapper(ArrayList<String> tags, ArrayList<ArrayList<String>> sortedRules ) {
//    	ArrayList<ArrayList<String>> sortedRules = this.wrapperForSortedRules(filename);

        for(int i=0; i<sortedRules.size(); i++){
            QP3(sortedRules.get(i).get(0), sortedRules.get(i).get(1), sortedRules.get(i).get(2), tags);
        }
//    	return sortedRules;
    }

    public static NestedLoopsJoins createCondExprQP3(String tagName1, String tagName2, String operand, FileScan iterator1, FileScan iterator2, ArrayList<String> tags) {
        int firstIndex = Integer.parseInt(tagName1);
        int secondIndex = Integer.parseInt(tagName2);
        String initRule = operand;
        CondExpr[] exprInit = null;
        if(tags.get(firstIndex-1).equals("*") && tags.get(secondIndex-1).equals("*")){

            // Both tags are *

            exprInit = new CondExpr[2];
            exprInit[0] = new CondExpr();
            exprInit[1] = new CondExpr();

            exprInit[0].next  = null;
            if(initRule.equals("AD")){
                exprInit[0].op    = new AttrOperator(AttrOperator.aopGT);
            }else{
                exprInit[0].op    = new AttrOperator(AttrOperator.aopPC);
            }

            exprInit[0].type1 = new AttrType(AttrType.attrSymbol);
            exprInit[0].type2 = new AttrType(AttrType.attrSymbol);
            exprInit[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
            exprInit[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
            exprInit[0].flag = 1;

            exprInit[1] = null;

        }
        else if(tags.get(firstIndex-1).equals("*") || tags.get(secondIndex-1).equals("*")){

            exprInit = new CondExpr[3];
            exprInit[0] = new CondExpr();
            exprInit[1] = new CondExpr();
            exprInit[2] = new CondExpr();

            if(initRule.equals("AD")){
                exprInit[0].op    = new AttrOperator(AttrOperator.aopGT);
            }else{
                exprInit[0].op    = new AttrOperator(AttrOperator.aopPC);
            }

            exprInit[0].type1 = new AttrType(AttrType.attrSymbol);
            exprInit[0].type2 = new AttrType(AttrType.attrSymbol);
            exprInit[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
            exprInit[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
            exprInit[0].flag = 1;

            if(tags.get(firstIndex-1).equals("*")){
                exprInit[1].op    = new AttrOperator(AttrOperator.aopEQ);
                exprInit[1].type1 = new AttrType(AttrType.attrSymbol);
                exprInit[1].type2 = new AttrType(AttrType.attrString);
                exprInit[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
                exprInit[1].operand2.string = tags.get(secondIndex-1);
                exprInit[1].next  = null;
            }else{
//                System.out.println("Right star loop");
                exprInit[1].op    = new AttrOperator(AttrOperator.aopEQ);
                exprInit[1].type1 = new AttrType(AttrType.attrSymbol);
                exprInit[1].type2 = new AttrType(AttrType.attrString);
                exprInit[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),2);
                exprInit[1].operand2.string = tags.get(firstIndex-1);
                exprInit[1].next  = null;
//                System.out.println("Right star value used" + tags.get(firstIndex-1));
            }
            exprInit[2] = null;
        }
        else{
            exprInit = new CondExpr[4];
            exprInit[0] = new CondExpr();
            exprInit[1] = new CondExpr();
            exprInit[2] = new CondExpr();
            exprInit[3] = new CondExpr();
            if(initRule.equals("AD")){
                exprInit[0].op    = new AttrOperator(AttrOperator.aopGT);
            }else{
                exprInit[0].op    = new AttrOperator(AttrOperator.aopPC);
            }

            exprInit[0].type1 = new AttrType(AttrType.attrSymbol);
            exprInit[0].type2 = new AttrType(AttrType.attrSymbol);
            exprInit[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
            exprInit[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
            exprInit[0].flag = 1;


            exprInit[1].op    = new AttrOperator(AttrOperator.aopEQ);
            exprInit[1].type1 = new AttrType(AttrType.attrSymbol);
            exprInit[1].type2 = new AttrType(AttrType.attrString);
            exprInit[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),2);
            exprInit[1].operand2.string = tags.get(firstIndex-1);
            exprInit[1].next  = null;


            exprInit[2].op    = new AttrOperator(AttrOperator.aopEQ);
            exprInit[2].next  = null;
            exprInit[2].type1 = new AttrType(AttrType.attrSymbol);
            exprInit[2].type2 = new AttrType(AttrType.attrString);
            exprInit[2].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
            exprInit[2].operand2.string = tags.get(secondIndex-1);

            exprInit[3] = null;

        }

//    	CondExpr[] expr = new CondExpr[4];
//        expr[0] = new CondExpr();
//        expr[1] = new CondExpr();
//        expr[2] = new CondExpr();
//        expr[3] = null;
//
//        expr[0].next  = null;
//
//        switch (operand) {
//        case "AD":
//            expr[0].op    = new AttrOperator(AttrOperator.aopGT);
//        	break;
//        case "PC":
//            expr[0].op    = new AttrOperator(AttrOperator.aopPC);
//        	break;
//        default:
//        	break;
//        }
//
//        expr[0].type1 = new AttrType(AttrType.attrSymbol);
//        expr[0].type2 = new AttrType(AttrType.attrSymbol);
//        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
//        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
//        expr[0].flag = 1;
//        if("*".equals(tagName1)) {
//        	expr[1] = null;
//        }else {
//        	expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
//        	expr[1].type1 = new AttrType(AttrType.attrSymbol);
//        	expr[1].type2 = new AttrType(AttrType.attrString);
//        	expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),2);
//        	expr[1].operand2.string = tagName1;
//        	expr[1].next  = null;
//
//        }
//        if("*".equals(tagName2)) {
//        	expr[2] = null;
//        }else {
//	        expr[2].op    = new AttrOperator(AttrOperator.aopEQ);
//	        expr[2].next  = null;
//	        expr[2].type1 = new AttrType(AttrType.attrSymbol);
//	        expr[2].type2 = new AttrType(AttrType.attrString);
//	        expr[2].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
//	        expr[2].operand2.string = tagName2;
//        }
        Tuple t = new Tuple();

        AttrType [] Stypes = new AttrType[2];
        Stypes[0] = new AttrType (AttrType.attrInterval);
        Stypes[1] = new AttrType (AttrType.attrString);

        //SOS
        short [] Ssizes = new short[1];
        Ssizes[0] = 10; //first elt. is 30

        FldSpec [] Sprojection = new FldSpec[2];
        Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

        boolean status = OK;



        FileScan am = null;
//        FileScan am2 = null;
        try {
            am  = new FileScan("test.in", Stypes, Ssizes, (short)2, (short)2, Sprojection, null);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        FldSpec [] proj_list = new FldSpec[4];
        proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        proj_list[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        proj_list[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        proj_list[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);

//        AttrType [] jtype = new AttrType[4];
//        jtype[0] = new AttrType (AttrType.attrInterval);
//        jtype[1] = new AttrType(AttrType.attrString);
//        jtype[2] = new AttrType (AttrType.attrInterval);
//        jtype[3] = new AttrType(AttrType.attrString);

        TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
        NestedLoopsJoins sm =null;

        try {
            sm = new NestedLoopsJoins(Stypes, 2, Ssizes, Stypes, 2, Ssizes, 10, am, "test.in", exprInit, null, proj_list, 4);

        }
        catch (Exception e) {
            System.err.println("*** join error in SortMerge constructor ***");
            status = FAIL;
            System.err.println (""+e);
            e.printStackTrace();
        }

        if (status != OK) {
            //bail out
            System.err.println ("*** Error constructing SortMerge");
            Runtime.getRuntime().exit(1);
        }

        return sm;
    }

    // another wrapper calls this for every rule
    public static void QP3(String tagName1, String tagName2, String constraint, ArrayList<String> tags) {

        String[] tagNames2 = {tagName1, tagName2};

        List<FileScan> fileScanIterators = new ArrayList<FileScan>();


        for (String tagName: tagNames2) {
            fileScanIterators.add(tagBasedSearchReturnFileScan(tagName));
        }

//    	SortMerge sortMergeInstance = this.createCondExprQP3(tagName1, tagName2, constraint, fileScanIterators.get(0), fileScanIterators.get(1));
        NestedLoopsJoins nestedLoopInstance = createCondExprQP3(tagName1, tagName2, constraint, fileScanIterators.get(0), fileScanIterators.get(1), tags);
        nestedLoopInstanceList.add(nestedLoopInstance);
//    	sortMergeInstanceList.add(sortMergeInstance);
    }


    public static FileScan tagBasedSearchReturnFileScan(String tagnname) {

        AttrType[] Stypes = new AttrType[2];
        Stypes[0] = new AttrType(AttrType.attrInterval);
        Stypes[1] = new AttrType(AttrType.attrString);

        short[] Ssizes = new short[1];
        Ssizes[0] = 10;

        FldSpec[] sproj = new FldSpec[2];
        sproj[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        sproj[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

        FileScan tagSearchScan = null;

        try{
            CondExpr[] expr = new CondExpr[1];
            expr[0] = new CondExpr();

            expr[0].op = new AttrOperator(AttrOperator.aopEQ);

            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrString);

            expr[0].operand1 = new Operand();
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);


            expr[0].operand2 = new Operand();
            expr[0].operand2.string = tagnname;

            tagSearchScan = new FileScan("test.in", Stypes, Ssizes, (short)2, (short)2, sproj, expr);

            return tagSearchScan;
        } catch (Exception e){
            e.printStackTrace();
        }

        return null;
    }

    public void tagBasedSearch(String tagnname) {
        CondExpr[] expr = new CondExpr[4];
        expr[0] = new CondExpr();
        expr[1] = new CondExpr();
        expr[2] = new CondExpr();
        expr[3] = new CondExpr();
        String parentTagName= "Example";
        String childTagName = "Test";
        String nextTagName = "Test";


        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopGT);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
        expr[0].flag = 1;
//        expr[1] = null;

//
        // Parent Node
        expr[2].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[2].next  = null;
        expr[2].type1 = new AttrType(AttrType.attrSymbol);
        expr[2].type2 = new AttrType(AttrType.attrString);
        expr[2].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),2);
        expr[2].operand2.string = "Entry";

        // Child Node
        expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[1].next  = null;
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrString);
        expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
        expr[1].operand2.string = "Org";

//
        expr[3] = null;



        Tuple t = new Tuple();

        AttrType [] Stypes = new AttrType[2];
        Stypes[0] = new AttrType (AttrType.attrInterval);
        Stypes[1] = new AttrType (AttrType.attrString);

        //SOS
        short [] Ssizes = new short[1];
        Ssizes[0] = 10; //first elt. is 30

        FldSpec [] Sprojection = new FldSpec[2];
        Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

        FldSpec [] Rprojection = new FldSpec[2];
        Rprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        Rprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);


        CondExpr [] selects = new CondExpr [1];
        selects = null;
        boolean status = OK;



        FileScan am = null;
        FileScan am2 = null;
        try {
//

            am  = new FileScan("test.in", Stypes, Ssizes,
                    (short)2, (short)2,
                    Sprojection, null);

            am2 = new FileScan("test.in", Stypes, Ssizes,
                    (short)2, (short)2,
                    Rprojection, null);

            boolean done = false;
//


        }
        catch (Exception e) {
            e.printStackTrace();
        }

        FldSpec [] proj_list = new FldSpec[4];
        proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        proj_list[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        proj_list[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        proj_list[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
        Map<String, Integer> tagIndex = new HashMap<String, Integer>();

        //Add projection index to map
        tagIndex.put(parentTagName, 1);
        tagIndex.put(childTagName, 2);

        AttrType [] jtype = new AttrType[2];
        jtype[0] = new AttrType (AttrType.attrInterval);
        jtype[1] = new AttrType (AttrType.attrInterval);
//        jtype[0] = new AttrType (AttrType.attrString);
//        jtype[1] = new AttrType (AttrType.attrString);


        NestedLoopsJoins sm =null;
        try {
//            sm = new SortMerge(Stypes, 2, Ssizes,
//                    Stypes, 2, Ssizes,
//                    2, 5,
//                    2, 5,
//                    10,
//                    am, am,
//                    false, false, ascending,
//                    expr, proj_list, 4);
            sm = new NestedLoopsJoins(Stypes, 2, Ssizes, Stypes,2,Ssizes,10,am,
                    "test.in",expr,null,proj_list,4 );
        }
        catch (Exception e) {
            System.err.println("*** join error in NestedLoop constructor ***");
            status = FAIL;
            System.err.println (""+e);
            e.printStackTrace();
        }

        int iteasd1 = 0;
        boolean done1 = false;
        try{
            while(!done1){
                t = sm.get_next();
                if(t == null){
                    done1 = true;
                    break;
                }
                iteasd1++;
                byte[] tupleArray = t.getTupleByteArray();
                IntervalType i = t.getIntervalFld(1);
                String tagname = t.getStrFld(2);
                IntervalType j = t.getIntervalFld(3);
                String tagname2 = t.getStrFld(4);
//                IntervalType k = t.getIntervalFld(5);
//                String tagname3 = t.getStrFld(6);
                //  XMLRecord rec = new XMLRecord(t);
                // System.out.println( "Start = " + i.start + " End = " +  i.end + " Level = " + i.level + " Tagname = " + tagname + "|   Start = " + j.start + " End = " +  j.end + " Level = " + j.level + " Tagname = " + tagname2);
                // System.out.println( "|    Start = " + k.start + " End = " +  k.end + " Level = " + k.level + " Tagname = " + tagname3);
            }
        } catch(Exception e){
            e.printStackTrace();
        }


        int nextProjectionIndex;

        if(nextTagName == childTagName){
            nextProjectionIndex = tagIndex.get(childTagName);
        } else if(nextTagName == parentTagName){
            nextProjectionIndex = tagIndex.get(parentTagName);
        } else {
            nextProjectionIndex = 0;
        }
//        System.out.println("--------------------------------------------------------------------------------------------------------------");

        /*
         *
         *
         *
         * SECOND LEVEL COMPUTATION
         *
         *
         *
         *
         */

        CondExpr[] expr1 = new CondExpr[3];
        expr1[0] = new CondExpr();
        expr1[1] = new CondExpr();
        expr1[2] = new CondExpr();

        expr1[2] = null;



        expr1[0].next  = null;

        expr1[0].op    = new AttrOperator(AttrOperator.aopGT);
        expr1[0].type1 = new AttrType(AttrType.attrSymbol);
        expr1[0].type2 = new AttrType(AttrType.attrSymbol);
        expr1[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),nextProjectionIndex);
        expr1[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
        expr1[0].flag = 1;
//        expr[1] = null;

//

        // Child Node
        expr1[1].op    = new AttrOperator(AttrOperator.aopEQ);
        expr1[1].next  = null;
        expr1[1].type1 = new AttrType(AttrType.attrSymbol);
        expr1[1].type2 = new AttrType(AttrType.attrString);
        expr1[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
        expr1[1].operand2.string = "Hi";


        Tuple t1 = new Tuple();

        FldSpec [] projection = new FldSpec[2];
        projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

        status = OK;

        AttrType [] Stypes1 = new AttrType[4];
        Stypes1[0] = new AttrType (AttrType.attrInterval);
        Stypes1[1] = new AttrType (AttrType.attrString);
        Stypes1[2] = new AttrType (AttrType.attrInterval);
        Stypes1[3] = new AttrType (AttrType.attrString);

        //SOS
        short [] Ssizes1 = new short[2];
        Ssizes1[0] = 10; //first elt. is 30
        Ssizes1[1] = 10;


        FileScan am1 = null;
        try {
            am1  = new FileScan("test.in", Stypes, Ssizes,
                    (short)2, (short)2,
                    projection, null);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void parseComplexPT(String path) throws FileNotFoundException {



        //Reading complex pattern tree file
        File file = new File(path);
        Scanner scan =new Scanner(file);
        boolean issecond = false;

        String filePath = path.substring(0,path.lastIndexOf("/")+1);

        String firstPT = scan.nextLine();
        firstPT = firstPT.substring(firstPT.indexOf(": ")+2);

        String secondPT = scan.nextLine();
        String [] operation = null;
        String nextline;
        String buffer;
        if(secondPT.indexOf("ptree") != -1 ){
            secondPT = secondPT.substring(secondPT.indexOf(": ")+2);
            issecond = true;
            nextline = scan.nextLine();
            nextline = nextline.substring(nextline.indexOf(": ")+2);
            operation = nextline.split(" ");
            buffer = scan.nextLine();
            buffer = buffer.substring(buffer.indexOf(": ")+2);
        } else if(secondPT.indexOf("operation") != -1){
            // secondPT will contain operation
            nextline = secondPT.substring(secondPT.indexOf(": ")+2);
            operation = nextline.split(" ");
            buffer = scan.nextLine();
            buffer = buffer.substring(buffer.indexOf(": ")+2);
        } else if(secondPT.indexOf("buf") != -1){
            operation = new String[1];
            operation[0] = "SEL";
            buffer = secondPT.substring(secondPT.indexOf(": ")+2);
        }
//         = scan.nextLine();
//        buffer = buffer.substring(buffer.indexOf(": ")+2);


        //ArrayLists for storing tags and rules from input
        Map<String,String> tagMap = new HashMap<>();
        ArrayList<String> tags = new ArrayList<>();
        ArrayList<ArrayList<String>> rules = new ArrayList<>();

        // Scan the first pattern tree

        File filePT1 = new File(filePath+firstPT+".txt");
        Scanner scanner = new Scanner(filePT1);

        //Scan numberoftags, tags and rules from file
        int numberOfTags = scanner.nextInt();
        for(int i=0; i<numberOfTags; i++){
            String tag = scanner.next();
            if(tag.length() > 5)
                tag = tag.substring(0,5);
            tags.add(tag);

        }
        int j = 0;

        while(scanner.hasNext()){
            ArrayList<String> temp = new ArrayList<>();
            String leftTag = scanner.next();
            String rightTag = scanner.next();
            String relation = scanner.next();

            temp.add(leftTag);
            temp.add(rightTag);
            temp.add(relation);

            rules.add(temp);

        }

        //Adding to reverse tags
        ArrayList<String> reversedtags = new ArrayList<>();
        for(int i= tags.size()-1; i >=0; i--){
            reversedtags.add(tags.get(i));
        }

        //Adding reversed rules
        ArrayList<ArrayList<String>> reversedRules = new ArrayList<ArrayList<String>>();
        for(int i = rules.size()-1; i >= 0; i--) {
            reversedRules.add(rules.get(i));
        }


        ArrayList<String> tagInit = new ArrayList<>();
        for(int i =1;i<=numberOfTags;i++){
            tagInit.add(Integer.toString(i));
        }

        ArrayList<ArrayList<String>> sortedRules = XMLQueryParsing.getSortedRules(tagInit, rules);
//        //Reading complex pattern tree file
//        File file = new File(path);
//        Scanner scan =new Scanner(file);
//        boolean issecond = false;
//
//        String filePath = path.substring(0,path.lastIndexOf("/")+1);
//
//        String firstPT = scan.nextLine();
//        firstPT = firstPT.substring(firstPT.indexOf(": ")+2);
//
//        String secondPT = scan.nextLine();
//        String [] operation = null;
//        String nextline;
//        String buffer;
//        if(secondPT.indexOf("ptree") != -1 ){
//            secondPT = secondPT.substring(secondPT.indexOf(": ")+2);
//            issecond = true;
//            nextline = scan.nextLine();
//            nextline = nextline.substring(nextline.indexOf(": ")+2);
//            operation = nextline.split(" ");
//            buffer = scan.nextLine();
//            buffer = buffer.substring(buffer.indexOf(": ")+2);
//        } else if(secondPT.indexOf("operation") != -1){
//            // secondPT will contain operation
//            nextline = secondPT.substring(secondPT.indexOf(": ")+2);
//            operation = nextline.split(" ");
//            buffer = scan.nextLine();
//            buffer = buffer.substring(buffer.indexOf(": ")+2);
//        } else if(secondPT.indexOf("buf") != -1){
//            operation = new String[1];
//            operation[0] = "SEL";
//            buffer = secondPT.substring(secondPT.indexOf(": ")+2);
//        }
////         = scan.nextLine();
////        buffer = buffer.substring(buffer.indexOf(": ")+2);
//
//
//        //ArrayLists for storing tags and rules from input
//        Map<String,String> tagMap = new HashMap<>();
//        ArrayList<String> tags = new ArrayList<>();
//        ArrayList<ArrayList<String>> rules = new ArrayList<>();
//
//        // Scan the first pattern tree
//
//        File filePT1 = new File(filePath+firstPT+".txt");
//        Scanner scanner = new Scanner(filePT1);
//
//        //Scan numberoftags, tags and rules from file
//        int numberOfTags = scanner.nextInt();
//        for(int i=0; i<numberOfTags; i++){
//            String tag = scanner.next();
//            if(tag.length() > 5)
//                tag = tag.substring(0,5);
//            tags.add(tag);
//
//        }
//        int j = 0;
//
//        while(scanner.hasNext()){
//            ArrayList<String> temp = new ArrayList<>();
//            String leftTag = scanner.next();
//            String rightTag = scanner.next();
//            String relation = scanner.next();
//
//            temp.add(leftTag);
//            temp.add(rightTag);
//            temp.add(relation);
//
//            rules.add(temp);
//
//        }
//
//        //Adding to reverse tags
//        ArrayList<String> reversedtags = new ArrayList<>();
//        for(int i= tags.size()-1; i >=0; i--){
//            reversedtags.add(tags.get(i));
//        }
//
//        //Adding reversed rules
//        ArrayList<ArrayList<String>> reversedRules = new ArrayList<ArrayList<String>>();
//        for(int i = rules.size()-1; i >= 0; i--) {
//            reversedRules.add(rules.get(i));
//        }
//
//
//        ArrayList<String> tagInit = new ArrayList<>();
//        for(int i =1;i<=numberOfTags;i++){
//            tagInit.add(Integer.toString(i));
//        }
//
//        ArrayList<ArrayList<String>> sortedRules = XMLQueryParsing.getSortedRules(tagInit, rules);
//        ArrayList<ArrayList<String>> reverseSortedRules = XMLQueryParsing.getReverseSortedRules(reversedtags, reversedRules);


//        ArrayList<ArrayList<String>> sortedRuleswithTag = new ArrayList<>();
//        for(int i=0; i< sortedRules.size();i++){
//            ArrayList<String> temp = new ArrayList<>();
//            temp.add(tags.get(Integer.parseInt(sortedRules.get(i).get(0))-1));
//            temp.add(tags.get(Integer.parseInt(sortedRules.get(i).get(1))-1));
//            temp.add(sortedRules.get(i).get(2));
//
//            sortedRuleswithTag.add(temp);
//
//        }

        // Parse the second pattern tree
        ArrayList<ArrayList<String>> sortedRules2 = null;
        ArrayList<String> tags2 = null;

        if(issecond){
            //ArrayLists for storing tags and rules from input
            Map<String,String> tagMap2 = new HashMap<>();
            tags2 = new ArrayList<>();
            ArrayList<ArrayList<String>> rules2 = new ArrayList<>();

            // Scan the first pattern tree

            File filePT2 = new File(filePath+secondPT+".txt");
            Scanner scanner2 = new Scanner(filePT2);

            //Scan numberoftags, tags and rules from file
            int numberOfTags2 = scanner2.nextInt();
            for(int i=0; i<numberOfTags2; i++){
                String tag = scanner2.next();
                if(tag.length() > 5)
                    tag = tag.substring(0,5);
                tags2.add(tag);

            }

            while(scanner2.hasNext()){
                ArrayList<String> temp = new ArrayList<>();
                String leftTag = scanner2.next();
                String rightTag = scanner2.next();
                String relation = scanner2.next();

                temp.add(leftTag);
                temp.add(rightTag);
                temp.add(relation);

                rules2.add(temp);

            }

            //Adding to reverse tags
            ArrayList<String> reversedtags2 = new ArrayList<>();
            for(int i= tags2.size()-1; i >=0; i--){
                reversedtags2.add(tags2.get(i));
            }

            //Adding reversed rules
            ArrayList<ArrayList<String>> reversedRules2 = new ArrayList<ArrayList<String>>();
            for(int i = rules2.size()-1; i >= 0; i--) {
                reversedRules2.add(rules2.get(i));
            }


            ArrayList<String> tagInit2 = new ArrayList<>();
            for(int i =1;i<=numberOfTags2;i++){
                tagInit2.add(Integer.toString(i));
            }

            sortedRules2 = XMLQueryParsing.getSortedRules(tagInit2, rules2);
        }


//        Final instance = new Final();
//        instance.wrapper(tags, );

        // Final rules are in sortedRuleswithTag

//         Perform complex query processing
        try{
            switch(operation[0]){
                case "SEL":
                    scanRelation(sortedRules,tags);
                    break;
                case "CP":
                    cartesianProductJoin(sortedRules,tags,sortedRules2,tags2);
                    break;
                case "TJ":
                    tagJoin(sortedRules,tags,sortedRules2,tags2,Integer.parseInt(operation[1]),Integer.parseInt(operation[2]));
                    break;
                case "NJ":
                    nodeJoin(sortedRules,tags,sortedRules2,tags2,Integer.parseInt(operation[1]),Integer.parseInt(operation[2]));
                    break;
                case "SRT":
                    tagSort(sortedRules,tags, Integer.parseInt(operation[1])*2);
                    break;
                case "GRP":
                    tagGroup(sortedRules,tags,Integer.parseInt(operation[1])*2);
                    break;
            }
        }catch(Exception e){
            System.err.println(e);
        }
    }

    public static void scanRelation(ArrayList<ArrayList<String>> sortedRules,ArrayList<String> tags)
            throws InvalidRelation, TupleUtilsException, FileScanException, IOException, NestedLoopException{
        // Scan results returned by NLJ



        wrapper(tags, sortedRules);
        SortMerge2 result = processQuery(sortedRules, tags);


//        Display the results

        int count = 0;
        boolean done = false;
        Tuple t = new Tuple();
        try{
            while(!done && result!=null){
                t = result.get_next();
                if(t == null){
                    done = true;
                    break;
                }
                count++;
                try{

                    Map<Integer,OutputNode> map = new HashMap<>();

                    OutputNode [] node = new OutputNode[currentProjCount/2];
                    int k = 0;
                    for(int i=1;i<=currentProjCount;i+=2){
                        node[k] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),i);
                        map.put(t.getIntervalFld(i).getStart(),node[k]);
                        k++;
                    }

                    TreeMap<Integer, OutputNode> treeMap = new TreeMap<>(map);
                    treeMap.putAll(map);

                    boolean init = true;
                    OutputClass root = null;
                    Stack<OutputClass> stack = new Stack<>();

                    for (Map.Entry<Integer, OutputNode> entry : treeMap.entrySet()){

//                    Stack<OutputClass> temp ;
                        if(init){
                            root = new OutputClass(entry.getValue());
                            init = false;
                            stack.push(root);
//                        temp.push(root);
                            continue;
                        }

                        OutputNode curr = entry.getValue();
                        Stack<OutputClass> temp = new Stack<>();
                        temp.addAll(stack);

                        while (!temp.isEmpty()){
                            OutputClass parent = temp.pop(); // root
                            if(curr.isChild(parent.getNode())){
                                curr.setSpace(parent.getNode().getSpace()+2);
                                OutputClass newChild = new OutputClass(curr);
                                parent.setChild(newChild);
                                stack.push(newChild);
                                break;
                            }
                            else{
                                continue;
                            }
                        }
                    }

                    // Printing Output

                    Stack<OutputClass> printStack = new Stack<>();
                    printStack.push(root);
                    while(!printStack.isEmpty()){
                        OutputClass entry = printStack.pop();
                        OutputNode entryNode = entry.getNode();
                        for(int s=0;s<entryNode.getSpace();s++)
                            System.out.print(" ");

                        System.out.print("--" +entryNode.getTagName() + " [" + entryNode.getStartValue() + ", "+ entryNode.getEndValue() + "]") ;
                        System.out.println();
                        if(entry.children != null){
                            for(int i =0;i<entry.children.size();i++)
                                printStack.push(entry.children.get(i));
                        }
                    }
                }
                catch(Exception e){
                    e.printStackTrace();
                }


            }
        } catch(Exception e){
            e.printStackTrace();
        }

        System.out.println("\nRecords  returned by Scan: " + count);

        System.out.println("\n\n\n\nQuery Plan\n");
        System.out.println("\tPlan-- Pattern Tree-1");
        System.out.println("\t\tFor Rule 1");
        System.out.println("\t\t\tFileScan on outer with filter-left tag");
        System.out.println("\t\t\tNestedLoopJoin on relation with operation(AD/PC)");
        System.out.println("\t\t\ttag based index with filter-right tag");
        System.out.println("\t\tFor Rule 2-N");
        System.out.println("\t\t\tPrevious step's output will be new outer");
        System.out.println("\t\t\tSortmerge join with filter-right tag");
        System.out.println("\n[Plan tests TagbasedIndex, NestedLoopsJoin and SortMerge join]");
        System.out.println("\n\tPlan-- Select");
        System.out.println("\t\tIterator scan using get_next() operator");
    }

    public static void cartesianProductJoin(ArrayList<ArrayList<String>> sortedRules,ArrayList<String> tags, ArrayList<ArrayList<String>> sortedRules2,ArrayList<String> tags2)
            throws InvalidRelation, TupleUtilsException, FileScanException, IOException, NestedLoopException {

//        System.out.println(sortedRules);
//        System.out.println(sortedRules2);
        wrapper(tags, sortedRules);
        SortMerge2 result1 = processQuery(sortedRules, tags);
        int projInc1 = currentProjCount;

        // Materialize result1
        materialize(result1,projInc1,"result1");
        currentProjCount = 6;
        wrapper(tags2, sortedRules2);
        SortMerge2 result2 = processQuery(sortedRules2, tags2);

        // Materialize result2
        int projInc2 = currentProjCount;
        materialize(result2,projInc2,"result2");


        AttrType [] Stypes = new AttrType[projInc1];
        for(int iter = 0; iter<projInc1;iter=iter+2){
            Stypes[iter] = new AttrType (AttrType.attrInterval);
            Stypes[iter+1] = new AttrType (AttrType.attrString);
        }

        AttrType [] Rtypes = new AttrType[projInc2];
        for(int iter = 0; iter<projInc2;iter=iter+2){
            Rtypes[iter] = new AttrType (AttrType.attrInterval);
            Rtypes[iter+1] = new AttrType (AttrType.attrString);
        }

        int sizeVal = projInc1 / 2;
        short [] Ssizes = new short[sizeVal];
        for(int iter = 0; iter<sizeVal;iter++){
            Ssizes[iter] = 10;
        }

        int sizeVal2 = projInc2 / 2;
        short [] Rsizes = new short[sizeVal2];
        for(int iter = 0; iter<sizeVal2;iter++){
            Rsizes[iter] = 10;
        }

        FldSpec [] outProjection1 = new FldSpec[projInc1];
        for(int i=0; i<projInc1; i++) {

            outProjection1[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);

        }

        int totalProjInc = projInc1 + projInc2;

        FldSpec [] outProjection = new FldSpec[totalProjInc];
        for(int i=0; i<projInc1; i++) {
            outProjection[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);
        }
        for(int i=0; i<projInc2; i++) {

            outProjection[projInc1 + i] = new FldSpec(new RelSpec(RelSpec.innerRel),i+1);

        }

        /*
         * Set up FileScan for one relation
         */
        FileScan scan = new FileScan("result1", Stypes, Ssizes, (short)projInc1,(short) projInc1, outProjection1, null);


        /*
         * Set up NLJ
         */
        NestedLoopsJoins nlj = new NestedLoopsJoins(Stypes, projInc1, Ssizes, Rtypes, projInc2, Rsizes, 10, scan, "result2", null, null, outProjection, totalProjInc);


        System.out.println("\n\nCartesian Product Results:");
        int count = -1;
        boolean done = false;
        try {

            while(!done) {
                count++;
                Tuple t = nlj.get_next();
                if(t == null) {
                    done = true;
                    break;
                }


                // Store the left side results
                Map<Integer,OutputNode> map1 = new HashMap<>();

                OutputNode [] node1 = new OutputNode[projInc1/2];
                int k = 0;
                for(int i=1;i<=projInc1;i+=2){
                    node1[k] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),1);
                    map1.put(t.getIntervalFld(i).getStart(),node1[k]);
                    k++;
                }

                TreeMap<Integer, OutputNode> treeMap1 = new TreeMap<>(map1);
                treeMap1.putAll(map1);

                boolean init = true;
                OutputClass root1 = null;
                Stack<OutputClass> stack1 = new Stack<>();

                for (Map.Entry<Integer, OutputNode> entry : treeMap1.entrySet()){

//                    Stack<OutputClass> temp ;
                    if(init){
                        root1 = new OutputClass(entry.getValue());
                        init = false;
                        stack1.push(root1);
//                        temp.push(root);
                        continue;
                    }

                    OutputNode curr = entry.getValue();
                    Stack<OutputClass> temp = new Stack<>();
                    temp.addAll(stack1);

                    while (!temp.isEmpty()){
                        OutputClass parent = temp.pop(); // root
                        if(curr.isChild(parent.getNode())){
                            curr.setSpace(parent.getNode().getSpace()+2);
                            OutputClass newChild = new OutputClass(curr);
                            parent.setChild(newChild);
                            stack1.push(newChild);
                            break;
                        }
                        else{
                            continue;
                        }
                    }
                }

                // Store right side results


                Map<Integer,OutputNode> map2 = new HashMap<>();

                OutputNode [] node2 = new OutputNode[projInc2/2];
                int p=0;
                for(int i=projInc1+1;i<=projInc2+projInc1;i+=2){
                    node2[p] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),1);
                    map2.put(t.getIntervalFld(i).getStart(),node2[p]);
                    p++;
                }

                TreeMap<Integer, OutputNode> treeMap2 = new TreeMap<>(map2);
                treeMap2.putAll(map2);

                boolean init2 = true;
                OutputClass root2 = null;
                Stack<OutputClass> stack2 = new Stack<>();

                for (Map.Entry<Integer, OutputNode> entry : treeMap2.entrySet()){

//                    Stack<OutputClass> temp ;
                    if(init2){
                        root2 = new OutputClass(entry.getValue());
                        init2 = false;
                        stack2.push(root2);
//                        temp.push(root);
                        continue;
                    }

                    OutputNode curr = entry.getValue();
                    Stack<OutputClass> temp = new Stack<>();
                    temp.addAll(stack2);

                    while (!temp.isEmpty()){
                        OutputClass parent = temp.pop(); // root
                        if(curr.isChild(parent.getNode())){
                            curr.setSpace(parent.getNode().getSpace()+2);
                            OutputClass newChild = new OutputClass(curr);
                            parent.setChild(newChild);
                            stack2.push(newChild);
                            break;
                        }
                        else{
                            continue;
                        }
                    }
                }

                // Printing Output of cartesian product

                Stack<OutputClass> printStack = new Stack<>();

                System.out.println("\n--CP_root");
                printStack.push(root1);

                // Print left subtree
                while(!printStack.isEmpty()){
                    OutputClass entry = printStack.pop();
                    OutputNode entryNode = entry.getNode();
                    for(int s=0;s<entryNode.getSpace();s++)
                        System.out.print(" ");

                    System.out.print("--" +entryNode.getTagName() + " [" + entryNode.getStartValue() + ", "+ entryNode.getEndValue() + "]") ;
                    System.out.println();
                    if(entry.children != null){
                        for(int i =0;i<entry.children.size();i++)
                            printStack.push(entry.children.get(i));
                    }
                }

                // Print right subtree
                printStack.push(root2);
                while(!printStack.isEmpty()){
                    OutputClass entry = printStack.pop();
                    OutputNode entryNode = entry.getNode();
                    for(int s=0;s<entryNode.getSpace();s++)
                        System.out.print(" ");

                    System.out.print("--" +entryNode.getTagName() + " [" + entryNode.getStartValue() + ", "+ entryNode.getEndValue() + "]") ;
                    System.out.println();
                    if(entry.children != null){
                        for(int i =0;i<entry.children.size();i++)
                            printStack.push(entry.children.get(i));
                    }
                }
            }
        }catch(Exception e) {
            e.printStackTrace();
        }

        System.out.println("Records returned by Cartesian Product = " + count);

        System.out.println("\n\n\n\nQuery Plan\n");
        System.out.println("\tPlan-- Pattern Tree-1");
        System.out.println("\t\tFor Rule 1");
        System.out.println("\t\t\tFileScan on outer with filter-left tag");
        System.out.println("\t\t\tNestedLoopJoin on relation with operation(AD/PC)");
        System.out.println("\t\t\ttag based index with filter-right tag");
        System.out.println("\t\tFor Rule 2-N");
        System.out.println("\t\t\tPrevious step's output will be new outer");
        System.out.println("\t\t\tSortmerge join with filter-right tag");
        System.out.println("\n[Plan tests TagbasedIndex, NestedLoopsJoin and SortMerge join]");
        System.out.println("\n\tPlan-- CP");
        System.out.println("\t\tNestedLoopJoin on witness trees of Pattern Tree-1 and Pattern Tree-2");
        System.out.println("\t\tConstruct OutputTrees of the join results");

    }

    public static void materialize(SortMerge2 nlj, int size, String filename){
        int count = 0;
        boolean done = false;
        Tuple t = new Tuple();
        try{
            while(!done && nlj!=null){
                t = nlj.get_next();
                if(t == null){
                    done = true;
                    break;
                }
                count++;


                Heapfile f = new Heapfile(filename);


                try {
                    f.insertRecord(t.returnTupleByteArray());
                } catch(Exception e) {
                    e.printStackTrace();
                }
//                System.out.println("Result " + (count) + ":");
//                System.out.print("\t");
//                for(int k=1;k<=size;k++){
//                    if(k%2 != 0)
//                        System.out.print(" | Start = " + t.getIntervalFld(k).getStart() + " End = " + t.getIntervalFld(k).getEnd() + " Level = " + t.getIntervalFld(k).getLevel());
//                    else
//                        System.out.print(" TagName = " + t.getStrFld(k) + " |");
//                }
//                System.out.println();


            }
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void tagJoin(ArrayList<ArrayList<String>> sortedRules,ArrayList<String> tags, ArrayList<ArrayList<String>> sortedRules2,ArrayList<String> tags2, int left, int right)
            throws InvalidRelation, TupleUtilsException, FileScanException, IOException, NestedLoopException {
//
//        System.out.println(sortedRules);
//        System.out.println(sortedRules2);
        wrapper(tags, sortedRules);
        SortMerge2 result1 = processQuery(sortedRules, tags);
        int projInc1 = currentProjCount;

        // Materialize result1
        materialize(result1,projInc1,"result1");
        currentProjCount = 4;
        wrapper(tags2, sortedRules2);
        SortMerge2 result2 = processQuery(sortedRules2, tags2);

        // Materialize result2
        int projInc2 = currentProjCount;
        materialize(result2,projInc2,"result2");


        AttrType [] Stypes = new AttrType[projInc1];
        for(int iter = 0; iter<projInc1;iter=iter+2){
            Stypes[iter] = new AttrType (AttrType.attrInterval);
            Stypes[iter+1] = new AttrType (AttrType.attrString);
        }

        AttrType [] Rtypes = new AttrType[projInc2];
        for(int iter = 0; iter<projInc2;iter=iter+2){
            Rtypes[iter] = new AttrType (AttrType.attrInterval);
            Rtypes[iter+1] = new AttrType (AttrType.attrString);
        }

        int sizeVal = projInc1 / 2;
        short [] Ssizes = new short[sizeVal];
        for(int iter = 0; iter<sizeVal;iter++){
            Ssizes[iter] = 10;
        }

        int sizeVal2 = projInc2 / 2;
        short [] Rsizes = new short[sizeVal2];
        for(int iter = 0; iter<sizeVal2;iter++){
            Rsizes[iter] = 10;
        }

        FldSpec [] outProjection1 = new FldSpec[projInc1];
        for(int i=0; i<projInc1; i++) {

            outProjection1[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);

        }
        int totalProjInc = projInc1 + projInc2;

        FldSpec [] outProjection = new FldSpec[totalProjInc];
        for(int i=0; i<projInc1; i++) {
            outProjection[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);
        }
        for(int i=0; i<projInc2; i++) {

            outProjection[projInc1 + i] = new FldSpec(new RelSpec(RelSpec.innerRel),i+1);

        }
        // Set up conditional expression for tag join

        CondExpr[] expr = new CondExpr[2];
        expr[0] = new CondExpr();
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),right*2);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), left*2);
        expr[0].next  = null;
        expr[1] = null;

        /*
         * Set up FileScan for one relation
         */
        FileScan scan = new FileScan("result1", Stypes, Ssizes, (short)projInc1,(short) projInc1, outProjection1, null);


        /*
         * Set up NLJ
         */
        NestedLoopsJoins nlj = new NestedLoopsJoins(Stypes, projInc1, Ssizes, Rtypes, projInc2, Rsizes, 10, scan, "result2", expr, null, outProjection, totalProjInc);


        System.out.println("\n\nTag Join Results:");
        int count = -1;
        String current = null;
        boolean done = false;
        try {

            while(!done) {
                count++;
                Tuple t = nlj.get_next();
                if(t == null) {
                    done = true;
                    break;
                }


                // Store the left side results
                Map<Integer,OutputNode> map1 = new HashMap<>();

                OutputNode [] node1 = new OutputNode[projInc1/2];
                int k = 0;
                for(int i=1;i<=projInc1;i+=2){
                    node1[k] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),1);
                    map1.put(t.getIntervalFld(i).getStart(),node1[k]);
                    current = t.getStrFld(left*2);
                    k++;
                }

                TreeMap<Integer, OutputNode> treeMap1 = new TreeMap<>(map1);
                treeMap1.putAll(map1);

                boolean init = true;
                OutputClass root1 = null;
                Stack<OutputClass> stack1 = new Stack<>();

                for (Map.Entry<Integer, OutputNode> entry : treeMap1.entrySet()){

//                    Stack<OutputClass> temp ;
                    if(init){
                        root1 = new OutputClass(entry.getValue());
                        init = false;
                        stack1.push(root1);
//                        temp.push(root);
                        continue;
                    }

                    OutputNode curr = entry.getValue();
                    Stack<OutputClass> temp = new Stack<>();
                    temp.addAll(stack1);

                    while (!temp.isEmpty()){
                        OutputClass parent = temp.pop(); // root
                        if(curr.isChild(parent.getNode())){
                            curr.setSpace(parent.getNode().getSpace()+2);
                            OutputClass newChild = new OutputClass(curr);
                            parent.setChild(newChild);
                            stack1.push(newChild);
                            break;
                        }
                        else{
                            continue;
                        }
                    }
                }

                // Store right side results


                Map<Integer,OutputNode> map2 = new HashMap<>();

                OutputNode [] node2 = new OutputNode[projInc2/2];
                int p=0;
                for(int i=projInc1+1;i<=projInc2+projInc1;i+=2){
                    node2[p] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),1);
                    map2.put(t.getIntervalFld(i).getStart(),node2[p]);
                    p++;
                }

                TreeMap<Integer, OutputNode> treeMap2 = new TreeMap<>(map2);
                treeMap2.putAll(map2);

                boolean init2 = true;
                OutputClass root2 = null;
                Stack<OutputClass> stack2 = new Stack<>();

                for (Map.Entry<Integer, OutputNode> entry : treeMap2.entrySet()){

//                    Stack<OutputClass> temp ;
                    if(init2){
                        root2 = new OutputClass(entry.getValue());
                        init2 = false;
                        stack2.push(root2);
//                        temp.push(root);
                        continue;
                    }

                    OutputNode curr = entry.getValue();
                    Stack<OutputClass> temp = new Stack<>();
                    temp.addAll(stack2);

                    while (!temp.isEmpty()){
                        OutputClass parent = temp.pop(); // root
                        if(curr.isChild(parent.getNode())){
                            curr.setSpace(parent.getNode().getSpace()+2);
                            OutputClass newChild = new OutputClass(curr);
                            parent.setChild(newChild);
                            stack2.push(newChild);
                            break;
                        }
                        else{
                            continue;
                        }
                    }
                }

                // Printing Output of cartesian product

                Stack<OutputClass> printStack = new Stack<>();

                System.out.println("\n--TJ_root_"+current);
                printStack.push(root1);

                // Print left subtree
                while(!printStack.isEmpty()){
                    OutputClass entry = printStack.pop();
                    OutputNode entryNode = entry.getNode();
                    for(int s=0;s<entryNode.getSpace();s++)
                        System.out.print(" ");

                    System.out.print("--" +entryNode.getTagName() + " [" + entryNode.getStartValue() + ", "+ entryNode.getEndValue() + "]") ;
                    System.out.println();
                    if(entry.children != null){
                        for(int i =0;i<entry.children.size();i++)
                            printStack.push(entry.children.get(i));
                    }
                }

                // Print right subtree
                printStack.push(root2);
                while(!printStack.isEmpty()){
                    OutputClass entry = printStack.pop();
                    OutputNode entryNode = entry.getNode();
                    for(int s=0;s<entryNode.getSpace();s++)
                        System.out.print(" ");

                    System.out.print("--" +entryNode.getTagName() + " [" + entryNode.getStartValue() + ", "+ entryNode.getEndValue() + "]") ;
                    System.out.println();
                    if(entry.children != null){
                        for(int i =0;i<entry.children.size();i++)
                            printStack.push(entry.children.get(i));
                    }
                }
            }
        }catch(Exception e) {
            e.printStackTrace();
        }

        System.out.println("Records returned by Tag Join = " + count);

        System.out.println("\n\n\n\nQuery Plan\n");
        System.out.println("\tPlan-- Pattern Tree-1");
        System.out.println("\t\tFor Rule 1");
        System.out.println("\t\t\tFileScan on outer with filter-left tag");
        System.out.println("\t\t\tNestedLoopJoin on relation with operation(AD/PC)");
        System.out.println("\t\t\ttag based index with filter-right tag");
        System.out.println("\t\tFor Rule 2-N");
        System.out.println("\t\t\tPrevious step's output will be new outer");
        System.out.println("\t\t\tSortmerge join with filter-right tag");
        System.out.println("\n[Plan tests TagbasedIndex, NestedLoopsJoin and SortMerge join]");
        System.out.println("\n\tPlan-- TJ i j");
        System.out.println("\t\tNestedLoopJoin on witness trees of Pattern Tree-1 and Pattern Tree-2 on tags of PT1.i = PT2.j");
        System.out.println("\t\tConstruct OutputTrees of the join results");

    }

    public static void nodeJoin(ArrayList<ArrayList<String>> sortedRules,ArrayList<String> tags, ArrayList<ArrayList<String>> sortedRules2,ArrayList<String> tags2, int left, int right)
            throws InvalidRelation, TupleUtilsException, FileScanException, IOException, NestedLoopException {

//        System.out.println(sortedRules);
//        System.out.println(sortedRules2);
        wrapper(tags, sortedRules);
        SortMerge2 result1 = processQuery(sortedRules, tags);
        int projInc1 = currentProjCount;

        // Materialize result1
        materialize(result1,projInc1,"result1");
        currentProjCount = 4;
        wrapper(tags2, sortedRules2);
        SortMerge2 result2 = processQuery(sortedRules2, tags2);

        // Materialize result2
        int projInc2 = currentProjCount;
        materialize(result2,projInc2,"result2");


        AttrType [] Stypes = new AttrType[projInc1];
        for(int iter = 0; iter<projInc1;iter=iter+2){
            Stypes[iter] = new AttrType (AttrType.attrInterval);
            Stypes[iter+1] = new AttrType (AttrType.attrString);
        }

        AttrType [] Rtypes = new AttrType[projInc2];
        for(int iter = 0; iter<projInc2;iter=iter+2){
            Rtypes[iter] = new AttrType (AttrType.attrInterval);
            Rtypes[iter+1] = new AttrType (AttrType.attrString);
        }

        int sizeVal = projInc1/2;
        short [] Ssizes = new short[sizeVal];
        for(int iter = 0; iter<sizeVal;iter++){
            Ssizes[iter] = 10;
        }

        int sizeVal2 = projInc2/2;
        short [] Rsizes = new short[sizeVal2];
        for(int iter = 0; iter<sizeVal2;iter++){
            Rsizes[iter] = 10;
        }

        FldSpec [] outProjection1 = new FldSpec[projInc1];
        for(int i=0; i<projInc1; i++) {

            outProjection1[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);

        }

        int totalProjInc = projInc1 + projInc2;

        FldSpec [] outProjection = new FldSpec[totalProjInc];
        for(int i=0; i<projInc1; i++) {
            outProjection[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);
        }
        for(int i=0; i<projInc2; i++) {

            outProjection[projInc1 + i] = new FldSpec(new RelSpec(RelSpec.innerRel),i+1);

        }


        // Set up conditional expression for tag join

        CondExpr[] expr = new CondExpr[2];
        expr[0] = new CondExpr();
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),(right*2)-1);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer),(left*2)-1);
        expr[0].next  = null;
        expr[0].flag = 1;
        expr[1] = null;

//        CondExpr[] expr = new CondExpr[2];
//        expr[0] = new CondExpr();
//        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
//        expr[0].type1 = new AttrType(AttrType.attrSymbol);
//        expr[0].type2 = new AttrType(AttrType.attrSymbol);
//        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),leftAttrribute);
//        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer),rightAttribute);
//        expr[0].next  = null;
//        expr[0].flag = 1;
//        expr[1] = null;

        /*
         * Set up FileScan for one relation
         */
        FileScan scan = new FileScan("result1", Stypes, Ssizes, (short)projInc1,(short) projInc1, outProjection1, null);


        /*
         * Set up NLJ
         */
        NestedLoopsJoins nlj = new NestedLoopsJoins(Stypes, projInc1, Ssizes, Rtypes, projInc2, Rsizes, 10, scan, "result2", expr, null, outProjection, totalProjInc);

        System.out.println("\n\nNode Join Results:");
        int count = -1;
        IntervalType current = null;
        boolean done = false;
        try {

            while(!done) {
                count++;
                Tuple t = nlj.get_next();
                if(t == null) {
                    done = true;
                    break;
                }

                // Store the left side results
                Map<Integer,OutputNode> map1 = new HashMap<>();

                OutputNode [] node1 = new OutputNode[projInc1/2];
                int k = 0;
                for(int i=1;i<=projInc1;i+=2){
                    node1[k] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),1);
                    map1.put(t.getIntervalFld(i).getStart(),node1[k]);
                    current = t.getIntervalFld((left*2)-1);
                    k++;
                }

                TreeMap<Integer, OutputNode> treeMap1 = new TreeMap<>(map1);
                treeMap1.putAll(map1);

                boolean init = true;
                OutputClass root1 = null;
                Stack<OutputClass> stack1 = new Stack<>();

                for (Map.Entry<Integer, OutputNode> entry : treeMap1.entrySet()){

//                    Stack<OutputClass> temp ;
                    if(init){
                        root1 = new OutputClass(entry.getValue());
                        init = false;
                        stack1.push(root1);
//                        temp.push(root);
                        continue;
                    }

                    OutputNode curr = entry.getValue();
                    Stack<OutputClass> temp = new Stack<>();
                    temp.addAll(stack1);

                    while (!temp.isEmpty()){
                        OutputClass parent = temp.pop(); // root
                        if(curr.isChild(parent.getNode())){
                            curr.setSpace(parent.getNode().getSpace()+2);
                            OutputClass newChild = new OutputClass(curr);
                            parent.setChild(newChild);
                            stack1.push(newChild);
                            break;
                        }
                        else{
                            continue;
                        }
                    }
                }

                // Store right side results


                Map<Integer,OutputNode> map2 = new HashMap<>();

                OutputNode [] node2 = new OutputNode[projInc2/2];
                int p=0;
                for(int i=projInc1+1;i<=projInc2+projInc1;i+=2){
                    node2[p] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),1);
                    map2.put(t.getIntervalFld(i).getStart(),node2[p]);
                    p++;
                }

                TreeMap<Integer, OutputNode> treeMap2 = new TreeMap<>(map2);
                treeMap2.putAll(map2);

                boolean init2 = true;
                OutputClass root2 = null;
                Stack<OutputClass> stack2 = new Stack<>();

                for (Map.Entry<Integer, OutputNode> entry : treeMap2.entrySet()){

//                    Stack<OutputClass> temp ;
                    if(init2){
                        root2 = new OutputClass(entry.getValue());
                        init2 = false;
                        stack2.push(root2);
//                        temp.push(root);
                        continue;
                    }

                    OutputNode curr = entry.getValue();
                    Stack<OutputClass> temp = new Stack<>();
                    temp.addAll(stack2);

                    while (!temp.isEmpty()){
                        OutputClass parent = temp.pop(); // root
                        if(curr.isChild(parent.getNode())){
                            curr.setSpace(parent.getNode().getSpace()+2);
                            OutputClass newChild = new OutputClass(curr);
                            parent.setChild(newChild);
                            stack2.push(newChild);
                            break;
                        }
                        else{
                            continue;
                        }
                    }
                }

                // Printing Output of cartesian product

                Stack<OutputClass> printStack = new Stack<>();

                System.out.println("\n--NJ_root_["+current.getStart()+","+current.getEnd()+"]");
                printStack.push(root1);

                // Print left subtree
                while(!printStack.isEmpty()){
                    OutputClass entry = printStack.pop();
                    OutputNode entryNode = entry.getNode();
                    for(int s=0;s<entryNode.getSpace();s++)
                        System.out.print(" ");

                    System.out.print("--" +entryNode.getTagName() + " [" + entryNode.getStartValue() + ", "+ entryNode.getEndValue() + "]") ;
                    System.out.println();
                    if(entry.children != null){
                        for(int i =0;i<entry.children.size();i++)
                            printStack.push(entry.children.get(i));
                    }
                }

                // Print right subtree
                printStack.push(root2);
                while(!printStack.isEmpty()){
                    OutputClass entry = printStack.pop();
                    OutputNode entryNode = entry.getNode();
                    for(int s=0;s<entryNode.getSpace();s++)
                        System.out.print(" ");

                    System.out.print("--" +entryNode.getTagName() + " [" + entryNode.getStartValue() + ", "+ entryNode.getEndValue() + "]") ;
                    System.out.println();
                    if(entry.children != null){
                        for(int i =0;i<entry.children.size();i++)
                            printStack.push(entry.children.get(i));
                    }
                }
            }
        }catch(Exception e) {
            e.printStackTrace();
        }

        System.out.println("Records returned by Node Join = " + count);

        System.out.println("\n\n\n\nQuery Plan\n");
        System.out.println("\tPlan-- Pattern Tree-1");
        System.out.println("\t\tFor Rule 1");
        System.out.println("\t\t\tFileScan on outer with filter-left tag");
        System.out.println("\t\t\tNestedLoopJoin on relation with operation(AD/PC)");
        System.out.println("\t\t\ttag based index with filter-right tag");
        System.out.println("\t\tFor Rule 2-N");
        System.out.println("\t\t\tPrevious step's output will be new outer");
        System.out.println("\t\t\tSortmerge join with filter-right tag");
        System.out.println("\n[Plan tests TagbasedIndex, NestedLoopsJoin and SortMerge join]");
        System.out.println("\n\tPlan-- NJ i j");
        System.out.println("\t\tNestedLoopJoin on witness trees of Pattern Tree-1 and Pattern Tree-2 on IDs of PT1.i = PT2.j");
        System.out.println("\t\tConstruct OutputTrees of the join results");

    }


    public static void tagSort(ArrayList<ArrayList<String>> sortedRules,ArrayList<String> tags,int sortCol ){

        int count = 0;
        boolean done = false;
        Tuple t = new Tuple();
        boolean status = OK;
        wrapper(tags, sortedRules);
        SortMerge2 result = processQuery(sortedRules, tags);

//        Display the results

//        int iteasd = 0;
//        try{
//            while(!done && result!=null){
//                t = result.get_next();
//                if(t == null){
//                    done = true;
//                    break;
//                }
//                iteasd++;
//                byte[] tupleArray = t.getTupleByteArray();
//
//                for(int k=1;k<=projInc;k++){
//                    if(k%2 != 0)
//                        System.out.print(" | Start = " + t.getIntervalFld(k).getStart() + " End = " + t.getIntervalFld(k).getEnd() + " Level = " + t.getIntervalFld(k).getLevel());
//                    else
//                        System.out.print(" TagName = " + t.getStrFld(k) );
//                }
//                System.out.println("\n");
//
//
//            }
//        } catch(Exception e){
//            e.printStackTrace();
//        }
//
//        System.out.println("\nRecords  returned by Nested Loop: " + iteasd);
//        int sortCol = 8;
//        System.out.println(currentProjCount);


        AttrType [] Stypes = new AttrType[currentProjCount];
        for(int i=0;i<currentProjCount;i+=2){
            Stypes[i] = new AttrType (AttrType.attrInterval);
            Stypes[i+1] = new AttrType (AttrType.attrString);
        }

        short [] Ssizes = new short[currentProjCount/2];
        for(int i=0;i<currentProjCount/2;i++){
            Ssizes[i] = 10;
        }

        FldSpec [] outProjectionSort = new FldSpec[currentProjCount];
        for(int i=0;i<currentProjCount;i++){
            outProjectionSort[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);
        }

        FldSpec[] projlist = new FldSpec[currentProjCount];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for(int i=0;i<currentProjCount;i++){
            projlist[i] = new FldSpec(rel, i+1);
        }

        TupleOrder[] order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);

        FileScan fscan = null;

        // try {
//            fscan = new FileScan("sortTemp.in", Stypes, Ssizes, (short) 8, 8, projlist, null);
//        }
//        catch (Exception e) {
//            status = FAIL;
//            e.printStackTrace();
//        }

        Sort sort = null;
        try {
            sort = new Sort(Stypes, (short) currentProjCount, Ssizes, result, sortCol, order[0], 10, 1000);
//            sort = new Sort(Stypes, (short) 8, Ssizes, result, 1, order[0], 5, 1000);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        count = 0;
        t = null;
        String outval = null;

        try {
            t = sort.get_next();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        boolean flag = true;

        // Display the sorted output in a tree order
        while (t != null){
            try{

                Map<Integer,OutputNode> map = new HashMap<>();

                OutputNode [] node = new OutputNode[currentProjCount/2];
                int k = 0;
                for(int i=1;i<=currentProjCount;i+=2){
                    node[k] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),i);
                    map.put(t.getIntervalFld(i).getStart(),node[k]);
                    k++;
                }

                TreeMap<Integer, OutputNode> treeMap = new TreeMap<>(map);
                treeMap.putAll(map);

                boolean init = true;
                OutputClass root = null;
                Stack<OutputClass> stack = new Stack<>();

                for (Map.Entry<Integer, OutputNode> entry : treeMap.entrySet()){

//                    Stack<OutputClass> temp ;
                    if(init){
                        root = new OutputClass(entry.getValue());
                        init = false;
                        stack.push(root);
//                        temp.push(root);
                        continue;
                    }

                    OutputNode curr = entry.getValue();
                    Stack<OutputClass> temp = new Stack<>();
                    temp.addAll(stack);

                    while (!temp.isEmpty()){
                        OutputClass parent = temp.pop(); // root
                        if(curr.isChild(parent.getNode())){
                            curr.setSpace(parent.getNode().getSpace()+2);
                            OutputClass newChild = new OutputClass(curr);
                            parent.setChild(newChild);
                            stack.push(newChild);
                            break;
                        }
                        else{
                            continue;
                        }
                    }
                }

                // Printing Output

                Stack<OutputClass> printStack = new Stack<>();
                printStack.push(root);
                while(!printStack.isEmpty()){
                    OutputClass entry = printStack.pop();
                    OutputNode entryNode = entry.getNode();
                    for(int s=0;s<entryNode.getSpace();s++)
                        System.out.print(" ");

                    System.out.print("--" +entryNode.getTagName() + " [" + entryNode.getStartValue() + ", "+ entryNode.getEndValue() + "]") ;
                    System.out.println();
                    if(entry.children != null){
                        for(int i =0;i<entry.children.size();i++)
                            printStack.push(entry.children.get(i));
                    }
                }
            }
            catch(Exception e){
                e.printStackTrace();
            }
            count++;
            try {
                t = sort.get_next();
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
            System.out.println();
        }

        System.out.println("\n\n\n\nQuery Plan\n");
        System.out.println("\tPlan-- Pattern Tree-1");
        System.out.println("\t\tFor Rule 1");
        System.out.println("\t\t\tFileScan on outer with filter-left tag");
        System.out.println("\t\t\tNestedLoopJoin on relation with operation(AD/PC)");
        System.out.println("\t\t\ttag based index with filter-right tag");
        System.out.println("\t\tFor Rule 2-N");
        System.out.println("\t\t\tPrevious step's output will be new outer");
        System.out.println("\t\t\tSortmerge join with filter-right tag");
        System.out.println("\n[Plan tests TagbasedIndex, NestedLoopsJoin and SortMerge join]");
        System.out.println("\n\tPlan-- SRT i");
        System.out.println("\t\tSort the witness trees on tag of i'th node");
        System.out.println("\t\tConstruct OutputTrees of the join results");
    }

    public static void tagGroup(ArrayList<ArrayList<String>> sortedRules,ArrayList<String> tagMap, int groupCol){

        int count = 0;
        boolean done = false;
        Tuple t = new Tuple();
        boolean status = OK;
        wrapper(tagMap, sortedRules);
        SortMerge2 result = processQuery(sortedRules, tagMap);
//        int groupCol = 8;

        AttrType [] Stypes = new AttrType[currentProjCount];
        for(int i=0;i<currentProjCount;i+=2){
            Stypes[i] = new AttrType (AttrType.attrInterval);
            Stypes[i+1] = new AttrType (AttrType.attrString);
        }

        short [] Ssizes = new short[currentProjCount/2];
        for(int i=0;i<currentProjCount/2;i++){
            Ssizes[i] = 10;
        }

        FldSpec [] outProjectionSort = new FldSpec[currentProjCount];
        for(int i=0;i<currentProjCount;i++){
            outProjectionSort[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);
        }

        FldSpec[] projlist = new FldSpec[currentProjCount];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for(int i=0;i<currentProjCount;i++){
            projlist[i] = new FldSpec(rel, i+1);
        }

        TupleOrder[] order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);

        FileScan fscan = null;

        // try {
//            fscan = new FileScan("sortTemp.in", Stypes, Ssizes, (short) 8, 8, projlist, null);
//        }
//        catch (Exception e) {
//            status = FAIL;
//            e.printStackTrace();
//        }

        Sort sort = null;
        try {
            sort = new Sort(Stypes, (short) currentProjCount, Ssizes, result, groupCol, order[0], 10, 1000);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        count = 0;
        t = null;
        String outval = null;

        try {
            t = sort.get_next();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        boolean flag = true;
        int currGroup = 1;
        String groupElem = null;
        String current = null;

        ArrayList<OutputClass> members = null;
        // Display the sorted output in a tree order
        while (t != null){
            try{

                Map<Integer,OutputNode> map = new HashMap<>();

                OutputNode [] node = new OutputNode[currentProjCount/2];
//                for(int i=1;i<=projInc;i+=2){
//                    node[i] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),i);
//                    map.put(t.getIntervalFld(i).getStart(),node[i]);
//                    current = t.getStrFld(groupCol);
//                }

                int k = 0;
                for(int i=1;i<=currentProjCount;i+=2){
//                    node[k] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),i+1);
                    node[k] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),2);
                    map.put(t.getIntervalFld(i).getStart(),node[k]);
                    current = t.getStrFld(groupCol);
                    k++;
                }

                TreeMap<Integer, OutputNode> treeMap = new TreeMap<>(map);
                treeMap.putAll(map);

                boolean init = true;
                OutputClass root = null;
                Stack<OutputClass> stack = new Stack<>();

                for (Map.Entry<Integer, OutputNode> entry : treeMap.entrySet()){

//                    Stack<OutputClass> temp ;
                    if(init){
                        root = new OutputClass(entry.getValue());
                        init = false;
                        stack.push(root);
//                        temp.push(root);
                        continue;
                    }

                    OutputNode curr = entry.getValue();
                    Stack<OutputClass> temp = new Stack<>();
                    temp.addAll(stack);

                    while (!temp.isEmpty()){
                        OutputClass parent = temp.pop(); // root
                        if(curr.isChild(parent.getNode())){
                            curr.setSpace(parent.getNode().getSpace()+2);
                            OutputClass newChild = new OutputClass(curr);
                            parent.setChild(newChild);
                            stack.push(newChild);
                            break;
                        }
                        else{
                            continue;
                        }
                    }
                }

                if(groupElem == null){
                    members = new ArrayList();
                    groupElem = current;
                    members.add(root);
                }else if(groupElem.equals(current)){
                    members.add(root);
                }else if(!groupElem.equals(current)){
                    // make changes here and display
                    if(groupElem!= null){
                        displayGroup(members,currGroup,groupElem);
                    }
                    currGroup += 1;
                    members = new ArrayList();
                    members.add(root);
                    groupElem = current;
                }

            }
            catch(Exception e){
                e.printStackTrace();
            }
            count++;
            try {
                t = sort.get_next();
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
            System.out.println();
        }
        displayGroup(members,currGroup,groupElem);

        System.out.println("\n\n\n\nQuery Plan\n");
        System.out.println("\tPlan-- Pattern Tree-1");
        System.out.println("\t\tFor Rule 1");
        System.out.println("\t\t\tFileScan on outer with filter-left tag");
        System.out.println("\t\t\tNestedLoopJoin on relation with operation(AD/PC)");
        System.out.println("\t\t\ttag based index with filter-right tag");
        System.out.println("\t\tFor Rule 2-N");
        System.out.println("\t\t\tPrevious step's output will be new outer");
        System.out.println("\t\t\tSortmerge join with filter-right tag");
        System.out.println("\n[Plan tests TagbasedIndex, NestedLoopsJoin and SortMerge join]");
        System.out.println("\n\tPlan-- GRP i");
        System.out.println("\t\tSort the witness trees on tag of i'th node");
        System.out.println("\t\tGroup the witness trees under the same root on tag of i'th node");
        System.out.println("\t\tConstruct OutputTrees of the join results");
    }

    public static void displayGroup(ArrayList<OutputClass> group,int groupInd,String groupVal){

        System.out.println("group_"+groupInd+"_root");
        System.out.println(" --"+"grouping_list"); // set the spaces correctly
        System.out.println("    --"+groupVal); // set the spaces correctly
        System.out.println(" --"+"group_subroot"); // set the spaces correctly


        for(int i = 0;i<group.size();i++){
            Stack<OutputClass> printStack = new Stack<>();
            printStack.push(group.get(i));
            while(!printStack.isEmpty()){
                OutputClass entry = printStack.pop();
                OutputNode entryNode = entry.getNode();
                for(int s=0;s<entryNode.getSpace();s++)
                    System.out.print(" ");

                System.out.print("--" +entryNode.getTagName() + " [" + entryNode.getStartValue() + ", "+ entryNode.getEndValue() + "]") ;
                System.out.println();
                if(entry.children != null){
                    for(int p =0;p<entry.children.size();p++)
                        printStack.push(entry.children.get(p));
                }
            }
        }
    }



    public static void main(String argv[]) throws ParserConfigurationException, IOException, SAXException, TupleUtilsException, FileScanException, InvalidRelation {

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setValidating(false);
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(new FileInputStream(new File("/home/user/Desktop/AfterChanges/dbmsiPhase2/javaminibase/src/xmldbTestXML/sample_data.xml")));

        Node root = doc.getDocumentElement();

        xmlParser.build(root);

        // xmlParser.BFS();

        xmlParser.preOrder(xmlParser.tree.root);
//        System.out.println("---------------------------");
//        System.out.println();
        xmlParser.BFSSetLevel();

        // xmlParser.BFSPrint();
        PCounter.initialize();
        Task4QueryPlan2 instance = new Task4QueryPlan2();
//        ArrayList<ArrayList<String>> sortedRules = instance.wrapper();
//        if (instance.nestedLoopInstanceList.size() > 1)
//        	instance.combine(sortedRules);
        String patternTreePath = "/home/user/Desktop/AfterChanges/dbmsiPhase2/javaminibase/src/xmldbTestXML/complextestQuery";
        instance.parseComplexPT(patternTreePath);
//
//        for (String result: globalResults) {
//            System.out.println(result);
//        }

//        System.out.println("Records returned: " + globalResults.size());
        System.out.println("\n\nRead Counter = " + PCounter.rcounter);
        System.out.println("Write Counter = " + PCounter.wcounter);
        System.out.println("Total = " + (PCounter.rcounter + PCounter.wcounter));
    }
}


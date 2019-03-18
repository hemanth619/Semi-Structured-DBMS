package xmldb;

//originally from : joins.C

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

@SuppressWarnings("Duplicates")
//Define the XMLTuple schema
class XMLTuple1 {

    public IntervalType interval;
    public String tagName;

    public XMLTuple1 (int start, int end, int level, String tag) {
        IntervalType val = new IntervalType();
        val.assign(start, end, level);
        interval = val;
        tagName = tag;
    }
}


class XMLRetrieve implements GlobalConst {

    private boolean OK = true;
    private boolean FAIL = false;
    private Vector xmlTuples;
    static XMLParser xmlParser = new XMLParser();

    /** Constructor
     */
    public XMLRetrieve() {

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
        Ssizes[0] = 5; //first elt. is 30

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
                System.out.println(((XMLTuple)xmlTuples.elementAt(i)).tagName + " " + i);
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


    public void createCondExpr(){

        CondExpr[] expr = new CondExpr[4];
        expr[0] = new CondExpr();
        expr[1] = new CondExpr();
        expr[2] = new CondExpr();
        expr[3] = new CondExpr();
        String tagName= "PFAM";


        // Working code for AD!!!!!!!!!!!!!!---------------------------------------
//        expr[0].next  = null;
//        expr[0].op    = new AttrOperator(AttrOperator.aopGT);
//        expr[0].type1 = new AttrType(AttrType.attrSymbol);
//        expr[0].type2 = new AttrType(AttrType.attrSymbol);
//        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
//        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
//        expr[0].flag = 1;
//        expr[1] = null;

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
        Ssizes[0] = 5; //first elt. is 30

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
        // Sort merge setup starts here
        FldSpec [] proj_list = new FldSpec[4];
        proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        proj_list[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        proj_list[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        proj_list[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);

        AttrType [] jtype = new AttrType[2];
        jtype[0] = new AttrType (AttrType.attrInterval);
        jtype[1] = new AttrType (AttrType.attrInterval);
//        jtype[0] = new AttrType (AttrType.attrString);
//        jtype[1] = new AttrType (AttrType.attrString);

        TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
//        SortMerge sm = null;
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

        if (status != OK) {
            //bail out
            System.err.println ("*** Error constructing NestedLoop");
            Runtime.getRuntime().exit(1);
        }

        int iteasd = 0;
        boolean done = false;
        try{
            while(!done){
                t = sm.get_next();
                if(t == null){
                    done = true;
                    break;
                }
                iteasd++;
                byte[] tupleArray = t.getTupleByteArray();
                IntervalType i = t.getIntervalFld(1);
                String tagname = t.getStrFld(2);
                IntervalType j = t.getIntervalFld(3);
                String tagname2 = t.getStrFld(4);
                XMLRecord rec = new XMLRecord(t);
                System.out.println( "Start = " + i.start + " End = " +  i.end + " Level = " + i.level + " Tagname = " + tagname + " Start = " + j.start + " End = " +  j.end + " Level = " + j.level + " Tagname = " + tagname2);

            }
        } catch(Exception e){
            e.printStackTrace();
        }

        System.out.println("Records  returned by NestedLoop: " + iteasd);

    System.out.println("-----------------------------------------------------------------------------------");


        CondExpr[] expr1 = new CondExpr[3];
        expr1[0] = new CondExpr();
        expr1[1] = new CondExpr();
        expr1[2] = new CondExpr();
//        expr[3] = new CondExpr();
        String tagName1= "PFAM";


        // Working code for AD!!!!!!!!!!!!!!---------------------------------------
//        expr[0].next  = null;
//        expr[0].op    = new AttrOperator(AttrOperator.aopGT);
//        expr[0].type1 = new AttrType(AttrType.attrSymbol);
//        expr[0].type2 = new AttrType(AttrType.attrSymbol);
//        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
//        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
//        expr[0].flag = 1;
//        expr[1] = null;

        expr1[0].next  = null;
        expr1[0].op    = new AttrOperator(AttrOperator.aopGT);
        expr1[0].type1 = new AttrType(AttrType.attrSymbol);
        expr1[0].type2 = new AttrType(AttrType.attrSymbol);
        expr1[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
        expr1[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
        expr1[0].flag = 1;
//        expr[1] = null;



        expr1[1].op    = new AttrOperator(AttrOperator.aopEQ);
        expr1[1].next  = null;
        expr1[1].type1 = new AttrType(AttrType.attrSymbol);
        expr1[1].type2 = new AttrType(AttrType.attrString);
        expr1[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
        expr1[1].operand2.string = "Org";

//
        expr1[2].op    = new AttrOperator(AttrOperator.aopEQ);
        expr1[2].next  = null;
        expr1[2].type1 = new AttrType(AttrType.attrSymbol);
        expr1[2].type2 = new AttrType(AttrType.attrString);
        expr1[2].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),2);
        expr1[2].operand2.string = "Murid";

//
        expr1[2] = null;


        Tuple t1 = new Tuple();

        AttrType [] Stypes1 = new AttrType[2];
        Stypes1[0] = new AttrType (AttrType.attrInterval);
        Stypes1[1] = new AttrType (AttrType.attrString);

        //SOS
        short [] Ssizes1 = new short[1];
        Ssizes1[0] = 5; //first elt. is 30

        FldSpec [] Sprojection1 = new FldSpec[2];
        Sprojection1[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        Sprojection1[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

        FldSpec [] Rprojection1 = new FldSpec[2];
        Rprojection1[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        Rprojection1[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);


        CondExpr [] selects1 = new CondExpr [1];
        selects = null;
        boolean status1 = OK;



        FileScan am12 = null;
        FileScan am22 = null;
        try {
//

            am12  = new FileScan("test.in", Stypes1, Ssizes1,
                    (short)2, (short)2,
                    Sprojection1, null);

            am22 = new FileScan("test.in", Stypes1, Ssizes1,
                    (short)2, (short)2,
                    Rprojection, null);

            boolean done1 = false;
//


        }
        catch (Exception e) {
            e.printStackTrace();
        }
        // Sort merge setup starts here
        FldSpec [] proj_list1 = new FldSpec[4];
        proj_list1[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        proj_list1[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        proj_list1[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        proj_list1[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);

        AttrType [] jtype1 = new AttrType[2];
        jtype1[0] = new AttrType (AttrType.attrInterval);
        jtype1[1] = new AttrType (AttrType.attrInterval);
//        jtype[0] = new AttrType (AttrType.attrString);
//        jtype[1] = new AttrType (AttrType.attrString);

//        TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
//        SortMerge sm = null;
        NestedLoopsJoins sm1 =null;
        try {
//            sm = new SortMerge(Stypes, 2, Ssizes,
//                    Stypes, 2, Ssizes,
//                    2, 5,
//                    2, 5,
//                    10,
//                    am, am,
//                    false, false, ascending,
//                    expr, proj_list, 4);
            sm1 = new NestedLoopsJoins(Stypes1, 2, Ssizes1, Stypes1,2,Ssizes1,10,am12,
                    "test.in",expr1,null,proj_list1,4 );
        }
        catch (Exception e) {
            System.err.println("*** join error in NestedLoop constructor ***");
            status = FAIL;
            System.err.println (""+e);
            e.printStackTrace();
        }

        if (status != OK) {
            //bail out
            System.err.println ("*** Error constructing NestedLoop");
            Runtime.getRuntime().exit(1);
        }

        int iteasd1 = 0;
        boolean done1 = false;
        try{
            while(!done1){
                t = sm1.get_next();
                if(t == null){
                    done1 = true;
                    break;
                }
                iteasd1++;
//                byte[] tupleArray = t.getTupleByteArray();
                IntervalType i1 = t.getIntervalFld(1);
                String tagname1 = t.getStrFld(2);
                IntervalType j1 = t.getIntervalFld(3);
                String tagname21 = t.getStrFld(4);
                XMLRecord rec1 = new XMLRecord(t);
                System.out.println( "Start = " + i1.start + " End = " +  i1.end + " Level = " + i1.level + " Tagname = " + tagname1 + " Start = " + j1.start + " End = " +  j1.end + " Level = " + j1.level + " Tagname = " + tagname21);

            }
        } catch(Exception e){
            e.printStackTrace();
        }

        System.out.println("Records  returned by NestedLoop: " + iteasd1);

        System.out.println("-----------------------------------------------------------------------------------");

        equalityScan(sm, sm1);

    }

    public void equalityScan(Iterator sm1, Iterator sm2){

        CondExpr[] expr = new CondExpr[4];
        expr[0] = new CondExpr();
        expr[1] = new CondExpr();
        expr[2] = new CondExpr();
        expr[3] = new CondExpr();
        String tagName= "PFAM";


        // Working code for AD!!!!!!!!!!!!!!---------------------------------------
//        expr[0].next  = null;
//        expr[0].op    = new AttrOperator(AttrOperator.aopGT);
//        expr[0].type1 = new AttrType(AttrType.attrSymbol);
//        expr[0].type2 = new AttrType(AttrType.attrSymbol);
//        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
//        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
//        expr[0].flag = 1;
//        expr[1] = null;

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
        Ssizes[0] = 5; //first elt. is 30

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
        // Sort merge setup starts here
        FldSpec [] proj_list = new FldSpec[4];
        proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        proj_list[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        proj_list[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        proj_list[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);

        AttrType [] jtype = new AttrType[2];
        jtype[0] = new AttrType (AttrType.attrInterval);
        jtype[1] = new AttrType (AttrType.attrInterval);
//        jtype[0] = new AttrType (AttrType.attrString);
//        jtype[1] = new AttrType (AttrType.attrString);

        TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
//        SortMerge sm = null;
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

        if (status != OK) {
            //bail out
            System.err.println ("*** Error constructing NestedLoop");
            Runtime.getRuntime().exit(1);
        }

        int iteasd = 0;
        boolean done = false;
        try{
            while(!done){
                t = sm.get_next();
                if(t == null){
                    done = true;
                    break;
                }
                iteasd++;
                byte[] tupleArray = t.getTupleByteArray();
                IntervalType i = t.getIntervalFld(1);
                String tagname = t.getStrFld(2);
                IntervalType j = t.getIntervalFld(3);
                String tagname2 = t.getStrFld(4);
                XMLRecord rec = new XMLRecord(t);
                System.out.println( "Start = " + i.start + " End = " +  i.end + " Level = " + i.level + " Tagname = " + tagname + " Start = " + j.start + " End = " +  j.end + " Level = " + j.level + " Tagname = " + tagname2);

            }
        } catch(Exception e){
            e.printStackTrace();
        }

        System.out.println("Records  returned by NestedLoop: " + iteasd);
    }


    }
    
    public void wrapper() {
    	// assume this reads the query file, and produces a list of tag names
    }
    
    public void createCondExprQP3(String tagName1, String tagName2, String operand, FileScan iterator1, FileScan iterator2) {
    	CondExpr[] expr = new CondExpr[2];
        expr[0] = new CondExpr();
        expr[1] = null;
        
        expr[0].next  = null;
        
        switch (operand) {
        case "AD":
            expr[0].op    = new AttrOperator(AttrOperator.aopGT);
        	break;
        case "PC":
            expr[0].op    = new AttrOperator(AttrOperator.aopPC);
        	break;
        default:
        	break;
        }
        
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        expr[0].flag = 1;
        

//        expr[0].next  = null;
//        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
//        expr[0].type1 = new AttrType(AttrType.attrSymbol);
//        expr[0].type2 = new AttrType(AttrType.attrString);
//        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),2);
//        expr[0].operand2.string = "";//new FldSpec (new RelSpec(RelSpec.innerRel),1);
//        //expr[0].flag = 1;

//        expr[1].next  = null;
//        expr[1].op    = new AttrOperator(AttrOperator.aopGT);
//        expr[1].type1 = new AttrType(AttrType.attrSymbol);
//        expr[1].type2 = new AttrType(AttrType.attrSymbol);
//        expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
//        expr[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
//        expr[1].flag = 1;
//
//        expr[2] = null;



        Tuple t = new Tuple();

        AttrType [] Stypes = new AttrType[2];
        Stypes[0] = new AttrType (AttrType.attrInterval);
        Stypes[1] = new AttrType (AttrType.attrString);

        //SOS
        short [] Ssizes = new short[1];
        Ssizes[0] = 5; //first elt. is 30

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

        AttrType [] jtype = new AttrType[4];
        jtype[0] = new AttrType (AttrType.attrInterval);
        jtype[1] = new AttrType(AttrType.attrString);
        jtype[2] = new AttrType (AttrType.attrInterval);
        jtype[1] = new AttrType(AttrType.attrString);
//        jtype[0] = new AttrType (AttrType.attrString);
//        jtype[1] = new AttrType (AttrType.attrString);

        TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
//        SortMerge sm = null;
        SortMerge sm =null;

        try {
//            sm = new SortMerge(Stypes, 2, Ssizes,
//                    Stypes, 2, Ssizes,
//                    2, 5,
//                    2, 5,
//                    10,
//                    am, am,
//                    false, false, ascending,
//                    expr, proj_list, 4);
//            sm = new SortMerge(Stypes, 2, Ssizes, Stypes, 2, Ssizes, 1, 12,1, 12, 10, iterator1, iterator2, false, false, ascending, expr, proj_list, 4);
            sm = new SortMerge(Stypes, 2, Ssizes, Stypes, 2, Ssizes, 1, 12, 1, 12, 10, iterator1, iterator2, false, false, ascending, expr, proj_list, 4);

        }
        catch (Exception e) {
            System.err.println("*** join error in NestedLoop constructor ***");
            status = FAIL;
            System.err.println (""+e);
            e.printStackTrace();
        }
        HashSet<String > tupleSet = new HashSet<String>();
        if (status != OK) {
            //bail out
            System.err.println ("*** Error constructing NestedLoop");
            Runtime.getRuntime().exit(1);
        }

        int iteasd = 0;
        boolean done = false;
        try{
            while(!done){
                t = sm.get_next();
                if(t == null) {
                    done = true;
                    break;
                }
                iteasd++;
                byte[] tupleArray = t.getTupleByteArray();
                IntervalType i = t.getIntervalFld(1);
                String tagname = t.getStrFld(2);
//                IntervalType j = t.getIntervalFld(3);
                IntervalType j = t.getIntervalFld(3);
                String tagname2 = t.getStrFld(4);
                XMLRecord rec = new XMLRecord(t);
                String result = "Start = " + i.start + " End = " +  i.end + " Level = " + i.level + " Tagname = " + tagname + " Start = " + j.start + " End = " +  j.end + " Level = " + j.level + " Tagname = " + tagname2;
                tupleSet.add(result);
                // System.out.println( result);
//                System.out.println( "Start = " + i.start + " End = " +  i.end + " Level = " + i.level + " Start = " + j.start + " End = " +  j.end + " Level = " + j.level);

                
            }
            
        } catch(Exception e){
            e.printStackTrace();
        }
        for (String result: tupleSet) {
        	System.out.println(result);
        }
//        for(Tuple tuple: tupleSet) {
//        	IntervalType i = tuple.getIntervalFld(1);
//            String tagname = tuple.getStrFld(2);
////            IntervalType j = t.getIntervalFld(3);
//            IntervalType j = t.getIntervalFld(3);
//            String tagname2 = t.getStrFld(4);
//            XMLRecord rec = new XMLRecord(t);
//            tupleSet.add(t);
////            System.out.println( "Start = " + i.start + " End = " +  i.end + " Level = " + i.level + " Tagname = " + tagname + " Start = " + j.start + " End = " +  j.end + " Level = " + j.level + " Tagname = " + tagname2);
//        }

        System.out.println("Records  returned by SortMerge: " + iteasd);
    }
    
    public void QP3(){

    	String[] tagNames2 = {"root", "Entry"};
    	
    	List<FileScan> fileScanIterators = new ArrayList<FileScan>();
    	
    	
    	for (String tagName: tagNames2) {
    		fileScanIterators.add(this.tagBasedSearchReturnFileScan(tagName));
    	}
    	
    	this.createCondExprQP3("root", "Entry", "PC", fileScanIterators.get(0), fileScanIterators.get(1));
    }
    
    
    public FileScan tagBasedSearchReturnFileScan(String tagnname) {

        AttrType[] Stypes = new AttrType[2];
        Stypes[0] = new AttrType(AttrType.attrInterval);
        Stypes[1] = new AttrType(AttrType.attrString);

        short[] Ssizes = new short[1];
        Ssizes[0] = 5;

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
        Ssizes[0] = 5; //first elt. is 30

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
                System.out.println( "Start = " + i.start + " End = " +  i.end + " Level = " + i.level + " Tagname = " + tagname + "|   Start = " + j.start + " End = " +  j.end + " Level = " + j.level + " Tagname = " + tagname2);
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
        System.out.println("--------------------------------------------------------------------------------------------------------------");

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
        Ssizes1[0] = 5; //first elt. is 30
        Ssizes1[1] = 5;


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


    public static void main(String argv[]) throws ParserConfigurationException, IOException, SAXException, TupleUtilsException, FileScanException, InvalidRelation {

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setValidating(false);
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(new FileInputStream(new File("/home/ronak/DBMSi Project/Phase2/dbmsiPhase2/javaminibase/src/xmldbTestXML/sample_data.xml")));

        Node root = doc.getDocumentElement();

        xmlParser.build(root);

        // xmlParser.BFS();

        xmlParser.preOrder(xmlParser.tree.root);
        System.out.println("---------------------------");
        System.out.println();
        xmlParser.BFSSetLevel();

        // xmlParser.BFSPrint();

        XMLRetrieve xmlinsert = new XMLRetrieve();
        // xmlinsert.createCondExpr();
//        xmlinsert.tagBasedSearch("*");
        xmlinsert.QP3();
    }
}


package io.pivotal.support;

/*
    service hbase-thrift start
    = "framed"

    hbase thrift start
    = "binary"

    hbase thrift start -c
    = "compact"
 */

import org.apache.hadoop.hbase.thrift.generated.AlreadyExists;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TFramedTransport;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;


public class HbaseThriftTest {

    static void usage() {
        System.out.println("\nUsage:");
        System.out.println("java -classpath HbaseThriftTest.jar:`hbase classpath` io.pivotal.support.HbaseThriftTest <host> <port> <protocol> <table name> <column family>");
        System.out.println("\nExamples:");
        System.out.println("java -classpath HbaseThriftTest.jar:`hbase classpath` io.pivotal.support.HbaseThriftTest hdm1 9090 compact sample_demo_table demodata");
        System.out.println("java -classpath HbaseThriftTest.jar:`hbase classpath` io.pivotal.support.HbaseThriftTest hdm1 9090 framed sample_demo_table demodata");
        System.out.println("java -classpath HbaseThriftTest.jar:`hbase classpath` io.pivotal.support.HbaseThriftTest hdm1 9090 binary sample_demo_table demodata");
        System.exit(2);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            usage();
        }

        // collect args
        String HostName = args[0];
        int Port = Integer.parseInt(args[1]);
        String Proto = args[2];
        String TableName = args[3];
        String ColFamily = args[4];

        // setup the hbase thrift connection
        TTransport Transport;
        Transport = new TSocket(HostName, Port);
        TCompactProtocol FProtocol = new TCompactProtocol(Transport);
        Hbase.Client Client = new Hbase.Client(FProtocol);
        if (Proto.equals("binary")) {
            TProtocol Protocol = new TBinaryProtocol(Transport, true, true);
            Client = new Hbase.Client(Protocol);
        } else if ( Proto.equals("framed")) {
            Transport = new TFramedTransport(new TSocket(HostName, Port));
            TProtocol Protocol = new TBinaryProtocol(Transport, true, true);
            Client = new Hbase.Client(Protocol);
        } else if ( ! Proto.equals("compact")) {
            System.out.println("Protocol must be compact or framed or binary");
            usage();
        }
        Transport.open();

        // prepare the column family
        List<ColumnDescriptor> Columns = new ArrayList<ColumnDescriptor>();
        ColumnDescriptor col = new ColumnDescriptor();
        col.name = ByteBuffer.wrap(ColFamily.getBytes());
        Columns.add(col);

        // dump existing tables
        System.out.println("#~ Dumping Existing tables");
        for (ByteBuffer tn : Client.getTableNames()) {
            System.out.println("-- found: " + new String(tn.array(), Charset.forName("UTF-8")));
        }

        // create the new table
        System.out.println("#~ Creating table: " + TableName);
        try {
            Client.createTable(ByteBuffer.wrap(TableName.getBytes()), Columns);
        } catch (AlreadyExists ae) {
            System.out.println("WARN: " + ae.message);
        }

        Transport.close();
    }

}

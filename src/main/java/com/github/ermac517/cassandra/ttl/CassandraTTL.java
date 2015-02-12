package com.github.ermac517.cassandra.ttl;

/**
 * Main Class
 */
public class CassandraTTL
{
    /**
     * Minimum Size for arguments
     */
    private static final int MIN_ARGS_LENGTH = 5;
    
    /**
     * Main Method
     * @param args
     */
    public static void main(String[] args)
    {
        if (args.length < MIN_ARGS_LENGTH)
        {
            System.out.println("Usage: <url> <port> <keyspace> <table> <ttl>");
            System.exit(0);
        }
        
        CassandraClient client = new CassandraClient(args);
        client.connect();
        client.close();
        
        System.exit(0);
    }

}

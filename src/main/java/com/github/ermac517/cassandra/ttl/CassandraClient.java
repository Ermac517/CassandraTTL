package com.github.ermac517.cassandra.ttl;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;

/**
 * Connects to Cassandra Server and updates TTL of each row of each column family
 */
public class CassandraClient
{
    /**
     * IP or domain name of any node in the cluster
     */
    private String url;
    /**
     * RPC port
     */
    private String port;
    /**
     * Name of the Keyspace
     */
    private String keyspace;
    /**
     * Name of the table/column family
     */
    private String table;
    /**
     * TTL value to assign to all keys
     */
    private String ttl;
    /**
     * Current Session with Cluster
     */
    private Session session;
    
    /**
     * Constructor
     * @param args
     */
    public CassandraClient(String... args)
    {
        this.url = args[0];
        this.port = args[1];
        this.keyspace = args[2];
        this.table = args[3];
        this.ttl = args[4];
    }
    
    /**
     * Starts a session with a cluster
     */
    public void connect()
    {
        Cluster cluster = Cluster.builder().addContactPoint(url)
                                           .withPort(Integer.parseInt(port))
                                           .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                                           .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                                           .build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) 
        {
            System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        this.session = cluster.connect();
    }
    
    /**
     * Terminates session with a cluster
     */
    public void close()
    {
        this.session.close();
    }
}
/*
public class CassandraClient
{
    private Cluster cluster;
    private Session session;
    
    public void connect(String node) 
    {
        cluster = Cluster.builder().addContactPoint(node).build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) 
        {
            System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
    }
    
    public void close() 
    {
        cluster.close();
    }
    
    public void createSchema()
    {
        session.execute("CREATE KEYSPACE simplex WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
        
        session.execute("CREATE TABLE simplex.songs (id uuid PRIMARY KEY, title text, album text, artist text, tags set<text>, data blob);");
        session.execute("CREATE TABLE simplex.playlists (id uuid, title text, album text, artist text, song_id uuid, PRIMARY KEY (id, title, album, artist));");
    }
    
    public void loadData()
    { 
        PreparedStatement statement = session.prepare("INSERT INTO simplex.songs " +
                                                      "(id, title, album, artist, tags) " +
                                                      "VALUES (?, ?, ?, ?, ?);");
        
        BoundStatement boundStatement = new BoundStatement(statement);
        Set<String> tags = new HashSet<String>();
        tags.add("jazz");
        tags.add("2013");
        session.execute(boundStatement.bind(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"),
                                            "La Petite Tonkinoise'",
                                            "Bye Bye Blackbird'",
                                            "Joséphine Baker",
                                            tags));

        statement = session.prepare("INSERT INTO simplex.playlists " +
                                    "(id, song_id, title, album, artist) " +
                                    "VALUES (?, ?, ?, ?, ?);");
        boundStatement = new BoundStatement(statement);
        session.execute(boundStatement.bind(UUID.fromString("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
                                            UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"),
                                            "La Petite Tonkinoise",
                                            "Bye Bye Blackbird",
                                            "Joséphine Baker"));
        
    }
    
    public void querySchema()
    {
        ResultSet results = session.execute("SELECT * FROM simplex.playlists WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
        
        System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
                                         "-------------------------------+-----------------------+--------------------"));
         for (Row row : results) 
         {
             System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"),
             row.getString("album"),  row.getString("artist")));
         }
         System.out.println();
    }
    
    public void dropSchema()
    {
        session.execute("DROP KEYSPACE simplex;");
    }
    
    public static void main(String[] args) 
    {
        CassandraClient client = new CassandraClient();
        client.connect("127.0.0.1");
        client.createSchema();
        client.loadData();
        client.querySchema();
        client.dropSchema();
        client.close();
     }

}
*/

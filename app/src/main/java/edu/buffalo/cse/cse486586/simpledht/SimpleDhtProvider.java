package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class SimpleDhtProvider extends ContentProvider {

    String myPort;
    String ownHash;
    final static String TAG = SimpleDhtProvider.class.getSimpleName();
    String predecessor=null;
    String successor=null;
    ArrayList<String> nodeList;
    private static int SERVER_PORT = 10000;
    HashMap<String ,String > hashMap;
    public Uri mUri;
    Object oneReplyLock;
    Object allReplyLock;
    Object oneDeleteLock;
    Object allDeleteLock;
    HashMap<String,String > result;
    Boolean ownQuery = true;
    Boolean ownQueryAll = true;
    HashMap<String, String > resultAll;
/* Logic

*   First 5554 will start.
*   Set own hash.
*   Create an empty table of "Alive Nodes"
*   If a new node is created (i.e. arrives in the network), it will send a message to 5554, showing his presence.
*   5554 will get it's ssh, see from the sorted arraylist where it fits.
*   Add it appropriately in the list.
*   Send the neighbours of that node to update their params to accept the new node as their neighbor.
*   Also, return a message containing "predecessor" and "successor" to that node. The node will set that values as it's own params.
*   The successor node will scan it's list and remove all the values=>NewNodeKey and send it to the new node. Load Balancing is done.
*   This goes on for all 1-5 nodes if they are created..
*   Now if any any message is entered or received from the server side, the receiving node will create a hash of that string and compare it to his predecessor and successor.
*   If Hash(str)>ownKey            : Forward the message to successor and forget about it
*   If Hash(str)<=PredecessorKey   : Forward the message to Predecessor and forget about it.
*   Else If Hash(str)<=ownKey      : Put it in own list.
*
*   If \"*\" in delete() > Send a dispatch to 5554 "DEL_ALL_REQ".
*   5554 will send a message "DEL_ALL" to all.
*   All will delete their tables.
*
*   if \"*\"  in query() > Send a dispatch to 5554 "GET_ALL_REQ".
*   5554 will send a message "GET_ALL" to all.
*   All nodes will send their tables.
*   5554 will collect it and send it to the requesting node.
*   The node will reply.
*
*   If \"@\" in delete() > Delete local table
*
*   If \"@\" in query() > Return local table
* */


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        switch (selection){
            case "\"@\"":
//               db.execSQL("DELETE FROM mytable");
                db.delete(TABLE_NAME,null,null);
                Log.i(TAG, "My own table is deleted");
                break;
            case "\"*\"":
                // Delete your own table and forward it to the next one

//                db.execSQL("DELETE from mytable");
                db.delete(TABLE_NAME,null,null);
                Log.i(TAG, "Deleted own table and forwarding to the next one");
                Message m = new Message();
                m.setType(Message.TYPE.DELETE_ALL);
                m.setRemotePort(Integer.toString(Integer.parseInt(hashMap.get(successor))*2));
                m.setSenderPort(myPort);
                try {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

                break;
            default:
                // Find a key and delete it

                // 1. own table
                Cursor cursor = db.rawQuery("SELECT * from mytable where key = ?",new String[]{selection});
                if(cursor!=null && cursor.getCount()>0){
                    Log.i(TAG, "Found in my table.. Deleting it..");
//                    db.rawQuery("DELETE from mytable where key = ?",new String[]{selection});
                    db.delete(TABLE_NAME,"key='"+selection+"'",null);
                }
                else{
                    // Send it to your successor
                    Log.i(TAG, "Didn't find. Sending DELETE_ONE to the successor. key: "+selection);
                    Message m1 = new Message();
                    m1.setType(Message.TYPE.DELETE_ONE);
                    m1.setRemotePort(Integer.toString(Integer.parseInt(hashMap.get(successor))*2));
                    m1.setKey(selection);
                    m1.setSenderPort(myPort);
                    try {
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m1, null).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }

                }


        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        try {
            String keyHash = genHash((String)values.get("key"));
            String value = (String)values.get("value");
            String key = (String)values.get("key");

            // If you are not the first node, then for all the rest nodes, predecessor will have smaller than own node's hash. (as nodes are in increasing order)
            // 4 (pre) - 5 (own) - 6 (succ) : here pre < Own
            // If you are first node, then pre and post are > own
            // This is how we will identify the first node, which requires special case to be handled

            // If there is only one node
            if(ownHash.equals(predecessor)){
                // Store in own DB
                Log.i(TAG, "Only one node is live. Storing in own");
                long rowID = db.insertWithOnConflict(TABLE_NAME,"",values,SQLiteDatabase.CONFLICT_REPLACE);
            }

            // If there are many nodes and this node is an internal node
            else if(predecessor.compareTo(ownHash)<0){
                if(keyHash.compareTo(ownHash)<=0 && keyHash.compareTo(predecessor)>0){
                    // Keep it with you
                    long rowID = db.insertWithOnConflict(TABLE_NAME,"",values,SQLiteDatabase.CONFLICT_REPLACE);
                    Log.i(TAG, "Storing in own DB");


                }
                else{
                    // Forward it to the successor

                    Message m = new Message();
                    m.setRemotePort(Integer.toString(Integer.parseInt(hashMap.get(successor))*2));
                    m.setKeyHash(keyHash);
                    m.setKey((String)values.get("key"));
                    m.setValue((String)values.get("value"));
                    m.setType(Message.TYPE.ADD);
//                    m.setUri(mUri);

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null).get();
                    Log.i(TAG, "Forwarded to the successor: "+ successor+ " keyHash: "+keyHash);
                }

            }
            else if(predecessor.compareTo(ownHash)>0){
                // Then you are the first node
                // If the message is bigger than the predecessor, then you will store that in you. Else forward it to the successor.
                if(keyHash.compareTo(predecessor)>0 || keyHash.compareTo(ownHash)<=0){
                    // Keep it in you
                    Log.i(TAG, "Storing in own DB");
                    long rowID = db.insertWithOnConflict(TABLE_NAME,"",values,SQLiteDatabase.CONFLICT_REPLACE);
                }
                else{
                    // Forward it to the successor
                    Message m = new Message();
                    m.setRemotePort(Integer.toString(Integer.parseInt(hashMap.get(successor))*2));
                    m.setKeyHash(keyHash);
                    m.setKey((String) values.get("key"));
                    m.setValue((String) values.get("value"));
                    m.setType(Message.TYPE.ADD);
//                    m.setUri(uri);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null).get();
                    Log.i(TAG, "This is First Node. Forwarded to the successor: "+ successor+ " keyHash: "+keyHash);
                }
            }
            else{
                Log.e(TAG, "Unhandled Case?");
            }


        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public boolean onCreate() {

        // Find Own Port
//        myPort = SimpleDhtActivity.myPort;
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr)*2));
        Log.i(TAG, "Node "+myPort+" created.");

        oneReplyLock  = new Object();
        allReplyLock = new Object();
        oneDeleteLock = new Object();
        allDeleteLock = new Object();
        try{
            ownHash = genHash(Integer.toString(Integer.parseInt(myPort) / 2));
            predecessor = ownHash;
            successor = ownHash;
            Log.i(TAG, "Own Port: "+myPort);
        }
        catch (NoSuchAlgorithmException e){
            e.printStackTrace();
        }
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
//        try {
//            Log.i(TAG, genHash("Rod3U5krEsFUbfi76sdI66SxDvFLEIut"));
//        } catch (NoSuchAlgorithmException e) {
//            e.printStackTrace();
//        }
        // Add all reverse lookup entries
        hashMap = new HashMap<>(5);
        try{
            hashMap.put(genHash("5554"),"5554");
            hashMap.put(genHash("5556"),"5556");
            hashMap.put(genHash("5558"),"5558");
            hashMap.put(genHash("5560"),"5560");
            hashMap.put(genHash("5562"),"5562");
            System.out.println(hashMap);
        }

        catch (NoSuchAlgorithmException e){
            e.printStackTrace();
        }
        // Create Database
        Context context = getContext();
        DBHelper dbHelper = new DBHelper(context);
        db = dbHelper.getWritableDatabase();

        if(db!=null) {
            Log.i(TAG,"DATABASE CREATED..Version: " + DB_VERSION);
        }
        else {
            Log.e(TAG,"DATABASE NOT CREATED..");
        }

        // Create Server
        try
        {
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,new ServerSocket(SERVER_PORT));
        }
        catch (IOException e){
            e.printStackTrace();
        }

        // if you are "11108" Create NodeList and add yourself (i.e. hash of 5554)
        // else send a msg to "11108" to register
        // Don't forget to half the port before creating the hash
        if(myPort.equals("11108")){
            nodeList = new ArrayList<String>();
            try
            {
                Log.i(TAG, "Node Joined: 11108");
                String hash = genHash("5554");
                nodeList.add(hash);
            }
            catch (NoSuchAlgorithmException e){
                e.printStackTrace();
            }
        }
        else{
            // Create a message with type "JOIN" and add your port
            // Send it to 5554
            Message m = new Message();
            m.setType(Message.TYPE.JOIN);
            m.setSenderPort(myPort);
            m.setRemotePort("11108");
            Log.i(TAG, "Sending JOIN message to 5554");
            try {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        return true;

    }

    private static ArrayList<String> sortNodeList(ArrayList<String> list){
        Collections.sort(list, new Comparator<String>() {
            @Override
            public int compare(String s, String s2) {
                return s.compareTo(s2);
            }
        });
        return list;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        qb.setTables(TABLE_NAME);
        String queryKey = selection;
        Cursor cursor = null;
        switch (queryKey){
            case "\"*\"":

                // If you are the only node alive, no need to send any msg to any node
                if(predecessor.compareTo(ownHash)==0)
                {
                    // No forwarding
                    cursor = db.rawQuery("SELECT * FROM mytable",null);
                    Log.v("query", selection);

                }
                else{
                    // Send a message to the next one requesting to get all messages from their AVDs.
                    // Create a lock and wait till you get all reply
                    // The message will collect reply from all and release the lock

                    //TODO
//                    if(ownQueryAll==true)
                    Log.i(TAG, "Executing * query from me.");
                    {
                        cursor = db.rawQuery("SELECT * FROM mytable",null);

                        HashMap<String, String> result = new HashMap<>();
                        if(cursor!=null && cursor.getCount()>0){
                            cursor.moveToFirst();
                            for(int i = 0 ; i < cursor.getCount() ; i++){
                                result.put(cursor.getString(0),cursor.getString(1));
                                cursor.moveToNext();
                            }
                        }


                        Message m = new Message();
                        m.setResult(result);
                        m.setType(Message.TYPE.GET_ALL);
                        m.setSenderPort(myPort);
                        m.setRemotePort(Integer.toString(Integer.parseInt(hashMap.get(successor))*2));
//                        try
                        {
                            Log.i(TAG, "Sending GET_ALL message to my successor "+m.getRemotePort());
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null);
                        }
//                        catch (InterruptedException e) {
//                            e.printStackTrace();
//                        } catch (ExecutionException e) {
//                            e.printStackTrace();
//                        }
                        synchronized (allReplyLock){
                            try {
                                allReplyLock.wait();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                            Log.i(TAG, "Got all replies.");
                            MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key","value"});
                            for(Map.Entry<String ,String > entry : resultAll.entrySet()){
                                matrixCursor.addRow(new Object[]{entry.getKey(),entry.getValue()});
                                Log.i(TAG,entry.getKey()+" "+entry.getValue());
                            }
                            cursor = matrixCursor;
                            cursor.moveToFirst();

                            // Now read resultALL, create a cursor and return it.
                        }

                    }
//                    else{
//                        cursor = db.rawQuery("SELECT * FROM mytable",null);
//                    }

                }

                break;

            case "\"@\"":
                // retrieve all from this machine
                cursor = db.rawQuery("SELECT * FROM mytable",null);
                Log.v("query", selection);
                break;

            default:
                // If it's in you, retrieve it, or forward the query message, wait for reply and give the result.
                // TODO
                cursor = db.rawQuery("SELECT * FROM mytable WHERE key = ?",new String[]{selection});
                Log.i(TAG,"Query Fired "+ selection);
//                if(ownQuery==true)
                {
                    Log.i(TAG,"For me");
                    // If the query was fired from your instance then only execute this code, else return the cursor as it is.
//                    ownQuery = false;
                    if(cursor==null || cursor.getCount()==0){
                        Log.i(TAG, "Result not found.. Forwarding to successor.");
                        Message m = new Message();
                        m.setType(Message.TYPE.GET_ONE);
                        m.setSenderPort(myPort);
                        m.setKey(selection);
                        m.setRemotePort(Integer.toString(Integer.parseInt(hashMap.get(successor))*2));
//                        try
                        {
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null);
                        }
//                        catch (InterruptedException e) {
//                            e.printStackTrace();
//                        } catch (ExecutionException e) {
//                            e.printStackTrace();
//                        }

                        synchronized(oneReplyLock){
                            try {
                                Log.i(TAG,"Waiting for lock release.");
                                oneReplyLock.wait();
                                Log.i(TAG, "Lock released");
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        String value = result.get(selection);
                        String key = selection;
                        Log.i(TAG,key +" "+value);
                        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key","value"});
                        matrixCursor.addRow(new Object[]{key,value});
                        cursor = matrixCursor;
                    }
                    Log.v("query", selection);
                }
//                else{
//                    Log.i(TAG,"For Sm1 Else");
//                }

        }

        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private class ClientTask extends AsyncTask<Message, Void, Void>{
        @Override
        protected Void doInBackground(Message... msgs) {
            Message msg = msgs[0];
            ObjectOutputStream objectOutputStream;
            BufferedOutputStream bufferedOutputStream;
            Socket socket = null;
//            if (msg.getType().equals(Message.TYPE.JOIN) || msg.getType().equals(Message.TYPE.UPDATE_LINKS) || msg.getType().equals(Message.TYPE.ADD) )
            {
               try{
                   socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msg.getRemotePort()));
                   bufferedOutputStream = new BufferedOutputStream(socket.getOutputStream());
                   objectOutputStream = new ObjectOutputStream(bufferedOutputStream);
                   objectOutputStream.writeObject(msg);
                   objectOutputStream.flush();
                   objectOutputStream.close();
                   socket.close();
                   if(msg.getType().equals(Message.TYPE.GET_ONE)) Log.i(TAG, "GET_ONE msg sent to "+ msg.getRemotePort());
               }
               catch (UnknownHostException e){
                   e.printStackTrace();
               }
                catch (IOException e){
                    e.printStackTrace();
                }
            }


            return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, Message,  Void>{

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            ServerSocket serverSocket = serverSockets[0];
            Socket socket;
            BufferedInputStream bufferedInputStream;
            ObjectInputStream objectInputStream;

            while (true){
                try
                {
                    socket = serverSocket.accept();
                    bufferedInputStream = new BufferedInputStream(socket.getInputStream());
                    objectInputStream = new ObjectInputStream(bufferedInputStream);
                    Message m = (Message) objectInputStream.readObject();
                    publishProgress(m);
                    objectInputStream.close();
                    socket.close();

                }
                catch (IOException e){
                    e.printStackTrace();
                }
                catch (ClassNotFoundException e){
                    e.printStackTrace();
                }

            }

        }
        protected void onProgressUpdate(Message... msgs){
            Message m = msgs[0];
            Message.TYPE type = m.getType();

            switch (type){
                case JOIN:
                    try {
                        String hash = genHash(Integer.toString(Integer.parseInt(m.getSenderPort()) / 2));
                        nodeList.add(hash);
                        nodeList = sortNodeList(nodeList);
                        Log.i(TAG, "Node Joined: "+m.getSenderPort());

                        // Send 3 messages.

        // 1. Tell Original Sender it's pre and successor.
                        Message m1 = new Message();
                        int index = nodeList.indexOf(hash);

                        // If it's the last node
                        if(index==nodeList.size()-1){
                            m1.setPre(nodeList.get(index-1));
                            m1.setSucc(nodeList.get(0));
                        }

                        // If it's first node
                        else if (index==0){
                            m1.setPre(nodeList.get(nodeList.size()-1));
                            m1.setSucc(nodeList.get(1));
                        }

                        else{
                            m1.setPre(nodeList.get(index-1));
                            m1.setSucc(nodeList.get(index+1));
                        }

                        m1.setRemotePort(m.getSenderPort());
                        m1.setType(Message.TYPE.UPDATE_LINKS);
//                        Log.i(TAG, "Sending UPDATE_LINKS to"+m1.getRemotePort());


        // 2. Find the predecessor and successor port numbers from the hashmap and send them the message to update their links
                        int indexPre = nodeList.indexOf(m1.getPre());
                        int indexSucc = nodeList.indexOf(m1.getSucc());

                        Message m2 = new Message();
                        int index2 = indexPre;

                        // If it's the last node
                        if(index2==nodeList.size()-1){
                            m2.setPre(nodeList.get(index2-1));
                            m2.setSucc(nodeList.get(0));
                        }

                        // If it's first node
                        else if (index2==0){
                            m2.setPre(nodeList.get(nodeList.size()-1));
                            m2.setSucc(nodeList.get(1));
                        }

                        else{
                            m2.setPre(nodeList.get(index2-1));
                            m2.setSucc(nodeList.get(index2+1));
                        }

                        m2.setRemotePort(Integer.toString(Integer.parseInt(hashMap.get(nodeList.get(index2)))*2));
                        m2.setType(Message.TYPE.UPDATE_LINKS);
//                        Log.i(TAG, "Sending UPDATE_LINKS to"+m2.getRemotePort());




                        int index3 = indexSucc;
                        Message m3 = new Message();

                        // If it's the last node
                        if(index3==nodeList.size()-1){
                            m3.setPre(nodeList.get(index3-1));
                            m3.setSucc(nodeList.get(0));
                        }

                        // If it's first node
                        else if (index3==0){
                            m3.setPre(nodeList.get(nodeList.size()-1));
                            m3.setSucc(nodeList.get(1));
                        }

                        else{
                            m3.setPre(nodeList.get(index3-1));
                            m3.setSucc(nodeList.get(index3+1));
                        }

                        m3.setRemotePort(Integer.toString(Integer.parseInt(hashMap.get(nodeList.get(index3)))*2));
//                        Log.i(TAG, "Sending UPDATE_LINKS to"+m3.getRemotePort());
                        m3.setType(Message.TYPE.UPDATE_LINKS);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m1, null);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m2, null);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m3, null);


                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                    break;

                case UPDATE_LINKS:
                    predecessor = m.getPre();
                    successor = m.getSucc();

                    Log.i(TAG, "Update: Predecessor = "+hashMap.get(predecessor));
                    Log.i(TAG, "Update: Successor = "+hashMap.get(successor));
                    break;
                case ADD:
                    insert(mUri, m.getContentValue());
                    break;
                case GET_ONE:
                    Log.i(TAG, "GET_ONE msg received.");
//                    ownQuery = false;
                    Cursor cursor = db.rawQuery("SELECT * FROM mytable where key = ?", new String[]{m.getKey()});
                    if(cursor!=null && cursor.getCount()>0){
                        // Then send the result to the original requester
                        cursor.moveToFirst();
                        String key = cursor.getString(0);
                        String value = cursor.getString(1);
                        Message msg = new Message();
                        msg.setType(Message.TYPE.REPLY_ONE);
                        HashMap<String, String> result = new HashMap<>();
                        result.put(key,value);
                        msg.setResult(result);
                        msg.setRemotePort(m.getSenderPort());
                        msg.setSenderPort(myPort);
                        Log.i(TAG, "Found GET_ONE result in my DB. Sending Back! "+m.getKey() +" "+value);
                        try {
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, null).get();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        }
                    }
                    else{
                        // else forward it to your successor
                        Log.i(TAG, "Didn't find the result.. Forwarding.."+m.getKey());
                        m.setRemotePort(Integer.toString(Integer.parseInt(hashMap.get(successor))*2));
                        try {
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null).get();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        }
                    }
//                    ownQuery = true;
                    break;
                case REPLY_ONE:
                    // If you receive this type of message, then you must have asked for it.
                    // Get the messages and create a cursor object and return
                    synchronized (oneReplyLock){
                        result = m.getResult();
                        Log.i(TAG, "Got Reply from "+ m.getSenderPort());
                        oneReplyLock.notify();
                        Log.i(TAG,"Lock notified");
                    }
                    break;
                case GET_ALL:
//                    ownQueryAll = false;
                    if(m.getRemotePort().equals(m.getSenderPort())){

                        synchronized (allReplyLock){
                            Log.i(TAG, "* query circle is finished. Releasing the lock.");
                            resultAll = m.getResult();
                            allReplyLock.notify();
                        }
                    }
                    else{
                        // Get own Replies and fill it
                        Log.i(TAG,"executing * in my app");
                        Cursor cursor1 = db.rawQuery("SELECT * FROM mytable",null);
                        HashMap<String , String > result = m.getResult();
                        if(cursor1!=null && cursor1.getCount()>0){
                            cursor1.moveToFirst();
                            for (int i = 0; i<cursor1.getCount(); i++){
                                result.put(cursor1.getString(0), cursor1.getString(1));
                                cursor1.moveToNext();
                            }

                        }
                        m.setResult(result);
                        m.setRemotePort(Integer.toString(Integer.parseInt(hashMap.get(successor))*2));
                        Log.i(TAG,"Finished. Now forwarding to the successor. "+m.getRemotePort());
                        try {
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null).get();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        }
                    }
                    break;
                case DELETE_ONE:
                    Log.i(TAG,"DELETE_ONE RECEIVED");
                    // Technically there shouldn't be an infinte loop in this anyways. Because the node which has the key will not forward it.
                    Cursor cursor1 = db.rawQuery("SELECT * from mytable where key = ?",new String[]{m.getKey()});
                    if(cursor1!=null && cursor1.getCount()>0){
                        Log.i(TAG, "Found it. Deleting it..");
//                        db.rawQuery("DELETE from mytable where key=?",new String[]{m.getKey()});
                        db.delete(TABLE_NAME,"key='"+m.getKey()+"'",null);
                        Log.i(TAG,"Deleted");
                    }
                    else{
                        // Forward it to the successor
                        Log.i(TAG, "Didn't find- didn't delete, forwarding to my successor");
                        m.setRemotePort(Integer.toString(Integer.parseInt(hashMap.get(successor))*2));
                        try {
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null).get();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        }
                    }

                    break;
                case DELETE_ALL:
                    if(m.getRemotePort().equals(m.getSenderPort())){
                        Log.i(TAG, "Loop Complete. All db must have been deleted.");
                    }
                    else{

//                        db.execSQL("DELETE from mytable");
                        db.delete(TABLE_NAME,null,null);
                        Log.i(TAG,"Deleted my table and forwarding to the successor");
                        m.setRemotePort(Integer.toString(Integer.parseInt(hashMap.get(successor))*2));
                        try {
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null).get();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        }
                    }
                    break;

            }

        }
    }

    private SQLiteDatabase db;
    static final String DB_NAME = "mydb";
    static final String TABLE_NAME = "mytable";
    static final String TABLE_NAME_2 = "nodetable";
    static final int DB_VERSION = 2;
    static final String CREATE_DB_TABLE = "CREATE TABLE " + TABLE_NAME+
            " ( key STRING PRIMARY KEY,"
            + " value STRING )";
//    static final String CREATE_DB_TABLE_2 = "CREATE TABLE " + TABLE_NAME_2+
//            " ( nodekey STRING PRIMARY KEY)";
    private static class DBHelper extends SQLiteOpenHelper {

        DBHelper(Context context){
            super(context, DB_NAME, null, DB_VERSION);
        }
        public void onCreate(SQLiteDatabase db){
            db.execSQL(CREATE_DB_TABLE);
//            db.execSQL(CREATE_DB_TABLE_2);
        }
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            Log.i(TAG,"Old version "+db.getVersion());
            db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);

            onCreate(db);
            Log.i(TAG, "Old table dropped ");
            Log.i(TAG, "New version " + db.getVersion());
        }
    }


}

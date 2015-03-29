package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
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

public class SimpleDhtProvider extends ContentProvider {

    String myPort;
    String ownHash;
    final static String TAG = SimpleDhtProvider.class.getSimpleName();
    String predecessor=null;
    String successor=null;
    ArrayList<String> nodeList;
    private static int SERVER_PORT = 10000;

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
        try{
            ownHash = genHash(myPort);
            Log.i(TAG, "Own Port: "+myPort);
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
                Log.i(TAG, "Node Joined: "+myPort);
                nodeList.add(genHash(myPort));
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
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null);
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
        // TODO Auto-generated method stub
        return null;
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

    private class ClientTask extends AsyncTask<Message, Void, Void>{
        @Override
        protected Void doInBackground(Message... msgs) {
            Message msg = msgs[0];
            ObjectOutputStream objectOutputStream;
            BufferedOutputStream bufferedOutputStream;
            Socket socket = null;
            if (msg.getType().equals(Message.TYPE.JOIN)){
               try{
                   socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msg.getRemotePort()));
                   bufferedOutputStream = new BufferedOutputStream(socket.getOutputStream());
                   objectOutputStream = new ObjectOutputStream(bufferedOutputStream);
                   objectOutputStream.writeObject(msg);
                   objectOutputStream.flush();
                   objectOutputStream.close();
                   socket.close();
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
                        nodeList.add(genHash(Integer.toString(Integer.parseInt(m.getSenderPort()) / 2)));
                        nodeList = sortNodeList(nodeList);
                        Log.i(TAG, "Node Joined: "+m.getSenderPort());

                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                    break;
            }

        }
    }

    private SQLiteDatabase db;
    static final String DB_NAME = "mydb";
    static final String TABLE_NAME = "mytable";
    static final String TABLE_NAME_2 = "nodetable";
    static final int DB_VERSION = 1;
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

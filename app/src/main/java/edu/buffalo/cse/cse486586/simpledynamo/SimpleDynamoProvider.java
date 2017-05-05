package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {

	private SharedPreferences mPrefs = null;
	private final static String SHARED_PREF_FILENAME = "edu.buffalo.cse.cse486586.dynamo.sharedpref";
	private SharedPreferences mAdminPrefs = null;
	private final static String ADMIN_SHARED_PREF_FILENAME = "edu.buffalo.cse.cse486586.dynamo.adminsharedpref";

	public final static String COLUMN_KEY = "key";
	public final static String COLUMN_VALUE = "value";

	private static final int LISTEN_PORT = 10000;
	private TreeMap<String, String> ring;
	private ServerSocket serverSocket;
	private String myPort;
	private String myHash;
	SimpleDynamoActivity ref;
	private volatile boolean synchOn;
	private ReentrantLock myLock;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		Log.d(TAG,"Delete: "+selection);
		if(selection.equals("*")) {
			//delete ALL k-v in entire DHT
			/*JSONObject deleteRequest = new JSONObject();
			try {
				deleteRequest.put("port_no",self.getPortNo());
				deleteRequest.put("msg_type","delete_all");
				Log.d(TAG,"GLOBAL DELETE REQUEST GOTTEN! Sending delete all request to "+nextNode.getPortNo());   //TODO: try sending to self first?
				//always send JOIN requests to avd5554
				Object t=new SendMessage().execute(deleteRequest.toString(),String.valueOf(nextNode.getPortNo())).get();
				Log.w(TAG,"t.get:"+t);
				//TODO: If a node joins at a later point in testing, then also fetch some items from this node's successor. In that case, either wait here, or in postExecute, fire another async

			} catch (JSONException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}*/
		} else if(selection.equals("@")) {
			//delete all k=v stored locally
			mPrefs.edit().clear().apply();
		} else {
			if(mPrefs.contains(selection)) {
				mPrefs.edit().remove(selection).apply();
				Log.d(TAG,"Delete: key found. WIll be deleted");

			}
			else {
				Log.d(TAG,"Delete: key not found! Propagating to others");

				try {
					JSONObject message = new JSONObject();
					message.put("key",selection);
					message.put("sender",myPort);
					message.put("type","delete");
					for(String t:getTargets(Util.genHash(selection))) {
						Log.d(TAG,"Forwarding delete to:"+t+":"+ring.get(t));
						String ret = new SendMessage().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message.toString(), ring.get(t)).get();
						if(ret == null)
							Log.e(TAG, "Some error occured(?) while sending delete to "+ring.get(t));
						else
							Log.w(TAG,"Deleted remotely");
					}
				} catch (JSONException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}


			}
//			int count = mPrefs.getAll().size();
			//Observed problem: Due to apply(), which is asynchronous, sometimes, a future query to @ will return "freshstart" pref, which is not what we want.
			//One solution is to use commit() . Another is to use a different pref file for freshstart alone. See if commit() takes a performance hit, else do option B.
			/*Log.d(TAG,count + " items remaining after delete");
			if(count == 1) {
				for(String k:mPrefs.getAll().keySet()) {
					Log.d(TAG,k+":"+mPrefs.getString(k,null));
				}
			}*/
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
		String key = values.getAsString(COLUMN_KEY);
		String hashedKey = Util.genHash(key);
		String value = values.getAsString(COLUMN_VALUE);
		if(mPrefs.getString(key,null)!=null)
			Log.e(TAG,"Duplicate insert on same avd");
		Log.d(TAG,"Inserting->"+key+" : "+value+". Hashed:"+hashedKey);

		//mPrefs.edit().putString(key,value);

		String[] targets = getTargets(hashedKey);

		try {
			JSONObject message = new JSONObject();
			message.put("key",key);
			message.put("value",value);
			message.put("sender",myPort);
			message.put("type","insert");
			for(String t:targets) {
				Log.d(TAG,"Forwarding insert to:"+t+":"+ring.get(t));
				String ret = new SendMessage().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message.toString(), ring.get(t)).get();
				if(ret == null)
					Log.e(TAG, "Some error occured(?) while sending to "+ring.get(t));
			}
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

		return uri;
	}

	/**
	 * Returns the coordinator for which this is a replica of AND the replica of this node(as a coordinator)
	 * This assumes that when a recovery occurs, all 5 nodes are always alive
	 * @param key
	 * @return target[2]. target[0] contains the node predecessor of the predecessor, target[1] contains the successor
	 */
	public String[] getSynchTargets(String key) {
		//populate 3 nodes to sen data to
		String [] targets = new String[3];
		//get the coordinator of this replica (two steps behind)
		//go one step behind
		targets[0] = ring.lowerKey(key);
		if(targets[0] == null) {	//key is the lowest
			targets[0] = ring.lastKey();
		}
		//go another step behind
		targets[1] = ring.lowerKey(targets[0]);
		if(targets[1] == null) {	//key is the lowest
			targets[1] = ring.lastKey();
		}

		//get the successor(key's replica)
		targets[2] = ring.higherKey(key);
		if(targets[2] == null)	//target[0] is last
			targets[2] = ring.firstKey();

		return targets;
	}


	public String[] getTargets(String key) {
		//populate 3 nodes to sen data to
		String [] targets = new String[3];
		for(String k:ring.keySet()) {
			if(k.equals(ring.firstKey())) {
				if(key.compareTo(ring.lastKey())>0 || key.compareTo(k)<=0) {		//k is lowest
//					Log.d(TAG,hashedKey+" b1elongs to "+k);
					targets[0] = k;
					break;
				}
				else
					continue;
			}
			else if(ring.lowerKey(k).compareTo(key)<0 && k.compareTo(key)>=0) {
//				Log.d(TAG,hashedKey+" belongs to "+k);
				targets[0] = k;
				break;
			}
		}

		//send message to coordinator and its successors (assuming no failures)
		targets[1] = ring.higherKey(targets[0]);
		if(targets[1] == null)	//target[0] is last
			targets[1] = ring.firstKey();

		targets[2] = ring.higherKey(targets[1]);
		if(targets[2] == null)
			targets[2] = ring.firstKey();
		return targets;
	}


	@Override
	public boolean onCreate() {
		Log.e(TAG,"oncreate. Locking..ThreadID:"+Thread.currentThread().getId());
		if(myLock != null) {
			Log.e(TAG,"myLock wasnt null in onCreate!!!");
		}
		myLock = new ReentrantLock();
		myLock.lock();
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf(Integer.parseInt(portStr)*2);
		myHash = Util.genHash(String.valueOf(Integer.parseInt(portStr)));
		try {
			serverSocket = new ServerSocket();
			serverSocket.setReuseAddress(true);
			serverSocket.bind(new InetSocketAddress(LISTEN_PORT));
			new AcceptMessages().execute();
		} catch (java.net.BindException e) {
			Log.w(TAG,"Bindexception occurred. Continuing");
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		mPrefs = getContext().getSharedPreferences(SHARED_PREF_FILENAME, Context.MODE_PRIVATE);//multiprocess rquired?
		mAdminPrefs = getContext().getSharedPreferences(ADMIN_SHARED_PREF_FILENAME, Context.MODE_PRIVATE);//multiprocess rquired?
//		mPrefs.edit().clear().apply();

		ring = new TreeMap<String, String>();
		ring.put(Util.genHash("5554"),"11108");
		ring.put(Util.genHash("5556"),"11112");
		ring.put(Util.genHash("5558"),"11116");
		ring.put(Util.genHash("5560"),"11120");
		ring.put(Util.genHash("5562"),"11124");

		Log.w(TAG,"Ring:");
		for(String k: ring.keySet()) {
			Log.w(TAG,k+":"+ring.get(k));
		}

		synchOn = false;

		//determine if this is a crash start or fresh start
		if(mAdminPrefs.getString("freshstart",null) == null) {
			//fresh start. Mark it as such bu storing someting in this key
			mAdminPrefs.edit().putString("freshstart","yes").commit();
		}
		else {
			//recovering from a crash. Remove from prefs and start synch
//			mPrefs.edit().remove("freshstart").commit();

			//Synch only if we previously had at least one KV pair. Otherwise, we are recovering after a delete. in which case, we don't want to synch
			if(mPrefs.getAll().size()>0) {
				Log.d(TAG, "Starting synch after recovery");

				synchOn = true;
				synch();
				synchOn = false;

			}
			else {
				Log.d(TAG,"No data found. Deletes had presumably occured. Not synching");
			}
		}
		Log.e(TAG,"onCreate unlocking..");
		myLock.unlock();
		return true;
	}



	/**
	 * Returns a JSON array of key value pairs of local data, converted to a String
	 * @return
	 */
	private String fetchLocalData() {
		JSONArray local = new JSONArray();
		try {
			Map<String, String> all = (Map<String, String>) mPrefs.getAll();
			for(String k:all.keySet()) {
				JSONObject obj = new JSONObject();
				obj.put("key",k);
				obj.put("value",all.get(k));
				local.put(obj);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return local.toString();
	}

	/**
	 *
	 * Returns a JSON array of key value pairs of my own data (not data in myself that is a replica of somebody else). This will be called by replicas of us.(via AcceptMessage of course)
	 * @return
	 * @param owner: Must be hashed!
	 */
	private String fetchDataBelongingTo(String owner) {
//		String previous = ring.lowerKey(myHash);
//		if(previous == null)
//			previous = ring.lastKey();

		JSONArray local = new JSONArray();
		try {
			Map<String, String> all = (Map<String, String>) mPrefs.getAll();
			for(String k:all.keySet()) {
				if(!doesKeyBelongTo(Util.genHash(k),owner)) {		//NOTE: owner must be hashed from caller of this method.
					Log.w(TAG,k+" doesnt belong to "+owner);
					continue;
				}
				JSONObject obj = new JSONObject();
				obj.put("key",k);
				obj.put("value",all.get(k));
				local.put(obj);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return local.toString();
	}


	private boolean doesKeyBelongTo(String key, String node) {	//both parameters are assumed to be hashed.
		String prev = ring.lowerKey(node);
		if(prev == null)
			prev = ring.lastKey();
		//find if 'k' falls between prev and node includingn the border cases
		if(node.equals(ring.firstKey())) {
			if(key.compareTo(prev)>0 || key.compareTo(node)<=0)
				return true;
		}
		else if(prev.compareTo(key)<0 && key.compareTo(node)<=0)
			return true;

		return false;
	}

	public void synch() {
		//synch from both next server.
		// Same as query * except this time we query only our next 2 guys, combine results and store them in our sharedprefs
		if(!myLock.isHeldByCurrentThread()) {
			throw new AssertionError("Lock has to be held while synching");
		}
		String[] targets = getSynchTargets(myHash);


		JSONObject message = new JSONObject();
		try {
			message.put("type","synch");
			message.put("sender", myPort);
			HashMap<String,String> synched = new HashMap<String, String>();
			for(int j=0;j<targets.length;j++) {
				if(!myLock.isHeldByCurrentThread()) {
					throw new AssertionError("2_Lock has to be held while synching");
				}
				String result;
				if(j==0 || j==1) {		//getting data from predecessors
					message.put("role","slave");	//"role" is our role.
					result = new SendMessage().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message.toString(), ring.get(targets[j])).get();
				} else {
					message.put("role","peer");		//this is our replica(first successor). So, we're its coordinator
					result = new SendMessage().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message.toString(), ring.get(targets[j])).get();
				}

				if (result != null) {
					//add contents of result into cursor
					JSONArray keyValus = new JSONArray(result);
					Log.d(TAG, "Synch: Received " + keyValus.length() + " entries from " + ring.get(targets[j]));
					for (int i = 0; i < keyValus.length(); i++) {
						JSONObject obj = keyValus.getJSONObject(i);
						if (synched.get(obj.getString("key")) != null && !synched.get(obj.getString("key")).equals(obj.getString("value"))) {
							Log.w(TAG, "Different values for key:" + obj.getString("key"));
						}
						//TODO: Compare versions somehow. synched is unused right now..

						storeLocally(obj.getString("key"), obj.getString("value"),true);
					}
				}
				else {
					Log.e(TAG,"Faile to synch from "+targets[j]);
				}
			}
			//combine result
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}
	class KeyCount {
		String keyStr;
		int count;

		public KeyCount(String value) {
			this.keyStr= value;
			this.count = 1;
		}
		public KeyCount(String keyString, int count) {
			this.keyStr = keyString;
			this.count = count;
		}

	}

	@Override
	public void shutdown() {
		Log.e(TAG,"Shutdown called contentprovider");
		if(serverSocket!=null)
			try {
				serverSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		super.shutdown();
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		Log.d(TAG,"Query: selection:"+selection);
		MatrixCursor cursor = new MatrixCursor(new String[]{COLUMN_KEY,COLUMN_VALUE});
		if(selection.equals("@")) {
			//return everything in this node
			Map<String, String> all = (Map<String, String>) mPrefs.getAll();
			for(String k:all.keySet()) {
				cursor.addRow(new Object[]{k,all.get(k)});
			}

			//Get latest version of data from everynode

			/*JSONObject message = new JSONObject();
			try {
				message.put("type","query");
				message.put("sender", myPort);
				HashMap<String, KeyCount> vals = new HashMap<String, KeyCount>();
				for(String t:getTargets(myHash)) {
					String result = new SendMessage().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,message.toString(),ring.get(t)).get();
					if(result!=null) {
						//add contents of result into cursor
						Log.d(TAG,"Received "+result.length()+" entries from " + ring.get(t));
						JSONArray keyValus = new JSONArray(result);
						for(int i=0;i<keyValus.length();i++) {
							JSONObject kv = keyValus.getJSONObject(i);
							String key = kv.getString("key");
							String value = kv.getString("value");
							if(vals.containsKey(value)) {
								vals.put(value,new KeyCount(key,vals.get(value).count+1));
							}
							else
								vals.put(value,new KeyCount(key));
						}
					}
				}

				//combine result

				//put KVs with count = 2 and 3
				for(String v:vals.keySet()) {
					if(vals.get(v).count==2 || vals.get(v).count==3) {
						cursor.addRow(new Object[]{vals.get(v).keyStr, v});
						vals.put(v,new KeyCount("",0));	//set the count of this entry to zero. Emptying the string is just saving some cpu cylces
					}

				}

				//put KVs with count 1
				for(String v:vals.keySet()) {
					if(vals.get(v).count!=0) {
						cursor.addRow(new Object[]{vals.get(v).keyStr, v});
					}
				}

			} catch (JSONException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}*/



		} else if(selection.equals("*")) {
			//everything in dynamo
			JSONObject message = new JSONObject();
			try {
				message.put("type","query");
				message.put("sender", myPort);
				for(String t:ring.keySet()) {
					String result = new SendMessage().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,message.toString(),ring.get(t)).get();
					if(result!=null) {
						//add contents of result into cursor
						Log.d(TAG,"Received "+result.length()+" entries from " + ring.get(t));
						JSONArray keyValus = new JSONArray(result);
						for(int i=0;i<keyValus.length();i++) {
							JSONObject kv = keyValus.getJSONObject(i);
							cursor.addRow(new Object[]{kv.get("key"),kv.get("value")});
						}
					}
				}
				//combine result
			} catch (JSONException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}


		} else {	//local query for one object
			Log.d(TAG,"Local query for "+selection);
			String value = mPrefs.getString(selection,null);
			if(value!=null)
				cursor.addRow(new Object[]{selection,value});
			else {	//contact all target nodes. Return the one with the latest version
				String[] targets = getTargets(Util.genHash(selection));
				try {
					JSONObject message = new JSONObject();
					message.put("key",selection);
					message.put("sender",myPort);
					message.put("type","singlequery");
					HashMap<String, Integer> replies = new HashMap<String, Integer>();
					for(String t:targets) {
						Log.d(TAG,"Forwarding query to:"+t+":"+ring.get(t));
						String reply = new SendMessage().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message.toString(), ring.get(t)).get();
						if(reply!=null) {
							//TODO: Compare latestReply with reply and update latestReply to be the actual latest version of teh retrieved data..
							if(replies.containsKey(reply))
								replies.put(reply,replies.get(reply)+1);	//increment count
							else
								replies.put(reply,1);	//first value here

							Log.d(TAG,"Received "+selection+":"+reply+" from "+ring.get(t));
						}
						else {
							Log.e(TAG,"SingleQuery response null");
						}
					}
					boolean majorityFound = false;
					for(String k:replies.keySet()) {
						if(replies.get(k)>=2) {
							cursor.addRow(new Object[]{selection,k});
							majorityFound = true;
							break;
						}
					}
					if(!majorityFound) {
						Log.e(TAG,"No key had a count of 2 or 3. Which means all keys were different. TThis is a problem!");
						//pick the first "key" which is the first value we receive and put into cursor. Putting something is probabilistically better than putting nothing at all
						for(String k:replies.keySet()) {
								cursor.addRow(new Object[]{selection,k});
								break;
						}
					}

				} catch (JSONException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
		}
		Log.d(TAG,"Returning "+cursor.getCount()+" items");
		Log.d(TAG,"Returning "+cursor.getCount()+" items");
		return cursor;
	}

	private void storeLocally(String key, String value, boolean insertedViaSynch) {
		if(insertedViaSynch) {
			Log.w(TAG,"(Synch)Message with hash:"+Util.genHash(key)+" stored locally("+myHash+")");
			Log.w(TAG,"(Synch)storeLocally KV:"+key+":"+value);
		}
		else {
			Log.w(TAG,"Message with hash:"+Util.genHash(key)+" stored locally("+myHash+")");
			Log.w(TAG,"storeLocally KV:"+key+":"+value);
		}
		mPrefs.edit().putString(key,value).apply();

	}


	private class SendMessage extends AsyncTask<String,Void,String> {

		@Override
		protected String doInBackground(String... strings) {
			Log.d(TAG,myPort+ "sending to "+strings[1]);
			String message = strings[0] + "\n";
			int portNo = Integer.valueOf(strings[1]);
			String returnVal = null;
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						portNo);
//				socket.setSoTimeout(1000);
				PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(),"UTF-8")),true);
				BufferedReader r=new BufferedReader(new InputStreamReader(socket.getInputStream(),"UTF-8"));
				writer.println(message);

				String rec = r.readLine();
				if(rec!=null) {
					JSONObject obj = new JSONObject(rec);
					if(obj.get("type").equals("insert_reply")) {
						returnVal  = "insert_reply";
					}
					else if(obj.get("type").equals("query_reply")) {
						returnVal = obj.getString("content");
					}
					else if(obj.get("type").equals("singlequery_reply")) {
						returnVal = obj.getString("content");
					}
					else if(obj.get("type").equals("synch_reply")) {
						returnVal = obj.getString("content");
					} else if(obj.get("type").equals("delete_reply")) {
						returnVal = "delete_reply";
					}
					else {
						Log.e(TAG,"Unknown reply type! SendMessage!:"+obj.toString());
						returnVal = rec;
					}
				}
				else {
					Log.v(TAG,strings[1] + " failed. rec null");
				}
				writer.close();
				r.close();
				socket.close();

			} catch (UnknownHostException ex) {
				//ex.printStackTrace();
				Log.e(TAG,strings[1] + " failed");
			} catch(SocketTimeoutException ex) {
				Log.e(TAG,strings[1] + " failed");
			} catch(StreamCorruptedException ex) {
				Log.e(TAG,strings[1] + " failed");
			} catch (EOFException ex) {
				Log.e(TAG,strings[1] + " failed");
			}  catch (IOException e) {
				//e.printStackTrace();
				Log.e(TAG,strings[1] + " failed");
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return returnVal;
		}
	}



	private class AcceptMessages extends AsyncTask<String,String,Void> {

		@Override
		protected Void doInBackground(String... strings) {
			String sender = "";

			try {
				while (true) {
					Socket client = serverSocket.accept();
					if(synchOn) {
						Log.e(TAG,"AcceptMessage called while synch on");
					}
					Log.w(TAG,"Locking..current thread ID:"+Thread.currentThread().getId());
					if(myLock.isLocked()) {
						Log.e(TAG,"Yeeah, onCreate holds a lock to myLock");
					}
					myLock.lock();
					Log.w(TAG,"Lock acquired");
					myLock.unlock();
					Log.d(TAG,"Accepted connection");
					BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream(),"UTF-8"));
					PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(client.getOutputStream(),"UTF-8")),true);
					String str = reader.readLine();
					if(str!=null) {
						JSONObject received = new JSONObject(str);
						sender = received.getString("sender");
						if(received.getString("type").equals("insert")) {
							storeLocally(received.getString("key"),received.getString("value"),false);
							JSONObject obj = new JSONObject();
							obj.put("type","insert_reply");
							writer.println(obj.toString());
						}
						else if(received.getString("type").equals("query")) {
							if(synchOn) {
								Log.e(TAG,"Query received while synch on");
							}
							JSONObject reply = new JSONObject();
							reply.put("content",fetchLocalData());
							reply.put("type","query_reply");
							writer.println(reply.toString());

							//maybe read again?

						}
						else if(received.getString("type").equals("singlequery")) {
							if(synchOn) {
								Log.e(TAG,"single Query received while synch on");
							}
							JSONObject reply = new JSONObject();
							reply.put("content",mPrefs.getString(received.getString("key"),null));
							reply.put("type","singlequery_reply");
							writer.println(reply.toString());

						}
						else if(received.getString("type").equals("synch")) {
							if(synchOn) {
								Log.e(TAG,"Synch request received while synch on");
							}
							Log.d(TAG,"Synch request from "+sender);
							String returnedData;
							if(received.getString("role").equals("slave"))	//role is the requestors role.
								returnedData = fetchDataBelongingTo(myHash);
							else {				//peer
								String senderHash = Util.genHash(String.valueOf(Integer.parseInt(sender)/2));
								returnedData = fetchDataBelongingTo(senderHash);	//11XX /2 and then hash it
							}
							JSONObject reply = new JSONObject();
							reply.put("content",returnedData);
							reply.put("type","synch_reply");
							writer.println(reply.toString());

						} else if(received.getString("type").equals("delete")) {
							mPrefs.edit().remove(received.getString("key")).apply();
							JSONObject obj = new JSONObject();
							obj.put("type","delete_reply");
							writer.println(obj.toString());
						}

						else {
							Log.e(TAG,"Invalid message type!");
						}
					}
					else {
						Log.v(TAG,"AcceptMessage received null");
					}

					writer.close();
					reader.close();
					client.close();

				}
			}catch(SocketTimeoutException ex) {
				Log.e(TAG,"AcceptMessages:" + sender + " failed");
			} catch(StreamCorruptedException ex) {
				Log.e(TAG,"AcceptMessages:" + sender + " failed");
			} catch (EOFException ex) {
				Log.e(TAG,"AcceptMessages:" + sender + " failed");
			} catch(IOException e){
//				e.printStackTrace();
				Log.e(TAG,"AcceptMessages:" + sender + " failed");
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return null;
		}
	}



	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		return 0;
	}


}

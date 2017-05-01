package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Handler;
import android.os.Looper;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.Toast;

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

	private ReentrantLock ringLock;
	private SharedPreferences mPrefs = null;
	private final static String SHARED_PREF_FILENAME = "edu.buffalo.cse.cse486586.dynamo.sharedpref";

	public final static String COLUMN_KEY = "key";
	public final static String COLUMN_VALUE = "value";


	private static final int LISTEN_PORT = 10000;
	private TreeMap<String, String> ring;
	private ServerSocket serverSocket;
	private String myPort;
	private String myHash;
	SimpleDynamoActivity ref;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
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
			mPrefs.edit().remove(selection).apply();
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
			ringLock.lock();
			for(String t:targets) {
				Log.d(TAG,"Forwarding insert to:"+t+":"+ring.get(t));
				new SendMessage().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message.toString(), ring.get(t));
			}
			ringLock.unlock();
		} catch (JSONException e) {
			e.printStackTrace();
		}




		return uri;
	}

	public String[] getTargets(String key) {
		ringLock.lock();
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
		if(ring.higherKey(targets[0]) == null)	//target[0] is last
			targets[1] = ring.firstKey();
			targets[2] = ring.higherKey(targets[1]);
		ringLock.unlock();
		return targets;
	}


	@Override
	public boolean onCreate() {
		Log.e(TAG,"oncreate");
		ringLock = new ReentrantLock();
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf(Integer.parseInt(portStr)*2);
		myHash = Util.genHash(String.valueOf(Integer.parseInt(portStr)));

		try {
			serverSocket = new ServerSocket(LISTEN_PORT);
			new AcceptMessages().execute();
		} catch (IOException e) {
			e.printStackTrace();
		}
		mPrefs = getContext().getSharedPreferences(SHARED_PREF_FILENAME, Context.MODE_PRIVATE);//multiprocess rquired?

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


		//determine if this is a crash start or fresh start
		if(mPrefs.getString("freshstart",null) == null) {
			//fresh start. Mark it as such bu storing someting in this key
			mPrefs.edit().putString("freshstart","yes").commit();
		}
		else {
			//recovering from a crash. Remove from prefs and start synch
//			mPrefs.edit().remove("freshstart").commit();
			Log.d(TAG, "Starting synch after recovery");
			synch();
		}
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
				if(k.equals("freshstart"))
					continue;
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

	public void synch() {
		//synch from both next server.
		// Same as query * except this time we query only our next 2 guys, combine results and store them in our sharedprefs
		String[] targets = getTargets(myHash);

		JSONObject message = new JSONObject();
		try {
			message.put("type","query");
			message.put("extra","iamback");
			message.put("sender", myPort);
			HashMap<String,String> synched = new HashMap<String, String>();
			for(String t:targets) {
				String result = new SendMessage().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,message.toString(),ring.get(t)).get();
				if(result!=null) {
					//add contents of result into cursor
					JSONArray keyValus = new JSONArray(result);
					Log.d(TAG,"Synch: Received "+keyValus.length()+" entries from " + ring.get(t));
					for(int i=0;i<keyValus.length();i++) {
						JSONObject obj = keyValus.getJSONObject(i);
						if(synched.get(obj.getString("key"))!=null && !synched.get(obj.getString("key")).equals(obj.getString("value")) ) {
							Log.w(TAG,"Different values for key:" + obj.getString("key"));
						}
						//TODO: Compare versions somehow. synched is unused right now..
						storeLocally(obj.getString("key"),obj.getString("value"));
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
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		MatrixCursor cursor = new MatrixCursor(new String[]{COLUMN_KEY,COLUMN_VALUE});
		if(selection.equals("@")) {
			//return everything in this node
			Map<String, String> all = (Map<String, String>) mPrefs.getAll();
			for(String k:all.keySet()) {
				if(k.equals("freshstart"))
					continue;
				cursor.addRow(new Object[]{k,all.get(k)});
			}
		} else if(selection.equals("*")) {
			//everything in dynamo
			JSONObject message = new JSONObject();
			try {
				message.put("type","query");
				message.put("sender", myPort);
				ringLock.lock();
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
				ringLock.unlock();
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
					String latestReply = null;
					ringLock.lock();
					for(String t:targets) {
						Log.d(TAG,"Forwarding query to:"+t+":"+ring.get(t));
						String reply = new SendMessage().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message.toString(), ring.get(t)).get();
						if(reply!=null) {
							//TODO: Compare latestReply with reply and update latestReply to be the actual latest version of teh retrieved data..
							latestReply = reply;	//value will be available in "content" key directly
						}
					}
					ringLock.unlock();
					if(latestReply==null) {
						Log.w(TAG,"Single query failed..none of the 3 targets responded with a value!");
					}
					cursor.addRow(new Object[]{selection,latestReply});
				} catch (JSONException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
		}

		return cursor;
	}

	private void storeLocally(String key, String value) {
		Log.w(TAG,"Message with hash:"+Util.genHash(key)+" stored locally("+myHash+")");
		Log.w(TAG,"storeLocally KV:"+key+":"+value);
		mPrefs.edit().putString(key,value).apply();
	}

	//Only for thread safety do we bother running this on the main thread
	private void removeFromRing(final String avdPort) {
		Handler mainHandler = new Handler(Looper.getMainLooper());
		Runnable myRunnable = new Runnable() {
			@Override
			public void run() {
				String correspondingKey = null;
				for(String k:ring.keySet()) {
					if(ring.get(k).equals(avdPort)) {
						correspondingKey = k;
						break;
					}
				}
				if(correspondingKey!=null) {
					ringLock.lock();
					ring.remove(correspondingKey);
					ringLock.unlock();
					Log.w(TAG, avdPort + " removed from ring");
					Toast.makeText(getContext(), avdPort + " removed from ring", Toast.LENGTH_SHORT).show();
				}
				else {
					Log.e(TAG, "Key not found in ring");
				}
			}
		};
		mainHandler.post(myRunnable);
	}

	private void addToRing(final String avdPort) {
		Handler mainHandler = new Handler(Looper.getMainLooper());
		Runnable myRunnable = new Runnable() {
			@Override
			public void run() {
				int port = Integer.parseInt(avdPort)/2;	//55xx number
				ringLock.lock();
				ring.put(Util.genHash(String.valueOf(port)),avdPort);
				ringLock.unlock();
				Log.w(TAG, avdPort + " added to  ring");
				Toast.makeText(getContext(), avdPort + " added to ring", Toast.LENGTH_SHORT).show();
			}
		};
		mainHandler.post(myRunnable);
	}
	private class SendMessage extends AsyncTask<String,Void,String> {

		@Override
		protected String doInBackground(String... strings) {
			Log.d(TAG,myPort+ "sending to "+strings[1]);
			String message = strings[0] + "\n";
			int portNo = Integer.valueOf(strings[1]);
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						portNo);
				socket.setSoTimeout(1000);
				PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(),"UTF-8")),true);
				BufferedReader r=new BufferedReader(new InputStreamReader(socket.getInputStream(),"UTF-8"));
				writer.println(message);

				String rec = r.readLine();
				if(rec!=null) {
					JSONObject obj = new JSONObject(rec);
					if(obj.get("type").equals("query_reply")) {
						return obj.getString("content");
					}
					else if(obj.get("type").equals("singlequery_reply")) {
						return obj.getString("content");
					}
				}
				else {
					Log.v(TAG,"SendMessage received null");
					removeFromRing(strings[1]);
				}
				writer.close();
				r.close();
				socket.close();

			} catch (UnknownHostException ex) {
				//ex.printStackTrace();
				Log.e(TAG,strings[1] + " failed");
				removeFromRing(strings[1]);
			} catch(SocketTimeoutException ex) {
				Log.e(TAG,strings[1] + " failed");
				removeFromRing(strings[1]);
			} catch(StreamCorruptedException ex) {
				Log.e(TAG,strings[1] + " failed");
				removeFromRing(strings[1]);
			} catch (EOFException ex) {
				Log.e(TAG,strings[1] + " failed");
				removeFromRing(strings[1]);
			}  catch (IOException e) {
				//e.printStackTrace();
				Log.e(TAG,strings[1] + " failed");
				removeFromRing(strings[1]);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return null;
		}
	}



	private class AcceptMessages extends AsyncTask<String,String,Void> {

		@Override
		protected Void doInBackground(String... strings) {
			String sender = "";

			try {
				while (true) {
					Socket client = serverSocket.accept();
					Log.d(TAG,"Accepted connection");
					BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream(),"UTF-8"));
					PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(client.getOutputStream(),"UTF-8")),true);
					String str = reader.readLine();
					if(str!=null) {
						JSONObject received = new JSONObject(str);
						sender = received.getString("sender");
						if(received.getString("type").equals("insert")) {
							storeLocally(received.getString("key"),received.getString("value"));
							JSONObject obj = new JSONObject();
							obj.put("type","bullshitack");
							writer.println(obj.toString());
						}
						else if(received.getString("type").equals("query")) {
							if("iamback".equals(received.optString("extra"))) {
								//add sender back to the ring
								addToRing(sender);
							}
							JSONObject reply = new JSONObject();
							reply.put("content",fetchLocalData());
							reply.put("type","query_reply");
							writer.println(reply.toString());

							//maybe read again?

						}
						else if(received.getString("type").equals("singlequery")) {
							JSONObject reply = new JSONObject();
							reply.put("content",mPrefs.getString(received.getString("key"),null));
							reply.put("type","singlequery_reply");
							writer.println(reply.toString());

						}

						else {
							Log.e(TAG,"Invalid message type!");
						}
					}
					else {
						Log.v(TAG,"AcceptMessage received null");
						removeFromRing(sender);
					}

					writer.close();
					reader.close();
					client.close();

				}
			}catch(SocketTimeoutException ex) {
				Log.e(TAG,"AcceptMessages:" + sender + " failed");
				removeFromRing(sender);
			} catch(StreamCorruptedException ex) {
				Log.e(TAG,"AcceptMessages:" + sender + " failed");
				removeFromRing(sender);
			} catch (EOFException ex) {
				Log.e(TAG,"AcceptMessages:" + sender + " failed");
				removeFromRing(sender);
			} catch(IOException e){
//				e.printStackTrace();
				Log.e(TAG,"AcceptMessages:" + sender + " failed");
				removeFromRing(sender);
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

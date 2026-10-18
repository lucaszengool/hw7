package cis5550.flame;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import cis5550.kvs.KVSClient;
import cis5550.tools.*;
import cis5550.tools.Partitioner.Partition;

public class FlameContextImpl implements FlameContext {

  private static final Logger logger = Logger.getLogger(FlameContextImpl.class);

  /* Shared by all jobs, so that two jobs that run at the same time never pick the same table names */

  private static final AtomicInteger jobCounter = new AtomicInteger(0);

  private final String jarName;
  private final String jobID;
  private final KVSClient kvs;
  private final StringBuilder output;
  private boolean outputWasCalled;
  private int nextTableNumber;
  private int keyRangesPerWorker;

  public FlameContextImpl(String jarNameArg) {
    jarName = jarNameArg;

    // The start time makes the table names unique even if the coordinator is restarted while the KVS keeps
    // running (and still holds the tables from earlier jobs); the counter separates jobs that start in the
    // same millisecond.

    jobID = System.currentTimeMillis()+"-"+jobCounter.incrementAndGet();

    // Each job gets its own KVS client, so the list of KVS workers is fetched fresh when the job starts.
    // The Flame workers will do the same, so both sides agree on which KVS worker holds which keys.

    kvs = new KVSClient(Coordinator.kvs.getCoordinator());
    output = new StringBuilder();
    outputWasCalled = false;
    nextTableNumber = 1;
    keyRangesPerWorker = 1;
  }

  public KVSClient getKVS() {
    return kvs;
  }

  public synchronized void output(String s) {
    outputWasCalled = true;
    output.append(s);
  }

  // Returns the concatenation of everything the job passed to output(), or null if output() was never called

  synchronized String getOutput() {
    return outputWasCalled ? output.toString() : null;
  }

  synchronized String newTableName() {
    return "job-"+jobID+"-"+(nextTableNumber++);
  }

  public FlameRDD parallelize(List<String> list) throws Exception {
    String tableName = newTableName();
    int i = 0;
    for (String s : list) {
      if (s == null)
        continue;

      // Hashing the sequence number gives keys that are unique, but spread across the whole key space, and
      // therefore across all the KVS workers

      kvs.put(tableName, Hasher.hash(""+(++i)), "value", s);
    }

    return new FlameRDDImpl(this, tableName);
  }

  /* fromTable() works like any other operation, except that its input is an ordinary KVS table rather than an RDD */

  public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
    String output = invokeOperation("/context/fromTable", Serializer.objectToByteArray(lambda), tableName, null, null);
    return new FlameRDDImpl(this, output);
  }

  public synchronized void setConcurrencyLevel(int keyRangesPerWorkerArg) {
    if (keyRangesPerWorkerArg < 1)
      throw new IllegalArgumentException("The concurrency level must be at least 1, not "+keyRangesPerWorkerArg);
    keyRangesPerWorker = keyRangesPerWorkerArg;
  }

  /* saveAsTable(): renaming the table is enough, since an RDD (or PairRDD) already is a table in the right format. An
     RDD that has no elements has no table in the KVS yet, so there is nothing to rename; the new name will then simply
     refer to an empty table.

     KVSClient.rename() sends the request to every KVS worker and returns false if any of them doesn't have the table.
     That is normal whenever all of an RDD's keys happen to live on some of the workers, so the return value alone does
     not say much; what matters is that the target name was free beforehand and no rows are left under the old name. */

  void renameTable(String oldName, String newName) throws Exception {
    if (newName == null)
      throw new IllegalArgumentException("saveAsTable() needs a table name");
    if (newName.equals(oldName))
      return;
    if (kvs.count(newName) > 0)
      throw new Exception("Cannot save as '"+newName+"' because a table with that name already exists in the KVS");
    if (!kvs.rename(oldName, newName) && (kvs.count(oldName) > 0))
      throw new Exception("The KVS refused to rename table '"+oldName+"' to '"+newName+"'");
  }

  /* This is the generic part of every RDD/PairRDD operation. The coordinator asks the Partitioner to split the key
     space into ranges and to assign each range to a Flame worker (preferably one that runs on the same machine as
     the KVS worker that holds these keys); then it sends a POST to 'operation' on every assigned worker, in parallel,
     and waits for all of them to finish. Each worker reads its range of 'inputTable', applies the operation, and
     writes the results to 'outputTableOrNull' (or a fresh table, if that is null). The lambda, if any, travels in the
     body of the request; any additional arguments travel as query parameters. Returns the name of the output table. */

  String invokeOperation(String operation, byte[] lambdaOrNull, String inputTable, String outputTableOrNull, Map<String,String> extraParams) throws Exception {
    return invokeOperation(operation, lambdaOrNull, inputTable, outputTableOrNull, extraParams, false);
  }

  String invokeOperation(String operation, byte[] lambdaOrNull, String inputTable, String outputTableOrNull, Map<String,String> extraParams, boolean oneRequestPerWorker) throws Exception {
    String outputTable = (outputTableOrNull != null) ? outputTableOrNull : newTableName();
    runOnWorkers(operation, lambdaOrNull, inputTable, outputTable, extraParams, oneRequestPerWorker);
    return outputTable;
  }

  /* Does the actual work for invokeOperation(), and also for the operations that have no output table (such as fold),
     which is why 'outputTableOrNull' may be null here. Normally, each key range gets a request of its own, so that a
     worker with several ranges works on them in parallel; with 'oneRequestPerWorker', each worker instead gets a single
     request that lists all of its ranges (mapPartitions needs this). Returns the bodies of the responses. */

  Vector<String> runOnWorkers(String operation, byte[] lambdaOrNull, String inputTable, String outputTableOrNull, Map<String,String> extraParams, boolean oneRequestPerWorker) throws Exception {
    Partitioner partitioner = new Partitioner();
    synchronized (this) {
      partitioner.setKeyRangesPerWorker(keyRangesPerWorker);
    }
    int numKVSWorkers = kvs.numWorkers();
    for (int i=0; i<numKVSWorkers; i++) {
      String address = kvs.getWorkerAddress(i);
      String id = kvs.getWorkerID(i);
      if (i < numKVSWorkers-1) {
        partitioner.addKVSWorker(address, id, kvs.getWorkerID(i+1));
      } else {

        // The last KVS worker is responsible both for the keys above its own ID and for the keys below the lowest
        // ID, so it gets two ranges

        partitioner.addKVSWorker(address, id, null);
        partitioner.addKVSWorker(address, null, kvs.getWorkerID(0));
      }
    }

    for (String flameWorker : Coordinator.getWorkers())
      partitioner.addFlameWorker(flameWorker);

    Vector<Partition> partitions = partitioner.assignPartitions();
    if (partitions == null)
      throw new Exception("Cannot run "+operation+" because there are no Flame workers");

    final List<List<Partition>> requests = new ArrayList<List<Partition>>();
    if (oneRequestPerWorker) {
      Map<String,List<Partition>> byWorker = new LinkedHashMap<String,List<Partition>>();
      for (Partition p : partitions)
        byWorker.computeIfAbsent(p.assignedFlameWorker, w -> new ArrayList<Partition>()).add(p);
      requests.addAll(byWorker.values());
    } else {
      for (Partition p : partitions)
        requests.add(Collections.singletonList(p));
    }

    // Build the requests. A single range goes into 'from' and 'to'; several ranges go into 'from1', 'to1', 'from2', ...,
    // with 'ranges' saying how many there are. A range that is open on one side has a null boundary; we leave out the
    // corresponding parameter in that case, so the worker does not mistake it for the string "null".

    final byte body[] = (lambdaOrNull != null) ? lambdaOrNull : new byte[0];
    final String urls[] = new String[requests.size()];
    final String workers[] = new String[requests.size()];
    for (int i=0; i<requests.size(); i++) {
      List<Partition> ranges = requests.get(i);
      workers[i] = ranges.get(0).assignedFlameWorker;
      StringBuilder url = new StringBuilder("http://"+workers[i]+operation);
      url.append("?input=").append(encode(inputTable));
      if (outputTableOrNull != null)
        url.append("&output=").append(encode(outputTableOrNull));
      url.append("&kvs=").append(encode(kvs.getCoordinator()));
      if (ranges.size() == 1) {
        appendRange(url, ranges.get(0), "");
      } else {
        url.append("&ranges=").append(ranges.size());
        for (int r=0; r<ranges.size(); r++)
          appendRange(url, ranges.get(r), ""+(r+1));
      }
      if (extraParams != null)
        for (Map.Entry<String,String> e : extraParams.entrySet())
          url.append("&").append(encode(e.getKey())).append("=").append(encode(e.getValue()));
      urls[i] = url.toString();
    }

    // Send all the requests at the same time, so that the workers really do work in parallel

    final String errors[] = new String[requests.size()];
    final String results[] = new String[requests.size()];
    Thread threads[] = new Thread[requests.size()];
    for (int i=0; i<requests.size(); i++) {
      final int j = i;
      threads[i] = new Thread(operation+" #"+(i+1)) {
        public void run() {
          try {
            HTTP.Response r = HTTP.doRequest("POST", urls[j], body);
            if (r == null)
              errors[j] = "Worker "+workers[j]+" did not respond";
            else if (r.statusCode() != 200)
              errors[j] = "Worker "+workers[j]+" returned status "+r.statusCode()+" for "+requests.get(j)+":\n"+new String(r.body(), StandardCharsets.UTF_8);
            else
              results[j] = new String(r.body(), StandardCharsets.UTF_8);
          } catch (Exception e) {
            errors[j] = "Request to worker "+workers[j]+" failed: "+e;
          }
        }
      };
      threads[i].start();
    }

    for (int i=0; i<threads.length; i++) {
      try {
        threads[i].join();
      } catch (InterruptedException ie) {
      }
    }

    StringBuilder failures = new StringBuilder();
    for (String error : errors)
      if (error != null)
        failures.append(error).append("\n");
    if (failures.length() > 0) {
      logger.error(operation+" failed on at least one worker:\n"+failures);
      throw new Exception(operation+" failed on at least one worker:\n"+failures);
    }

    return new Vector<String>(Arrays.asList(results));
  }

  private static void appendRange(StringBuilder url, Partition p, String suffix) {
    if (p.fromKey != null)
      url.append("&from").append(suffix).append("=").append(encode(p.fromKey));
    if (p.toKeyExclusive != null)
      url.append("&to").append(suffix).append("=").append(encode(p.toKeyExclusive));
  }

  static String encode(String s) {
    try {
      return URLEncoder.encode(s, "UTF-8");
    } catch (java.io.UnsupportedEncodingException uee) {
      throw new RuntimeException(uee);
    }
  }
}

package cis5550.flame;

import java.util.*;

import cis5550.kvs.Row;
import cis5550.tools.Serializer;

/* An RDD is a KVS table in which each row has a unique key and a single column called 'value' that holds one element. */

public class FlameRDDImpl implements FlameRDD {

  private final FlameContextImpl context;
  private String tableName;
  private boolean destroyed;

  FlameRDDImpl(FlameContextImpl contextArg, String tableNameArg) {
    context = contextArg;
    tableName = tableNameArg;
    destroyed = false;
  }

  String getTableName() {
    return tableName;
  }

  // Every method calls this first, so that an RDD can no longer be used once its table has been deleted

  void checkUsable() throws Exception {
    if (destroyed)
      throw new IllegalStateException("This RDD has been destroyed");
  }

  public int count() throws Exception {
    checkUsable();
    return context.getKVS().count(tableName);
  }

  public void saveAsTable(String tableNameArg) throws Exception {
    checkUsable();
    context.renameTable(tableName, tableNameArg);
    tableName = tableNameArg;
  }

  /* distinct: the workers use the hash of each element as its row key, so all copies of an element land in the same
     row and overwrite each other */

  public FlameRDD distinct() throws Exception {
    checkUsable();
    String output = context.invokeOperation("/rdd/distinct", null, tableName, null, null);
    return new FlameRDDImpl(context, output);
  }

  public void destroy() throws Exception {
    checkUsable();
    context.getKVS().delete(tableName);
    destroyed = true;
  }

  public Vector<String> take(int num) throws Exception {
    checkUsable();
    Vector<String> result = new Vector<String>();
    Iterator<Row> rows = context.getKVS().scan(tableName);
    while ((result.size() < num) && rows.hasNext()) {
      String value = rows.next().get("value");
      if (value != null)
        result.add(value);
    }
    return result;
  }

  /* fold: each worker folds the elements in its own key range, starting from the zero element, and sends back its
     accumulator. The coordinator then folds the accumulators of the key ranges that had any elements at all; ranges
     without elements are skipped, so the zero element isn't folded in once more for each of them. */

  public String fold(String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception {
    checkUsable();
    Map<String,String> params = new HashMap<String,String>();
    if (zeroElement != null)
      params.put("zero", zeroElement);
    Vector<String> partials = context.runOnWorkers("/rdd/fold", Serializer.objectToByteArray(lambda), tableName, null, params, false);

    String result = null;
    boolean haveResult = false;
    for (String partial : partials) {
      if (Worker.isEmptyAccumulator(partial))
        continue;
      String accumulator = Worker.decodeAccumulator(partial);
      result = haveResult ? lambda.op(result, accumulator) : accumulator;
      haveResult = true;
    }

    return haveResult ? result : zeroElement;
  }

  public List<String> collect() throws Exception {
    checkUsable();
    List<String> result = new ArrayList<String>();
    Iterator<Row> rows = context.getKVS().scan(tableName);
    while (rows.hasNext()) {
      String value = rows.next().get("value");
      if (value != null)
        result.add(value);
    }
    return result;
  }

  public FlameRDD flatMap(StringToIterable lambda) throws Exception {
    checkUsable();
    String output = context.invokeOperation("/rdd/flatMap", Serializer.objectToByteArray(lambda), tableName, null, null);
    return new FlameRDDImpl(context, output);
  }

  public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
    checkUsable();
    String output = context.invokeOperation("/rdd/flatMapToPair", Serializer.objectToByteArray(lambda), tableName, null, null);
    return new FlamePairRDDImpl(context, output);
  }

  public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
    checkUsable();
    String output = context.invokeOperation("/rdd/mapToPair", Serializer.objectToByteArray(lambda), tableName, null, null);
    return new FlamePairRDDImpl(context, output);
  }

  // ----------------------- EXTRA CREDIT ITEMS ---------------------------

  /* intersection() runs in three parallel passes. First, the workers copy the elements of this RDD into a temporary
     table, using the hash of each element as its row key and 'a' as the column; then they do the same with the
     elements of the other RDD, using 'b' as the column. Equal elements land in the same row, and duplicates simply
     overwrite each other. In the third pass, the workers keep the rows that have both columns. */

  public FlameRDD intersection(FlameRDD r) throws Exception {
    checkUsable();
    if (!(r instanceof FlameRDDImpl))
      throw new IllegalArgumentException("intersection() needs an RDD that was created by this Flame context");
    ((FlameRDDImpl)r).checkUsable();

    String tagged = context.newTableName();
    context.invokeOperation("/rdd/tagByValue", null, tableName, tagged, Map.of("tag", "a"));
    context.invokeOperation("/rdd/tagByValue", null, ((FlameRDDImpl)r).getTableName(), tagged, Map.of("tag", "b"));
    String output = context.invokeOperation("/rdd/keepIfTaggedTwice", null, tagged, null, null);
    deleteQuietly(tagged);
    return new FlameRDDImpl(context, output);
  }

  public FlameRDD sample(double f) throws Exception {
    checkUsable();
    String output = context.invokeOperation("/rdd/sample", null, tableName, null, Map.of("probability", ""+f));
    return new FlameRDDImpl(context, output);
  }

  /* groupBy() runs in two parallel passes. First, the workers apply the lambda to each element and build a PairRDD
     that contains (k, element) for each element; then they turn each row of that PairRDD into a single pair whose value
     is the comma-separated list of all the elements in the row. */

  public FlamePairRDD groupBy(StringToString lambda) throws Exception {
    checkUsable();
    String grouped = context.invokeOperation("/rdd/groupBy", Serializer.objectToByteArray(lambda), tableName, null, null);
    String output = context.invokeOperation("/pairRdd/joinValues", null, grouped, null, null);
    deleteQuietly(grouped);
    return new FlamePairRDDImpl(context, output);
  }

  public FlameRDD filter(StringToBoolean lambda) throws Exception {
    checkUsable();
    String output = context.invokeOperation("/rdd/filter", Serializer.objectToByteArray(lambda), tableName, null, null);
    return new FlameRDDImpl(context, output);
  }

  /* mapPartitions: each worker gets a single request that covers all of its key ranges, so it invokes the lambda exactly
     once, with an iterator over all the elements it is responsible for */

  public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
    checkUsable();
    String output = context.invokeOperation("/rdd/mapPartitions", Serializer.objectToByteArray(lambda), tableName, null, null, true);
    return new FlameRDDImpl(context, output);
  }

  private void deleteQuietly(String temporaryTable) {
    try {
      context.getKVS().delete(temporaryTable);
    } catch (Exception e) {
      // The table may never have been created (for instance, if the input was empty); either way it's gone
    }
  }
}

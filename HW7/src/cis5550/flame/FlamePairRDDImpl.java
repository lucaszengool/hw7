package cis5550.flame;

import java.util.*;

import cis5550.kvs.Row;
import cis5550.tools.Serializer;

/* A PairRDD is a KVS table with one row per distinct key k; each column of that row holds one value v, so that the
   row represents the pairs (k, v) for all of its columns. The column names only need to be unique within the row. */

public class FlamePairRDDImpl implements FlamePairRDD {

  private final FlameContextImpl context;
  private String tableName;
  private boolean destroyed;

  FlamePairRDDImpl(FlameContextImpl contextArg, String tableNameArg) {
    context = contextArg;
    tableName = tableNameArg;
    destroyed = false;
  }

  String getTableName() {
    return tableName;
  }

  // Every method calls this first, so that a PairRDD can no longer be used once its table has been deleted

  void checkUsable() throws Exception {
    if (destroyed)
      throw new IllegalStateException("This PairRDD has been destroyed");
  }

  public List<FlamePair> collect() throws Exception {
    checkUsable();
    List<FlamePair> result = new ArrayList<FlamePair>();
    Iterator<Row> rows = context.getKVS().scan(tableName);
    while (rows.hasNext()) {
      Row row = rows.next();
      for (String column : row.columns())
        result.add(new FlamePair(row.key(), row.get(column)));
    }
    return result;
  }

  public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
    checkUsable();
    Map<String,String> params = new HashMap<String,String>();
    if (zeroElement != null)
      params.put("zero", zeroElement);
    String output = context.invokeOperation("/pairRdd/foldByKey", Serializer.objectToByteArray(lambda), tableName, null, params);
    return new FlamePairRDDImpl(context, output);
  }

  public void saveAsTable(String tableNameArg) throws Exception {
    checkUsable();
    context.renameTable(tableName, tableNameArg);
    tableName = tableNameArg;
  }

  public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
    checkUsable();
    String output = context.invokeOperation("/pairRdd/flatMap", Serializer.objectToByteArray(lambda), tableName, null, null);
    return new FlameRDDImpl(context, output);
  }

  public void destroy() throws Exception {
    checkUsable();
    context.getKVS().delete(tableName);
    destroyed = true;
  }

  public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
    checkUsable();
    String output = context.invokeOperation("/pairRdd/flatMapToPair", Serializer.objectToByteArray(lambda), tableName, null, null);
    return new FlamePairRDDImpl(context, output);
  }

  /* join: the workers scan their key ranges of this PairRDD and look up each key in the other PairRDD. Both tables use
     the key of a pair as its row key, so the matching row, if any, is in the same key range. */

  public FlamePairRDD join(FlamePairRDD other) throws Exception {
    String output = context.invokeOperation("/pairRdd/join", null, tableName, null, Map.of("other", tableOf(other)));
    return new FlamePairRDDImpl(context, output);
  }

  // ----------------------- EXTRA CREDIT ITEMS ---------------------------

  /* cogroup: like join, except that a key needs to be present in only one of the two PairRDDs. The workers handle the
     keys from this PairRDD first, and then the keys that appear only in the other one. */

  public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
    String output = context.invokeOperation("/pairRdd/cogroup", null, tableName, null, Map.of("other", tableOf(other)));
    return new FlamePairRDDImpl(context, output);
  }

  private String tableOf(FlamePairRDD other) throws Exception {
    checkUsable();
    if (!(other instanceof FlamePairRDDImpl))
      throw new IllegalArgumentException("The other PairRDD must have been created by this Flame context");
    ((FlamePairRDDImpl)other).checkUsable();
    return ((FlamePairRDDImpl)other).getTableName();
  }
}

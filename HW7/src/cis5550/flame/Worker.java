package cis5550.flame;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.net.*;
import java.io.*;

import static cis5550.webserver.Server.*;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import cis5550.kvs.*;
import cis5550.webserver.Request;

class Worker extends cis5550.generic.Worker {

  /* What an operation does with one row of its input table. 'lambda' is the deserialized lambda from the request
     body (or null if there was none), and results go into 'outputTable' via 'kvs'. */

  interface RowOperation {
    void apply(Row row, Object lambda, Request request, KVSClient kvs, String outputTable) throws Exception;
  }

  /* What an operation does with its entire key range of the input table, for the operations that need to see all of
     the range at once. Whatever it returns becomes the body of the response ("OK" if it returns null). */

  interface RangeOperation {
    String apply(Iterator<Row> rows, Object lambda, Request request, KVSClient kvs, String outputTable) throws Exception;
  }

  static File myJAR;

	public static void main(String args[]) {
    if (args.length != 2) {
    	System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
    	System.exit(1);
    }

    int port = Integer.parseInt(args[0]);
    String server = args[1];
	  startPingThread(server, ""+port, port);
    myJAR = new File("__worker"+port+"-current.jar");

  	port(port);

    post("/useJAR", (request,response) -> {
      FileOutputStream fos = new FileOutputStream(myJAR);
      fos.write(request.bodyAsBytes());
      fos.close();
      return "OK";
    });

    /* flatMap: each string in the Iterable the lambda returns becomes a new element. The output keys are derived from
       the (unique) input key and the position in the Iterable, so equal strings still end up in separate rows. */

    registerOperation("/rdd/flatMap", (row, lambda, request, kvs, outputTable) -> {
      String value = row.get("value");
      if (value == null)
        return;
      Iterable<String> results = ((FlameRDD.StringToIterable)lambda).op(value);
      if (results == null)
        return;
      int i = 0;
      for (String s : results) {
        if (s != null)
          kvs.put(outputTable, Hasher.hash(row.key()+"-"+i), "value", s);
        i++;
      }
    });

    /* mapToPair: the pair (k,v) goes into row k. The input key is unique, so it serves as the column name; that way,
       several pairs with the same k become several columns of the same row instead of overwriting each other. */

    registerOperation("/rdd/mapToPair", (row, lambda, request, kvs, outputTable) -> {
      String value = row.get("value");
      if (value == null)
        return;
      FlamePair pair = ((FlameRDD.StringToPair)lambda).op(value);
      if ((pair == null) || (pair._1() == null) || (pair._2() == null))
        return;
      kvs.put(outputTable, pair._1(), row.key(), pair._2());
    });

    /* foldByKey: one row is one key, so this worker sees all of the key's values. The accumulator starts at the zero
       element and is always the lambda's FIRST argument; the value is the second. */

    registerOperation("/pairRdd/foldByKey", (row, lambda, request, kvs, outputTable) -> {
      String accumulator = request.queryParams("zero");
      for (String column : row.columns())
        accumulator = ((FlamePairRDD.TwoStringsToString)lambda).op(accumulator, row.get(column));
      if (accumulator != null)
        kvs.put(outputTable, row.key(), "value", accumulator);
    });

    /* fromTable: the input is an ordinary KVS table, so the lambda gets the whole Row. The input key is unique, so it can
       serve as the key of the new element, too. */

    registerOperation("/context/fromTable", (row, lambda, request, kvs, outputTable) -> {
      String result = ((FlameContext.RowToString)lambda).op(row);
      if (result != null)
        kvs.put(outputTable, row.key(), "value", result);
    });

    /* flatMapToPair: like mapToPair, except that one element can produce several pairs. The column name combines the
       (unique) input key with the position in the Iterable, so no two pairs with the same key share a column. Keys can't
       contain spaces (that's the KVS's separator), so a space keeps the two parts apart; hashing keeps the name short. */

    registerOperation("/rdd/flatMapToPair", (row, lambda, request, kvs, outputTable) -> {
      String value = row.get("value");
      if (value == null)
        return;
      Iterable<FlamePair> results = ((FlameRDD.StringToPairIterable)lambda).op(value);
      if (results == null)
        return;
      int i = 0;
      for (FlamePair pair : results) {
        if ((pair != null) && (pair._1() != null) && (pair._2() != null))
          kvs.put(outputTable, pair._1(), Hasher.hash(row.key()+" "+i), pair._2());
        i++;
      }
    });

    /* distinct: all copies of an element hash to the same row key, so they overwrite each other and only one is left */

    registerOperation("/rdd/distinct", (row, lambda, request, kvs, outputTable) -> {
      String value = row.get("value");
      if (value != null)
        kvs.put(outputTable, Hasher.hash(value), "value", value);
    });

    /* fold: folds every element in this worker's key range into one accumulator and sends it back to the coordinator,
       which combines the accumulators from all the ranges. There is no output table. */

    registerRangeOperation("/rdd/fold", false, (rows, lambda, request, kvs, outputTable) -> {
      String accumulator = request.queryParams("zero");
      boolean rangeWasEmpty = true;
      while (rows.hasNext()) {
        String value = rows.next().get("value");
        if (value == null)
          continue;
        accumulator = ((FlamePairRDD.TwoStringsToString)lambda).op(accumulator, value);
        rangeWasEmpty = false;
      }
      return encodeAccumulator(rangeWasEmpty, accumulator);
    });

    /* PairRDD.flatMap: each column of row k is one pair (k, v). The output key combines the row key, the column name
       (which is unique within the row), and the position in the Iterable, so equal strings end up in separate rows. */

    registerOperation("/pairRdd/flatMap", (row, lambda, request, kvs, outputTable) -> {
      for (String column : row.columns()) {
        Iterable<String> results = ((FlamePairRDD.PairToStringIterable)lambda).op(new FlamePair(row.key(), row.get(column)));
        if (results == null)
          continue;
        int i = 0;
        for (String s : results) {
          if (s != null)
            kvs.put(outputTable, Hasher.hash(row.key()+" "+column+" "+i), "value", s);
          i++;
        }
      }
    });

    /* PairRDD.flatMapToPair: the same, except that the results are pairs, so they go into row k' of the output, in a
       column whose name is unique for the same reason */

    registerOperation("/pairRdd/flatMapToPair", (row, lambda, request, kvs, outputTable) -> {
      for (String column : row.columns()) {
        Iterable<FlamePair> results = ((FlamePairRDD.PairToPairIterable)lambda).op(new FlamePair(row.key(), row.get(column)));
        if (results == null)
          continue;
        int i = 0;
        for (FlamePair pair : results) {
          if ((pair != null) && (pair._1() != null) && (pair._2() != null))
            kvs.put(outputTable, pair._1(), Hasher.hash(row.key()+" "+column+" "+i), pair._2());
          i++;
        }
      }
    });

    /* join: row k of the input table holds the pairs (k, a) of the first PairRDD, and row k of the 'other' table holds
       the pairs (k, b) of the second one. Every combination becomes a pair (k, "a,b"). Its column name joins the hashes
       of the two column names, each of which is unique within its row, with a character that hashes never contain. */

    registerOperation("/pairRdd/join", (row, lambda, request, kvs, outputTable) -> {
      Row otherRow = kvs.getRow(requiredParam(request, "other"), row.key());
      if (otherRow == null)
        return;
      for (String column : row.columns())
        for (String otherColumn : otherRow.columns())
          kvs.put(outputTable, row.key(), Hasher.hash(column)+"_"+Hasher.hash(otherColumn), row.get(column)+","+otherRow.get(otherColumn));
    });

    // ----------------------- EXTRA CREDIT ITEMS ---------------------------

    /* sample: every element, including each copy of a repeated element, is kept independently with the given probability */

    registerOperation("/rdd/sample", (row, lambda, request, kvs, outputTable) -> {
      double probability = Double.parseDouble(request.queryParams("probability"));
      String value = row.get("value");
      if ((value != null) && (ThreadLocalRandom.current().nextDouble() < probability))
        kvs.put(outputTable, row.key(), "value", value);
    });

    /* intersection, passes 1 and 2: equal elements hash to the same row, and the 'tag' column records which RDD they
       came from. Row keys are hashes rather than the elements themselves, so elements can contain any characters. */

    registerOperation("/rdd/tagByValue", (row, lambda, request, kvs, outputTable) -> {
      String value = row.get("value");
      if (value != null)
        kvs.put(outputTable, Hasher.hash(value), request.queryParams("tag"), value);
    });

    /* intersection, pass 3: a row with both tags is an element that occurs in both RDDs; it is written exactly once */

    registerOperation("/rdd/keepIfTaggedTwice", (row, lambda, request, kvs, outputTable) -> {
      String a = row.get("a"), b = row.get("b");
      if ((a != null) && (b != null))
        kvs.put(outputTable, row.key(), "value", a);
    });

    /* groupBy, pass 1: like mapToPair with (lambda(x), x), so all elements with the same k end up in row k */

    registerOperation("/rdd/groupBy", (row, lambda, request, kvs, outputTable) -> {
      String value = row.get("value");
      if (value == null)
        return;
      String key = ((FlameRDD.StringToString)lambda).op(value);
      if (key != null)
        kvs.put(outputTable, key, row.key(), value);
    });

    /* groupBy, pass 2: turn row k into the single pair (k, "x1,x2,..."). The elements are sorted, so the result
       doesn't depend on the order in which the KVS happens to return the columns. */

    registerOperation("/pairRdd/joinValues", (row, lambda, request, kvs, outputTable) -> {
      List<String> values = new ArrayList<String>();
      for (String column : row.columns())
        values.add(row.get(column));
      Collections.sort(values);
      kvs.put(outputTable, row.key(), "value", String.join(",", values));
    });

    /* filter: an element is copied, with its key, if and only if the predicate holds for it */

    registerOperation("/rdd/filter", (row, lambda, request, kvs, outputTable) -> {
      String value = row.get("value");
      if ((value != null) && ((FlameRDD.StringToBoolean)lambda).op(value))
        kvs.put(outputTable, row.key(), "value", value);
    });

    /* mapPartitions: the lambda is invoked once for this key range, with an iterator over its elements. The output keys
       need to be unique across all ranges, so each range numbers its results under an ID of its own. */

    registerRangeOperation("/rdd/mapPartitions", true, (rows, lambda, request, kvs, outputTable) -> {
      Iterator<String> results = ((FlameRDD.IteratorToIterator)lambda).op(new ValueIterator(rows));
      if (results == null)
        return null;
      String rangeID = UUID.randomUUID().toString();
      int i = 0;
      while (results.hasNext()) {
        String s = results.next();
        if (s != null)
          kvs.put(outputTable, Hasher.hash(rangeID+" "+i), "value", s);
        i++;
      }
      return null;
    });

    /* cogroup: for each key k in this range, the output gets the pair (k, "[X],[Y]"), where X and Y list the values
       of k in this PairRDD and in the 'other' one. The keys that are in this PairRDD come first; then the same range
       of the other table is scanned for the keys that only it has. Both lists are sorted, so the result doesn't depend
       on the order in which the KVS happens to return the columns. */

    registerRangeOperation("/pairRdd/cogroup", true, (rows, lambda, request, kvs, outputTable) -> {
      String inputTable = request.queryParams("input");
      String otherTable = requiredParam(request, "other");
      while (rows.hasNext()) {
        Row row = rows.next();
        kvs.put(outputTable, row.key(), "value", "["+sortedValues(row)+"],["+sortedValues(kvs.getRow(otherTable, row.key()))+"]");
      }
      Iterator<Row> otherRows = scanRanges(kvs, otherTable, request);
      while (otherRows.hasNext()) {
        Row otherRow = otherRows.next();
        if (kvs.getRow(inputTable, otherRow.key()) == null)
          kvs.put(outputTable, otherRow.key(), "value", "[],["+sortedValues(otherRow)+"]");
      }
      return null;
    });
	}

  /* Every operation follows the same pattern: decode the parameters, deserialize the lambda (loading any classes it
     needs from the job's JAR file), scan this worker's range of the input table, and apply the operation to each row.
     A missing 'from' or 'to' means that the range is open on that side. */

  static void registerOperation(String path, RowOperation operation) {
    registerRangeOperation(path, true, (rows, lambda, request, kvs, outputTable) -> {
      while (rows.hasNext())
        operation.apply(rows.next(), lambda, request, kvs, outputTable);
      return null;
    });
  }

  /* The same, for operations that work on the whole range at once. Operations that only send a result back to the
     coordinator (such as fold) don't need an output table. */

  static void registerRangeOperation(String path, boolean needsOutputTable, RangeOperation operation) {
    post(path, (request, response) -> {
      String inputTable = request.queryParams("input");
      String outputTable = request.queryParams("output");
      String kvsCoordinator = request.queryParams("kvs");
      if ((inputTable == null) || (kvsCoordinator == null) || (needsOutputTable && (outputTable == null))) {
        response.status(400, "Bad request");
        return "Missing parameter: "+path+" requires 'input', "+(needsOutputTable ? "'output', " : "")+"and 'kvs'";
      }

      Object lambda = null;
      byte body[] = request.bodyAsBytes();
      if ((body != null) && (body.length > 0)) {
        lambda = Serializer.byteArrayToObject(body, myJAR);
        if (lambda == null) {
          response.status(400, "Bad request");
          return "Could not deserialize the lambda for "+path;
        }
      }

      try {
        KVSClient kvs = new KVSClient(kvsCoordinator);
        Iterator<Row> rows = scanRanges(kvs, inputTable, request);
        String result = operation.apply(rows, lambda, request, kvs, outputTable);
        return (result != null) ? result : "OK";
      } catch (Throwable e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        response.status(500, "Internal Server Error");
        return path+" failed on input table '"+inputTable+"':\n"+sw;
      }
    });
  }

  /* Scans the key range(s) that this request covers: usually just 'from'-'to', but if the request has a 'ranges'
     parameter, the ranges 'from1'-'to1', 'from2'-'to2', and so on, one after the other */

  static Iterator<Row> scanRanges(KVSClient kvs, String table, Request request) throws Exception {
    String numRanges = request.queryParams("ranges");
    if (numRanges == null)
      return kvs.scan(table, request.queryParams("from"), request.queryParams("to"));

    List<Row> none = Collections.emptyList();
    Iterator<Row> result = none.iterator();
    for (int i=Integer.parseInt(numRanges); i>=1; i--) {
      final Iterator<Row> rest = result;
      final Iterator<Row> range = kvs.scan(table, request.queryParams("from"+i), request.queryParams("to"+i));
      result = new Iterator<Row>() {
        public boolean hasNext() {
          return range.hasNext() || rest.hasNext();
        }
        public Row next() {
          return range.hasNext() ? range.next() : rest.next();
        }
      };
    }
    return result;
  }

  static String requiredParam(Request request, String name) {
    String value = request.queryParams(name);
    if (value == null)
      throw new IllegalArgumentException("Missing parameter '"+name+"'");
    return value;
  }

  // The values in all the columns of a PairRDD row, sorted and separated by commas ("" if there is no such row)

  static String sortedValues(Row rowOrNull) {
    if (rowOrNull == null)
      return "";
    List<String> values = new ArrayList<String>();
    for (String column : rowOrNull.columns())
      values.add(rowOrNull.get(column));
    Collections.sort(values);
    return String.join(",", values);
  }

  /* fold sends its accumulator back in the body of the response. The first character says what follows, so the
     coordinator can tell a key range without any elements apart from an accumulator that is null or empty. */

  static String encodeAccumulator(boolean rangeWasEmpty, String accumulator) {
    if (rangeWasEmpty)
      return "E";
    return (accumulator == null) ? "N" : "V"+accumulator;
  }

  static boolean isEmptyAccumulator(String encoded) {
    return encoded.startsWith("E");
  }

  static String decodeAccumulator(String encoded) {
    return encoded.startsWith("V") ? encoded.substring(1) : null;
  }

  /* Turns a scan of an RDD's table into an iterator over its elements, for mapPartitions */

  static class ValueIterator implements Iterator<String> {
    private final Iterator<Row> rows;
    private String next;

    ValueIterator(Iterator<Row> rowsArg) {
      rows = rowsArg;
      advance();
    }

    private void advance() {
      next = null;
      while ((next == null) && rows.hasNext())
        next = rows.next().get("value");
    }

    public boolean hasNext() {
      return next != null;
    }

    public String next() {
      if (next == null)
        throw new NoSuchElementException();
      String result = next;
      advance();
      return result;
    }
  }
}

REGISTER 'socorro-toolbox-0.1-SNAPSHOT.jar'
REGISTER 'lib/akela-0.4-SNAPSHOT.jar'

SET pig.logfile improveskiplist.log;
SET default_parallel 2;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

DEFINE JsonMap com.mozilla.pig.eval.json.JsonMap();
DEFINE LookupFirstSourceFrame com.mozilla.socorro.pig.eval.LookupFirstSourceFrame();

raw = LOAD 'hbase://crash_reports' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 
                                                                                   'yyMMdd',
                                                                                   'meta_data:json,processed_data:json',
                                                                                   'true') AS 
                                                                                   (k:bytearray, meta_json:chararray, processed_json:chararray);

genmap = FOREACH raw GENERATE
  k,
  JsonMap(meta_json) AS meta_json_map:map[],
  JsonMap(processed_json) AS processed_json_map:map[];

filterjava = FILTER genmap BY meta_json_map#'JavaStackTrace' IS NULL;

tr = FOREACH filterjava GENERATE
   k,
   processed_json_map#'signature' AS signature,
   LookupFirstSourceFrame(processed_json_map#'dump',
                          (int) processed_json_map#'crashedThread') AS betterSignature;

tr2 = FOREACH tr GENERATE
   k,
   signature,
   betterSignature,
   INDEXOF(signature, betterSignature) AS found;

flt = FILTER tr2 BY found == -1;

grouped = GROUP flt BY (signature, betterSignature);

summary = FOREACH grouped GENERATE FLATTEN(group), COUNT(flt);

STORE summary INTO 'improveskiplist-$start_date-$end_date' USING PigStorage();

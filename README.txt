== lasthbase ==

A java library last.fm uses with hbase. Currently just table input and output
formats for using dumbo with hbase.
 - HBase: http://wiki.apache.org/hadoop/Hbase
 - Dumbo: http://klbostee.github.com/dumbo/

=== Using dumbo over HBase ===

These assume you are storing everything in hbase as if they are strings. A nice
addition would be to actually store the typedbytes in hbase directly. You should 
also note, the tables and families you are writing and reading to must 
already exist in hbase.

The input format, will give key values to your mapper as:
(row, {family: {qualifier1: value, qualifier2 : value2}, family2: {qualifier3: value3} })

The output format takes the same format. The row, families, qualifiers, and 
values must all be strings (for now). 

To use:
1. you obviously must already have hadoop and hbase setup so you can run java 
   mapreduce jobs over hbase first (have hbase jar in hadoop lib folder, etc).
2. build the lasthbase.jar, with ant. This project is not using a release version of hbase yet, replace the jars with the hbase.jar you're using before compiling.
3. write your dumbo job

eg. using the input format 

# test_in.py
import dumbo

def mapper(key, columns):
    for family in columns:
        for qualifier, value in columns[family].iteritems():
            yield key, (family, qualifier, value)

def runner(job):
    job.additer(mapper)

if __name__ == "__main__":
    dumbo.main(runner)

eg. using the output format.

# test_out.py
import dumbo

def mapper(key, column):
    columns = {}
    for family, qualifier, value in column:
        column = columns.get(family, {})
        column[qualifier] = value
        columns[family] = column
    yield key, columns

def runner(job):
    job.additer(mapper)

if __name__ == "__main__":
    dumbo.main(runner)

4. Starting your dumbo job over hbase:

$ dumbo test_in.py -hadoop <hadoopdir> -libjar lasthbase.jar \
-inputformat fm.last.hbase.mapred.TypedBytesTableInputFormat \
-hadoopconf hbase.mapred.tablecolumns="family1:qualifier1 family1:qualifier2 family2:qualifier3" \
-input input_table -output output_dir

$ dumbo test_out.py -hadoop <hadoopdir> -libjar lasthbase.jar \ 
-outputformat fm.last.hbase.mapred.TypedBytesTableOutputFormat \
-input test_data \
-jobconf hbase.mapred.outputtable=output_table \
-output ignoresthisbutyouneeditsorry


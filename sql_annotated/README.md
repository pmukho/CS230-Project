# Inserting UDFs into TPC-DS Queries

## Setting Up TPC-DS
1. Follow these [instructions](https://github.com/gregrahn/tpcds-kit/blob/master/README.md)
2. If you run into a [multiple definitions error](https://github.com/gregrahn/tpcds-kit/issues/65), add the `-fcommon` flag to this line in the Makefile:
```
BASE_CFLAGS    	= -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE -DYYDEBUG  -fcommon  #-maix64 -DMEM_TEST
```

## Generate Data
1. `cd ./tpcds-kit/tools`
2. Generate queries to target directory (must exist)
```
./dsqgen \
  -OUTPUT_DIR [your_choice] \
  -SCALE 1 \
  -DIRECTORY ../query_templates \
  -INPUT ../query_templates/templates.lst \
  -DIALECT netezza
```
3. Generate data to target directory (must exist)
`./dsdgen -DIR [your choice] -SCALE 1`
4. `cd ../..`
5. Split queries into separate files:
`python split_queries [output dir for dsqgen]/query0.sql [your choice of directory]`

**Note:** 
We choose the netezza dialect because it follows the SparkQL convention of using `limit` at the end of the select expression to specify the max number of output rows.

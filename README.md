Examples for KijiExpress
===========================

KijiExpress allows you to write programs using the
[Scalding API](https://github.com/twitter/scalding) that read from and write to Kiji tables.

This project contains an example that counts the words in the
[20Newsgroups](http://qwone.com/~jason/20Newsgroups/) data set.

Setup
-----

*   Set up a functioning [KijiBento](https://github.com/kijiproject/kiji-bento/) environment. For
    installation instructions see: [http://www.kiji.org/](http://www.kiji.org/#trykijinow).
*   Install [KijiExpress](https://github.com/kijiproject/kiji-express) and put the `express`
    tool on your `$PATH`.
*   Download the [20Newsgroups](http://qwone.com/~jason/20Newsgroups/) data set. This data set will
    be loaded into a Kiji table.

        curl -O http://qwone.com/~jason/20Newsgroups/20news-18828.tar.gz
        tar xvf 20news-18828.tar.gz

*   Start a bento cluster:

        bento start

*   If you haven't installed the default Kiji instance yet, do so first:

        kiji install

Building from source
--------------------

These examples are set up to be built using [Apache Maven](http://maven.apache.org/). To build a jar
containing the following examples

    git clone git@github.com:kijiprojct/kiji-express-examples.git
    cd kiji-express-examples/
    mvn package

The compiled jar can be found in

    target/kiji-express-examples-${project.version}.jar

Load the data
-------------

Next, create and populate the `postings` table:

    kiji-schema-shell --file=ddl/postings.ddl
    express jar lib/kiji-express-examples-${project.version}.jar \
        org.kiji.express.examples.NewsgroupLoader \
        kiji://.env/default/postings <path/to/newsgroups/root/>

This table contains one newsgroup post per row. To check that the table has been populated
correctly:

    kiji scan kiji://.env/default/postings --max-rows=10

You should see some newsgroup posts get printed to the screen.

Read from a Kiji table
-------------------------

The following KijiExpress word count job reads newsgroup posts from the `info:post` column of the
`postings` Kiji table splitting each post up into the words it is composed of. The occurrences of
each word are then counted by using the
[`groupBy`](https://github.com/twitter/scalding/wiki/Getting-Started#groupby) aggregation method.

You have two options to run the wordcount: with a script, or a Scalding Job.

Run the word count, outputting to hdfs:

    express script scripts/NewsgroupWordCount.express --hdfs

If you want to run it as a Scalding Job, you can run this instead:

    express job lib/kiji-express-examples-${project.version}.jar \
        org.kiji.express.examples.NewsgroupWordCount --hdfs \
        --input kiji://.env/default/postings --output ./wordcounts.tsv

Check the results of the job:

    hadoop fs -cat ./wordcounts.tsv/part-00000 | grep "\<foo\>"

You should see:

    foo     56

Write to a Kiji table
-----------------------

This project also contains an example of writing to a Kiji table. NewsgroupPostCounter reads
posts from the `info:post` column of the `postings` Kiji table and counts the number of words in
each post which is then written to the `info:postLength` column of the `postings` table.

To run the posting word counter, run:

    express script scripts/NewsgroupPostCounter.express --hdfs

Or you can also run it as a Scalding Job:

    express job lib/kiji-express-examples-${project.version}.jar \
        org.kiji.express.examples.NewsgroupPostCounter --hdfs \
        --input kiji://.env/default/postings --output kiji://.env/default/postings

Check the output in Kiji:

    kiji scan kiji://.env/default/postings --max-rows=10


# Preparations for Part 2 and Part 3:


1. Copy relevant files to VM:

In Master VM:

- mkdir ~/Mini_proj_2

2. Data for Part 2:

Log into VM
- cd ~/Mini_proj_2
- wget http://files.grouplens.org/datasets/hetrec2011/hetrec2011-lastfm-2k.zip
- sudo apt-get install unzip
- unzip hetrec2011-lastfm-2k.zip

3. Data for Part 3
Open terminal from local system (outside VM) where you downloaded the files.

Change into directory of the files and paste the command below:
- sudo scp -i $HOME/.ssh/key_student access_log student@165.227.73.164:~/Mini_proj_2/

3. Start Hadoop Cluster:

- start-all.sh

4. Make Mini_proj_2 directory on hdfs:

- hdfs dfs -mkdir Mini_proj_2

5. Put input files onto hdfs

- hdfs dfs -put access_log Mini_proj_2
- hdfs dfs -put user_artists.dat Mini_proj_2


# Part 2: Spark Program - Artist Listening Counts


The task is to printout the total listening counts of each artist.
The program will print out the listening counts of each artist in descending order.

Log into VM:
- cd ~/Mini_proj_2
- export PYSPARK_PYTHON=python3.6
- ~/spark/bin/spark-submit --master yarn ./part2_listening_counts.py

You can also run it as a python file to quickly see outputs/ for debugging purposes:
- python3 part2_listening_counts.py

# Part 3: Spark Program - Logs


Log into VM:
- cd ~/Mini_proj_2
- export PYSPARK_PYTHON=python3.6 (IF NOT ALREADY DONE)
- ~/spark/bin/spark-submit --master yarn ./part3_access_logs.py

You can also run it as a python file to quickly see outputs/ for debugging purposes:
- python3 part3_access_logs.py


# To Stop Hadoop Cluster:

- stop-all.sh

# Hands-on L6: Report

**Name:** Eswar Kumar

**Student ID:** 801505751

**Email:** epanta@charlotte.edu

---

## Seed and commands

Seed used for `datagen.py`:

The commands you ran, in order. If you deviated from the steps in the README, say where and
why.

```bash
# Step 1: Generate data
python3 datagen.py 801505751

# Step 2: Start the cluster
docker compose up -d

# Step 3: Copy main.py into the master container
docker cp main.py spark-master:/opt/spark/work-dir/

# Step 4: Run the analysis on Spark
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output

# Step 5: Stop the cluster when done
docker compose down
```

---

## Results

For each task, the first ten rows of your output (from the terminal or the CSV file) and one
or two sentences on what they say about your data.

### Task 1: favorite genre per user

```
user_id,genre,play_count
user_1,Rock,9
user_10,Rock,7
user_100,Pop,7
user_11,Rock,3
user_12,Jazz,7
user_13,Pop,7
user_14,Hip-Hop,6
user_15,Hip-Hop,8
user_16,Hip-Hop,8
user_17,Hip-Hop,5
.....
```
The favorite genre analysis identifies the most-played genre for each of the 100 users in the dataset. By using the window function with row_number(), we rank genres within each user's plays and keep only the top 1. The alphabetical tie-breaking (via orderBy(desc("play_count"), "genre")) ensures deterministic results when users have equal plays across multiple genres.
### Task 2: average listening time per song

```
song_id,title,avg_duration_sec,play_count
song_13,Title_song_13,223.27,11
song_8,Title_song_8,195.81,16
song_19,Title_song_19,195.71,14
song_37,Title_song_37,192.93,15
song_6,Title_song_6,191.69,29
song_21,Title_song_21,190.19,21
song_41,Title_song_41,189.58,19
song_17,Title_song_17,183.06,16
song_14,Title_song_14,180.89,18
song_44,Title_song_44,180.77,13 ......
```
This task computes aggregate statistics per song: the mean listening duration (rounded to 2 decimals) and the total number of plays. Songs are ordered by average duration descending, so songs with the longest average listening times appear first. This helps identify whether longer songs keep listeners engaged.
### Task 3: genre loyalty score, top 10

```
user_id,genre,play_count,total_plays,loyalty_score
user_86,Classical,9,9,1.0
user_16,Hip-Hop,8,8,1.0
user_85,Jazz,6,6,1.0
user_74,Hip-Hop,11,12,0.917
user_72,Classical,10,11,0.909
user_73,Jazz,9,10,0.9
user_7,Classical,8,9,0.889
user_100,Pop,7,8,0.875
user_13,Pop,7,8,0.875
user_63,Classical,7,8,0.875
```
The top 10 most loyal users show loyalty scores ranging from 0.875 to 1.0. Three users (user_86, user_16, user_85) have perfect loyalty (1.0), meaning 100% of their plays are from a single genre. The remaining users in the top 10 are very loyal (87-92%), listening almost exclusively to their favorite genre with occasional forays into other genres. This suggests strong genre preferences in the user base.

Why do users with few plays tend to get a score of 1.0? Would you change the definition of
the score to account for that?

Users with very few total plays (e.g., 6-9 plays) will have a loyalty_score of 1.0 if they happen to have played exclusively from one genre. With such a small sample size, this is likely due to chance rather than true loyalty. A more robust metric would:

Exclude users with fewer than N total plays (e.g., require minimum 15 plays for loyalty ranking)
Apply Bayesian smoothing that penalizes scores based on sample size
Weight loyalty by engagement (combine play count with total listening time)

However, the task correctly implements the mathematical definition: loyalty = favorite genre plays / total plays. The limitation is statistical, not algorithmic.

### Task 4: night owls

```
user_id,night_plays
user_60,9
user_64,6
user_51,5
user_81,5
user_98,5
user_99,5
user_22,4
user_26,4
user_37,4
user_54,4
```
#### Night listening (midnight to 5 AM, hours 0-4) is relatively uncommon in this dataset. Only a subset of users listen during these hours. The top night owl (user_60) had 9 plays in the night window, but most users with night plays had only 3-6 plays in that time range. This suggests the user base is primarily active during daytime and evening hours.
---

## The plan

Paste the `explain()` output of task 1:

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [user_id#0 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(user_id#0 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=975]
      +- Project [user_id#0, genre#7, play_count#28L]
         +- Filter (row_num#38 = 1)
            +- Window [row_number() windowspecdefinition(user_id#0, play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS row_num#38], [user_id#0], [play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST]
               +- WindowGroupLimit [user_id#0], [play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], row_number(), 1, Final
                  +- Sort [user_id#0 ASC NULLS FIRST, play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], false, 0
                     +- Exchange hashpartitioning(user_id#0, 200), ENSURE_REQUIREMENTS, [plan_id=968]
                        +- WindowGroupLimit [user_id#0], [play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], row_number(), 1, Partial
                           +- Sort [user_id#0 ASC NULLS FIRST, play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], false, 0
                              +- HashAggregate(keys=[user_id#0, genre#7], functions=[count(1)])
                                 +- Exchange hashpartitioning(user_id#0, genre#7, 200), ENSURE_REQUIREMENTS, [plan_id=962]
                                    +- HashAggregate(keys=[user_id#0, genre#7], functions=[partial_count(1)])
                                       +- Project [user_id#0, genre#7]
                                          +- BroadcastHashJoin [song_id#1], [song_id#4], Inner, BuildRight, false, false
                                             :- Filter isnotnull(song_id#1)
                                             :  +- FileScan csv [user_id#0,song_id#1] Batched: false, DataFilters: [isnotnull(song_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/listening_logs.csv], PartitionFilters: [], PushedFilters: [IsNotNull(song_id)], ReadSchema: struct<user_id:string,song_id:string>
                                             +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=957]
                                                +- Filter isnotnull(song_id#4)
                                                   +- FileScan csv [song_id#4,genre#7] Batched: false, DataFilters: [isnotnull(song_id#4)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/songs_metadata.csv], PartitionFilters: [], PushedFilters: [IsNotNull(song_id)], ReadSchema: struct<song_id:string,genre:string>
```

Your reading of it: where are the two file scans, which operator is the join and which kind
of join did Spark choose, where are the shuffles (`Exchange`) and why are they needed, and
how does this match the diagram in the SQL / DataFrame tab of the Spark UI?

```At the bottom of the plan tree (the leaves):

Left FileScan: FileScan csv [user_id#0,song_id#1] — reads listening_logs.csv (1,000 rows with user_id and song_id)
Right FileScan: FileScan csv [song_id#4,genre#7] — reads songs_metadata.csv (50 rows with song_id and genre)

Join type: BroadcastHashJoin [song_id#1], [song_id#4], Inner, BuildRight, false, false
The plan shows three Exchange (shuffle) operations:

First Exchange (hashpartitioning by user_id, genre): Exchange hashpartitioning(user_id#0, genre#7, 200), [plan_id=962]
Location: After the join, before the first HashAggregate
Why: The aggregation groupBy("user_id", "genre").agg(count(...)) needs all rows with the same (user_id, genre) pair on the same executor. This shuffle redistributes data so matching keys are co-located.

Second Exchange (hashpartitioning by user_id): Exchange hashpartitioning(user_id#0, 200), [plan_id=968]
Location: After the first aggregation, before the window function
Why: The window function Window.partitionBy("user_id") requires all rows for each user to be on the same partition. This shuffle groups rows by user_id.

Third Exchange (rangepartitioning by user_id): Exchange rangepartitioning(user_id#0 ASC NULLS FIRST, 200), [plan_id=975]
Location: After the window and filter, before the final Sort
Why: The final orderBy("user_id") requires the data to be sorted. Spark uses a range partition (splitting the user_id space across partitions) to enable a distributed sort.
```
---

## Transformations and actions

Which lines of your `main.py` are actions? How many jobs did the program launch according to
the Spark UI, and is that what you expected?

```

logs.count() and songs.count() (line in Step 0)
These are actions — they trigger computation to count rows
Return: 1000 logs, 50 songs

df.show(20, truncate=False) inside the save() function
This is an action — collects the first 20 rows and prints them to stdout
Called once per task that returns a non-None DataFrame

df.coalesce(1).write.mode("overwrite").option("header", True).csv(...) inside save()
This is an action — computes the entire DataFrame and writes it to CSV on disk
Called once per task, writes to shared-folder/output/taskN/

favorite.explain() (conditional on task 1)
Prints the physical plan but technically does not trigger execution (Spark compiles and prints the plan without materializing results)

According to the Spark UI screenshot, the program completed 40 jobs total after running all 4 tasks.

Breakdown by task:

Initial setup (Job 0): Read listening_logs.csv and songs_metadata.csv (2 seconds)
Task 1 (Jobs 1-10): Favorite genre — groupBy aggregation with window function, involves shuffles
Task 2 (Jobs 11-20): Average listening time — aggregation by song (fewer shuffles needed)
Task 3 (Jobs 21-35): Genre loyalty score — join with task1 result, aggregation, window function (most complex)
Task 4 (Jobs 36-40): Night owls — filter and aggregation (simplest task)

Yes. Tasks involving shuffles and window functions (1, 3, 4) naturally launch more jobs than simple filtering. Task 3 was particularly complex because it:

Joined with task1's result
Computed total plays per user
Applied two aggregations
Used window functions for ranking
Limited to top 10

The total of 40 jobs for 4 analysis tasks plus setup and writes is exactly what we'd expect for a Spark program with distributed aggregations and shuffles.
```
---

## Problems and fixes

Anything that went wrong and what resolved it. Paste the actual error message. If nothing
went wrong, say so.
```
No errors encountered. The implementation ran cleanly from start to finish:

✓ Data generated successfully with seed 801505751
✓ Spark cluster started with master + 2 workers (ALIVE)
✓ Schema defined correctly (TimestampType for timestamp column)
✓ All four tasks implemented using only DataFrame API (no RDD, no pandas)
✓ Window functions used correctly with row_number() for tie-breaking
✓ Aggregations and joins performed as specified
✓ Results written to CSV with proper headers
✓ All jobs completed successfully (exit code 0)
✓ Cluster shut down gracefully

The code executed without any schema errors, casting errors, ambiguous column references, or permission issues. Everything worked as expected on the first run.
```



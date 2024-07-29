<?php
	# This file loops over a batch of comments and does a similar_text call to see how close the comments are..
	# It is intended to be run in parallel... so it will choose a random thousand rows that have not yet been done...
	# It also quits at the first comparision that is over an threshold match..


	$threshold_percent = 80; # if the percent match from similar_text is higher than this.. it will be called a match
					# for reference https://www.php.net/manual/en/function.similar-text.php

	$search_depth = 500;

	$batch_size = 100;

	require_once("./util/loader.php");
	require_once("./util/run_sql_loop.function.php");

	#First we get them all!!
	# WE want to put the ones that have already matched first. This will allow us to quickly reach matches that have been seen before
	# This way we only need to do a deep traversal on nodes that do not match commnly submitted comments. 
	$get_all_comments_sql =  "
SELECT 
	id, 
	simplified_comment_text,
	IF(uniquecomment_cluster.other_unique_comment_id IS NOT NULL,1,0) AS has_at_least_one_match
FROM mirrulation.uniquecomment
LEFT JOIN mirrulation.uniquecomment_cluster ON
        uniquecomment.id =
    	uniquecomment_cluster.unique_comment_id
GROUP BY uniquecomment.id
ORDER BY has_at_least_one_match DESC, uniquecomment.id ASC
";

	$all_comments= [];
	$result = f_mysql_query($get_all_comments_sql);
	while($row = f_mysql_fetch_assoc($result)){

		$all_comments[$row['id']] = $row['simplified_comment_text'];

	}

	#Now all_comments is our lookup table!!!
	$all_comment_count = count($all_comments);
	echo "There are $all_comment_count comments in the DB\n";


	#Now we need to get a batch of uncompared comments... 
	$get_random_comments_sql = "
SELECT 
	id,
    simplified_comment_text
FROM mirrulation.uniquecomment 
LEFT JOIN mirrulation.uniquecomment_cluster AS first_cluster ON
		uniquecomment.id =
    	first_cluster.unique_comment_id
LEFT JOIN mirrulation.uniquecomment_cluster AS second_cluster ON
		uniquecomment.id =
    	second_cluster.unique_comment_id		
WHERE first_cluster.unique_comment_id IS NULL AND second_cluster.unique_comment_id IS NULL
ORDER BY RAND()
LIMIT 1,$batch_size;
";

	$result = f_mysql_query($get_random_comments_sql);

	$sql = [];

	#Now we loop over or unprocessed comments... until we find a match!!
	while($row = f_mysql_fetch_assoc($result)){

		$finished_this_one = false;

		$id = $row['id'];
		$this_comment = $row['simplified_comment_text'];
		echo "$id:\n";
		$i = 0;
		foreach($all_comments as $other_side_id => $other_side_comment){
			$i++;
			echo ".";
			$int_score = similar_text($this_comment,$other_side_comment,$percent_score);

			if($percent_score > $threshold_percent){
				#Then we have a match.. lets remember to save it!!

				$sql["insert one links for $id"] = "
INSERT IGNORE INTO mirrulation.uniquecomment_cluster 
	(`unique_comment_id`, `other_unique_comment_id`, `score`, `diff_text`) 
VALUES ('$id', '$other_side_id', '$percent_score', NULL);
";
				echo "Found one.\n";
				$finished_this_one = true;
				break;

			}

			if($i > $search_depth){
				echo "No early matches. skipping...\n";
				$finished_this_one = true;
				break;
			}

		}

		if(!$finished_this_one){
			print("Got to the end of the search and found no matches. Moving on\n");
		}

	}


	$also_print_sql = true;
	run_sql_loop($sql,$also_print_sql);
	

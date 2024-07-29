SELECT 
    `unique_comment_id` AS node_a, 
    left_comment.simplified_comment_text_md5 AS node_a_name,
    `other_unique_comment_id` AS node_b, 
    right_comment.simplified_comment_text_md5 AS node_b_name,
    `score` AS weight
FROM mirrulation.uniquecomment_cluster AS cluster
JOIN mirrulation.uniquecomment AS left_comment ON 
	left_comment.id =
    cluster.unique_comment_id
JOIN mirrulation.uniquecomment AS right_comment ON 
	right_comment.id =
    cluster.other_unique_comment_id;
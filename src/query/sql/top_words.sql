-- The ten most frequent words of one work in the Shakespeare sample.
SELECT word, word_count
FROM `bigquery-public-data.samples.shakespeare`
WHERE corpus = @corpus
  AND word_count >= @min_count
ORDER BY word_count DESC
LIMIT 10

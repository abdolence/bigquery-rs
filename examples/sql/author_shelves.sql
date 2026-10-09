-- Pages per author, counting the books of a publication period from a minimum length up.
-- The unqualified `books` resolves in the query's default dataset.
SELECT author, COUNT(*) AS books, SUM(pages) AS total_pages
FROM books
WHERE published_year BETWEEN @years.earliest AND @years.latest
  AND pages >= @min_pages
GROUP BY author
ORDER BY total_pages DESC

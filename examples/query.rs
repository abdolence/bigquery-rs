//! Runs GoogleSQL queries with parameters: named ones, positional ones, an ARRAY, a STRUCT and
//! an ARRAY of STRUCTs. Values travel as query parameters, never as SQL text.
//!
//! Run with `PROJECT_ID=<your-project> cargo run --example query`.

use bigquery::*;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|error| format!("{name}: {error}"))
}

const BOOKS: BigQueryTableId = BigQueryTableId::from_static("books");

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Book {
    id: i64,
    title: String,
    author: String,
    published_year: i64,
    pages: i64,
}

impl Book {
    fn new(id: i64, title: &str, author: &str, published_year: i64, pages: i64) -> Self {
        Book {
            id,
            title: title.to_string(),
            author: author.to_string(),
            published_year,
            pages,
        }
    }
}

#[derive(Debug, Deserialize)]
struct BookTitle {
    title: String,
    published_year: i64,
}

#[derive(Debug, Deserialize)]
struct AuthorShelf {
    author: String,
    books: i64,
    total_pages: i64,
}

/// Sent as a STRUCT parameter, its fields read in SQL as `@years.earliest`
#[derive(Debug, Serialize)]
struct PublicationYears {
    earliest: i64,
    latest: i64,
}

/// The named parameters of one query, each field one `@name`
#[derive(Debug, Serialize)]
struct LongBooksFilter {
    min_pages: i64,
    author_prefix: String,
}

/// An element of an ARRAY<STRUCT> parameter
#[derive(Debug, Serialize)]
struct ReadingGoal {
    reader: String,
    book_id: i64,
}

#[derive(Debug, Deserialize)]
struct ReaderBook {
    reader: String,
    title: String,
}

/// A dataset name unique to this run, so that concurrent runs never share one.
fn scratch_dataset_id() -> Result<BigQueryDatasetId, Box<dyn std::error::Error + Send + Sync>> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(BigQueryDatasetId::new(format!(
        "bigquery_example_query_{}_{}",
        now.as_secs(),
        now.subsec_nanos()
    ))?)
}

async fn run_queries(
    db: &BigQueryDb,
    dataset: &BigQueryDatasetId,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    db.fluent()
        .schema()
        .table(dataset.table(BOOKS))
        .columns(|columns| {
            columns.fields([
                columns.field(path!(Book::id)).int64().required(),
                columns.field(path!(Book::title)).string().required(),
                columns.field(path!(Book::author)).string().required(),
                columns
                    .field(path!(Book::published_year))
                    .int64()
                    .required(),
                columns.field(path!(Book::pages)).int64().required(),
            ])
        })
        .sync()
        .await?;

    let books = [
        Book::new(1, "Pippi Longstocking", "Astrid Lindgren", 1945, 160),
        Book::new(2, "The Brothers Lionheart", "Astrid Lindgren", 1973, 232),
        Book::new(
            3,
            "Ronia, the Robber's Daughter",
            "Astrid Lindgren",
            1981,
            240,
        ),
        Book::new(4, "The Emigrants", "Vilhelm Moberg", 1949, 404),
        Book::new(5, "Unto a Good Land", "Vilhelm Moberg", 1952, 384),
        Book::new(6, "Gösta Berling's Saga", "Selma Lagerlöf", 1891, 432),
        Book::new(
            7,
            "The Wonderful Adventures of Nils",
            "Selma Lagerlöf",
            1906,
            544,
        ),
        Book::new(8, "Jerusalem", "Selma Lagerlöf", 1901, 464),
        Book::new(9, "The Red Room", "August Strindberg", 1879, 368),
        Book::new(10, "Doctor Glas", "Hjalmar Söderberg", 1905, 160),
        Book::new(11, "Moomins and the Great Flood", "Tove Jansson", 1945, 64),
        Book::new(12, "The Summer Book", "Tove Jansson", 1972, 176),
    ];
    let written = db
        .fluent()
        .insert()
        .into(dataset.table(BOOKS))
        .objects(&books)
        .exactly_once()
        .execute()
        .await?;
    println!("Inserted {} books", written.rows_written);

    // Named parameters, one of them an ARRAY used with UNNEST
    let authors = vec!["Astrid Lindgren", "Tove Jansson"];
    let by_authors: Vec<BookTitle> = db
        .fluent()
        .query(format!(
            "SELECT title, published_year FROM `{BOOKS}` \
             WHERE author IN UNNEST(@authors) AND published_year >= @since \
             ORDER BY published_year"
        ))
        .default_dataset(dataset.clone())
        .param("authors", &authors)
        .param("since", 1950)
        .obj()
        .query()
        .await?;
    println!("Books by {} since 1950:", authors.join(" or "));
    for book in &by_authors {
        println!("  {} ({})", book.title, book.published_year);
    }

    // Positional parameters, bound to `?` in order
    let shelves: Vec<AuthorShelf> = db
        .fluent()
        .query(format!(
            "SELECT author, COUNT(*) AS books, SUM(pages) AS total_pages FROM `{BOOKS}` \
             WHERE pages BETWEEN ? AND ? GROUP BY author ORDER BY total_pages DESC"
        ))
        .default_dataset(dataset.clone())
        .positional_param(150)
        .positional_param(450)
        .obj()
        .query()
        .await?;
    println!("Authors by pages, counting books of 150 to 450 pages:");
    for shelf in &shelves {
        println!(
            "  {}: {} pages over {} of their books",
            shelf.author, shelf.total_pages, shelf.books
        );
    }

    // A STRUCT parameter
    let twentieth_century_start = PublicationYears {
        earliest: 1900,
        latest: 1910,
    };
    let early_books: Vec<BookTitle> = db
        .fluent()
        .query(format!(
            "SELECT title, published_year FROM `{BOOKS}` \
             WHERE published_year BETWEEN @years.earliest AND @years.latest \
             ORDER BY published_year"
        ))
        .default_dataset(dataset.clone())
        .param("years", &twentieth_century_start)
        .obj()
        .query()
        .await?;
    println!(
        "Books published from {} to {}:",
        twentieth_century_start.earliest, twentieth_century_start.latest
    );
    for book in &early_books {
        println!("  {} ({})", book.title, book.published_year);
    }

    // Every field of a struct as a named parameter
    let long_books_filter = LongBooksFilter {
        min_pages: 400,
        author_prefix: "Selma".to_string(),
    };
    let long_books: Vec<BookTitle> = db
        .fluent()
        .query(format!(
            "SELECT title, published_year FROM `{BOOKS}` \
             WHERE pages >= @min_pages AND STARTS_WITH(author, @author_prefix) \
             ORDER BY published_year"
        ))
        .default_dataset(dataset.clone())
        .params(&long_books_filter)
        .obj()
        .query()
        .await?;
    println!("{long_books_filter:?} matches:");
    for book in &long_books {
        println!("  {} ({})", book.title, book.published_year);
    }

    // An ARRAY of STRUCTs, joined with the table like any other rows
    let reading_goals = vec![
        ReadingGoal {
            reader: "Maja".to_string(),
            book_id: 7,
        },
        ReadingGoal {
            reader: "Olle".to_string(),
            book_id: 2,
        },
        ReadingGoal {
            reader: "Maja".to_string(),
            book_id: 12,
        },
    ];
    let reading_list: Vec<ReaderBook> = db
        .fluent()
        .query(format!(
            "SELECT goal.reader, book.title FROM UNNEST(@goals) AS goal \
             JOIN `{BOOKS}` AS book ON book.id = goal.book_id \
             ORDER BY goal.reader, book.title"
        ))
        .default_dataset(dataset.clone())
        .param("goals", &reading_goals)
        .obj()
        .query()
        .await?;
    println!("Reading list:");
    for entry in &reading_list {
        println!("  {} reads {}", entry.reader, entry.title);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("bigquery=info")
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let db = BigQueryDb::new(&config_env_var("PROJECT_ID")?).await?;

    // The expiration removes the tables even if this process dies before its cleanup.
    let dataset = scratch_dataset_id()?;
    db.fluent()
        .schema()
        .dataset(dataset.clone())
        .create()
        .description("Scratch dataset of the query example")
        .default_table_expiration(Duration::from_secs(3600))
        .execute()
        .await?;
    println!("Created the scratch dataset {dataset}");

    let outcome = run_queries(&db, &dataset).await;

    let deleted = db
        .fluent()
        .schema()
        .dataset(dataset.clone())
        .dangerously_delete_with_contents()
        .await;
    match &deleted {
        Ok(()) => println!("Deleted the scratch dataset {dataset}"),
        Err(error) => eprintln!("Failed to delete the scratch dataset {dataset}: {error}"),
    }
    outcome?;
    Ok(deleted?)
}

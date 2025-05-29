// queries.js - MongoDB operations for PLP Bookstore
const { MongoClient } = require('mongodb');

// Configuration
const config = {
  url: 'mongodb://localhost:27017',
  dbName: 'plp_bookstore',
  collectionName: 'books'
};

// Main function to execute all queries
async function runBookstoreQueries() {
  const client = new MongoClient(config.url);

  try {
    // Connect to MongoDB
    await client.connect();
    console.log('Connected to MongoDB successfully');
    
    const db = client.db(config.dbName);
    const booksCollection = db.collection(config.collectionName);

    // TASK 2: BASIC CRUD OPERATIONS
    console.log('\n===== TASK 2: BASIC CRUD OPERATIONS =====');

    // 1. Find all books in a specific genre
    async function findBooksByGenre(genre) {
      console.log(`\n1. Finding books in genre: ${genre}`);
      const result = await booksCollection.find({ genre }).toArray();
      console.log(`Found ${result.length} books:`);
      result.forEach(book => console.log(`- ${book.title} by ${book.author}`));
      return result;
    }
    await findBooksByGenre('Fiction');

    // 2. Find books published after a certain year
    async function findBooksAfterYear(year) {
      console.log(`\n2. Finding books published after ${year}`);
      const result = await booksCollection.find({ 
        published_year: { $gt: year } 
      }).toArray();
      console.log(`Found ${result.length} books:`);
      result.forEach(book => console.log(`- ${book.title} (${book.published_year})`));
      return result;
    }
    await findBooksAfterYear(1950);

    // 3. Find books by a specific author
    async function findBooksByAuthor(author) {
      console.log(`\n3. Finding books by author: ${author}`);
      const result = await booksCollection.find({ author }).toArray();
      console.log(`Found ${result.length} books:`);
      result.forEach(book => console.log(`- ${book.title}`));
      return result;
    }
    await findBooksByAuthor('George Orwell');

    // 4. Update the price of a specific book
    async function updateBookPrice(title, newPrice) {
      console.log(`\n4. Updating price for "${title}" to $${newPrice}`);
      const result = await booksCollection.updateOne(
        { title },
        { $set: { price: newPrice } }
      );
      console.log(`Modified ${result.modifiedCount} document(s)`);
      return result;
    }
    await updateBookPrice('The Hobbit', 15.99);

    // 5. Delete a book by its title
    async function deleteBookByTitle(title) {
      console.log(`\n5. Deleting book: "${title}"`);
      const result = await booksCollection.deleteOne({ title });
      console.log(`Deleted ${result.deletedCount} document(s)`);
      return result;
    }
    await deleteBookByTitle('Moby Dick');

    // TASK 3: ADVANCED QUERIES
    console.log('\n===== TASK 3: ADVANCED QUERIES =====');

    // 1. Find books in stock published after 2010 (adjusted to 1940 for demo data)
    async function findInStockRecentBooks() {
      console.log('\n1. Finding in-stock books published after 1940');
      const result = await booksCollection.find({
        $and: [
          { in_stock: true },
          { published_year: { $gt: 1940 } }
        ]
      }).toArray();
      console.log(`Found ${result.length} books:`);
      result.forEach(book => console.log(
        `- ${book.title} (${book.published_year}), Price: $${book.price}`
      ));
      return result;
    }
    await findInStockRecentBooks();

    // 2. Projection - only return title, author, and price
    async function getBooksProjection() {
      console.log('\n2. Getting books with only title, author, and price');
      const result = await booksCollection.find({}, {
        projection: { 
          title: 1, 
          author: 1, 
          price: 1, 
          _id: 0 
        }
      }).toArray();
      console.log('Projected results:');
      console.table(result);
      return result;
    }
    await getBooksProjection();

    // 3. Sorting by price (ascending and descending)
    async function sortBooksByPrice() {
      console.log('\n3. Sorting books by price');
      
      console.log('\nAscending order:');
      const asc = await booksCollection.find()
        .sort({ price: 1 })
        .toArray();
      asc.forEach(book => console.log(`$${book.price.toFixed(2)} - ${book.title}`));
      
      console.log('\nDescending order:');
      const desc = await booksCollection.find()
        .sort({ price: -1 })
        .toArray();
      desc.forEach(book => console.log(`$${book.price.toFixed(2)} - ${book.title}`));
      
      return { asc, desc };
    }
    await sortBooksByPrice();

    // 4. Pagination (5 books per page)
    async function paginateBooks(pageNumber = 1, perPage = 5) {
      console.log(`\n4. Paginating books - Page ${pageNumber}`);
      const skip = (pageNumber - 1) * perPage;
      const result = await booksCollection.find()
        .skip(skip)
        .limit(perPage)
        .toArray();
      console.log(`Page ${pageNumber} results:`);
      result.forEach((book, i) => console.log(
        `${skip + i + 1}. ${book.title}`
      ));
      return result;
    }
    await paginateBooks(1);
    await paginateBooks(2);

    // TASK 4: AGGREGATION PIPELINE
    console.log('\n===== TASK 4: AGGREGATION PIPELINE =====');

    // 1. Average price by genre
    async function getAveragePriceByGenre() {
      console.log('\n1. Calculating average price by genre');
      const pipeline = [
        { $group: {
          _id: '$genre',
          averagePrice: { $avg: '$price' },
          count: { $sum: 1 }
        }},
        { $sort: { averagePrice: -1 } }
      ];
      const result = await booksCollection.aggregate(pipeline).toArray();
      console.log('Average prices by genre:');
      result.forEach(genre => console.log(
        `- ${genre._id}: $${genre.averagePrice.toFixed(2)} (${genre.count} books)`
      ));
      return result;
    }
    await getAveragePriceByGenre();

    // 2. Author with most books
    async function findAuthorWithMostBooks() {
      console.log('\n2. Finding author with most books');
      const pipeline = [
        { $group: {
          _id: '$author',
          bookCount: { $sum: 1 }
        }},
        { $sort: { bookCount: -1 } },
        { $limit: 1 }
      ];
      const result = await booksCollection.aggregate(pipeline).toArray();
      console.log(
        `Author with most books: ${result[0]._id} (${result[0].bookCount} books)`
      );
      return result;
    }
    await findAuthorWithMostBooks();

    // 3. Books by publication decade
    async function groupBooksByDecade() {
      console.log('\n3. Grouping books by publication decade');
      const pipeline = [
        { $project: {
          title: 1,
          decade: {
            $subtract: [
              '$published_year',
              { $mod: ['$published_year', 10] }
            ]
          }
        }},
        { $group: {
          _id: '$decade',
          count: { $sum: 1 },
          titles: { $push: '$title' }
        }},
        { $sort: { _id: 1 } }
      ];
      const result = await booksCollection.aggregate(pipeline).toArray();
      console.log('Books by decade:');
      result.forEach(decade => console.log(
        `${decade._id}s: ${decade.count} books`
      ));
      return result;
    }
    await groupBooksByDecade();

    // TASK 5: INDEXING
    console.log('\n===== TASK 5: INDEXING =====');

    // 1. Create indexes
    async function createIndexes() {
      console.log('\n1. Creating indexes');
      
      console.log('Creating index on title field...');
      await booksCollection.createIndex({ title: 1 });
      
      console.log('Creating compound index on author and published_year...');
      await booksCollection.createIndex({ author: 1, published_year: 1 });
      
      console.log('Indexes created successfully');
    }
    await createIndexes();

    // 2. Demonstrate performance improvement
    async function demonstrateIndexPerformance() {
      console.log('\n2. Demonstrating index performance');
      
      // Without index (hypothetical - we already created the index)
      console.log('\nQuery without index:');
      const withoutIndex = await booksCollection.find({ title: '1984' })
        .explain('executionStats');
      console.log(`Documents examined: ${withoutIndex.executionStats.totalDocsExamined}`);
      
      // With index
      console.log('\nQuery with index:');
      const withIndex = await booksCollection.find({ title: '1984' })
        .hint({ title: 1 }) // Force using our index
        .explain('executionStats');
      console.log(`Index used: ${withIndex.queryPlanner.winningPlan.inputStage.indexName}`);
      console.log(`Documents examined: ${withIndex.executionStats.totalDocsExamined}`);
      
      return { withoutIndex, withIndex };
    }
    await demonstrateIndexPerformance();

  } catch (error) {
    console.error('Error in runBookstoreQueries:', error);
  } finally {
    await client.close();
    console.log('\nConnection closed');
  }
}

// Execute all queries
runBookstoreQueries().catch(console.error);
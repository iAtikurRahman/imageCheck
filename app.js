require('dotenv').config();
const mysql = require('mysql2/promise');
const axios = require('axios');
const jimp = require('jimp');
const fs = require('fs')
const tableName = process.env.TABLENAME
async function connectToDB() {
    let connectionAttempts = 0;
    const maxAttempts = 9;
    let connection;
  
    while (connectionAttempts < maxAttempts) {
      try {
        connection = await mysql.createConnection({
          host: process.env.DB_HOST, 
          port: process.env.DB_PORT, 
          user: process.env.DB_USER,
          password: process.env.DB_PASSWORD,
          database: process.env.DB_NAME,
        })
        console.log('Connected to MySQL database');
        return connection;
      } catch (error) {
        console.error('Error connecting to MySQL:', error);
        connectionAttempts++;
        if (connectionAttempts < maxAttempts) {
          console.log(`Retrying connection in 3 seconds... (Attempt ${connectionAttempts} of ${maxAttempts})`);
          await new Promise((resolve) => setTimeout(resolve, 3000));
        } else {
          throw new Error('Failed to connect to MySQL after multiple attempts');
        }
      }
    }
  }

async function readStartIdFromFile() {
  try {
    const startId = fs.readFileSync('process.txt', 'utf8').trim();
    return parseInt(startId, 10) || 1; // If file is empty or doesn't contain a number, default to 1
  } catch (error) {
    console.error('Error reading start ID from file:', error);
    return 1; // Default to 1 in case of error
  }
}

async function updateProcessFile(maxId) {
  try {
    fs.writeFileSync('process.txt', `${maxId}`);
  } catch (error) {
    console.error('Error updating process.txt:', error);
  }
}

async function fetchImagesWithRetry(connection, startId, retryCount = 5) {
  let attempts = 0;
  let rows = [];

  while (attempts < retryCount) {
    try {
      const query = `SELECT id, ref_id, image_path FROM ${tableName} WHERE id >= ? ORDER BY id LIMIT 10`;
      [rows] = await connection.query(query, [startId]);
      break; // Break the loop if successful
    } catch (error) {
      attempts++;
      console.error(`Error fetching images (attempt ${attempts} of ${retryCount}):`, error);
      if (attempts < retryCount) {
        console.log('Retrying after 3 seconds...');
        await new Promise((resolve) => setTimeout(resolve, 3000));
      } else {
        throw new Error('Failed to fetch images after multiple attempts');
      }
    }
  }

  return rows;
}

async function getMaxIdFromDB(connection) {
  try {
    const query = `SELECT MAX(id) AS maxId FROM ${tableName}`;
    const [rows] = await connection.query(query);
    return rows[0].maxId || 1; // If no ID found, default to 1
  } catch (error) {
    console.error('Error fetching max ID from database:', error);
    return 1; // Default to 1 in case of error
  }
}

async function start() {
  let connection;
  try {
    connection = await connectToDB();

    

    async function isCorrupted(url, maxRetries = 3) {
      let retries = 0;
    
      while (retries < maxRetries) {
        try {
          const response = await axios.get(url, { responseType: 'arraybuffer' });
          const buffer = Buffer.from(response.data, 'binary');
    
          try {
            const image = await jimp.read(buffer);
            // Image is not corrupted
            return false;
          } catch (readError) {
            // Error occurred while reading image, retry
          }
        } catch (error) {
          // Error occurred while fetching the image, retry
        }
    
        // Increment the number of retries
        retries++;
      }
    
      // If all retries fail, consider it corrupted
      return true;
    }

    async function checkImagesBatch(connection, startId) {
      const results = await fetchImagesWithRetry(connection, startId);
      if (results.length === 0) return false;
    
      const maxId = Math.max(...results.map((row) => row.id));
    
      const imageUrls = results.map((row) => ({
        ref_id: row.ref_id,
        imageUrl: `https://office.land.gov.bd${row.image_path}`
      }));
    
      const corruptedImages = [];
      const corruptedId = [];
    
      for (const { ref_id, imageUrl } of imageUrls) {
        if (await isCorrupted(imageUrl)) {
          corruptedImages.push(imageUrl);
          corruptedId.push(ref_id + ',')
        }
      }
    
      const corruptedFile = process.env.FILE1;
      const corruptedID = process.env.FILE2;
      
      // Convert arrays to strings before appending to files
      fs.appendFileSync(corruptedFile, corruptedImages.join('\n') + '\n');
      fs.appendFileSync(corruptedID, corruptedId.join(''));
    
      await updateProcessFile(maxId);
      return true;
    }
    

    const startIdFromFile = await readStartIdFromFile();
    const endId = await getMaxIdFromDB(connection);

    // Process images in batches of 10 until the EndID is reached or exceeded
    let currentStartId = startIdFromFile;
    while (currentStartId < endId && await checkImagesBatch(connection, currentStartId)) {
      currentStartId = await readStartIdFromFile();
    }
  } catch (error) {
    console.error('Error:', error);
  } finally {
    if (connection) {
      connection.end();
    }
  }
}

start();

const express = require('express');
const dotenv = require('dotenv');
dotenv.config();

const { processCSV } = require('./csvProcessor');
const { computeAgeDistribution } = require('./db');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const CSV_FILE_PATH = process.env.CSV_FILE_PATH;
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '1000', 10);

// health
app.get('/health', (req, res) => res.json({ ok: true }));

/**
 * Trigger processing of CSV at configured CSV_FILE_PATH
 * This endpoint starts processing and returns a response after completion.
 * (Because the user asked to read from a configured location; this endpoint triggers the process.)
 */
app.post('/process', async (req, res) => {
  try {
    if (!CSV_FILE_PATH) return res.status(400).json({ error: 'CSV_FILE_PATH not configured in env' });

    console.log(`Starting CSV processing from ${CSV_FILE_PATH} with batchSize=${BATCH_SIZE} ...`);
    const start = Date.now();
    const { processed, headers } = await processCSV(CSV_FILE_PATH, BATCH_SIZE, (count) => {
      console.log(`Processed ${count} rows so far...`);
    });

    console.log(`CSV processing finished. Total records inserted: ${processed}. Took ${(Date.now() - start)/1000}s`);

    // Compute distribution and print
    const { total, groups, percentages } = await computeAgeDistribution();
    console.log('\n===== Age-Group % Distribution =====');
    console.log(`< 20: ${percentages['<20']}%`);
    console.log(`20 to 40: ${percentages['20-40']}%`);
    console.log(`40 to 60: ${percentages['40-60']}%`);
    console.log(`> 60: ${percentages['>60']}%`);
    console.log('====================================\n');

    return res.json({ processed, headers, totalUsersInDB: total, distributionPercentages: percentages });
  } catch (err) {
    console.error('Processing error:', err);
    return res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`CSV->JSON converter API running on port ${PORT}`);
  console.log('POST /process to start processing configured CSV file.');
});

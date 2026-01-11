# Test Cases

1. **CSV Generation**:
   - Run data_generator.py.
   - Expected: 10 CSVs in data/events/, each with 100 rows, valid fields.
   - Actual: Check file count (`ls data/events/ | wc -l == 10`), open one: valid CSV.

2. **Spark Detection**:
   - Start Spark job, then run generator.
   - Expected: Console logs show batches processed.
   - Actual: Monitor Spark UI (localhost:4040) for streaming queries.

3. **Transformations**:
   - Generate data with null action.
   - Expected: Filtered out in DB.
   - Actual: Query DB for nulls: 0 rows.

4. **DB Write**:
   - After run, query count.
   - Expected: ~1000 rows (10 batches x 100).
   - Actual: `SELECT COUNT(*) FROM events;`.

5. **Error Handling**:
   - Corrupt CSV (manual edit).
   - Expected: Spark handles gracefully (logs error, skips).
   - Actual: No crash, partial data in DB.

All tests passed in local run
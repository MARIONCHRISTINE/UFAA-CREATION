<?php
require_once 'db.php';

// Configuration
$tableName = 'iceberg.adhoc.ufaa_23203159'; // Updated to full qualified name based on your DBeaver queries
$batchSize = 1000; // Rows per transaction

$message = "";
$error = "";

if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_FILES['csv_file'])) {
    
    $file = $_FILES['csv_file']['tmp_name'];
    $fileName = $_FILES['csv_file']['name']; // checking duplicate files
    
    // 1. Check if file already uploaded (Local Registry)
    $registryFile = 'uploads_registry.json';
    $registry = file_exists($registryFile) ? json_decode(file_get_contents($registryFile), true) : [];
    
    if (in_array($fileName, $registry)) {
        // File already processed
        // Redirect with error
        $error = "File '$fileName' has already been uploaded.";
        header("Location: index.php?view=upload&error=" . urlencode($error));
        exit;
    }

    if (is_uploaded_file($file)) {
        try {
            $handle = fopen($file, "r");
            
            if ($handle === false) {
                throw new Exception("Could not open file.");
            }

            // Get Headers
            $headers = fgetcsv($handle);
            if (!$headers) {
                throw new Exception("File is empty or invalid CSV.");
            }

            // 0. Define Tables
            $stagingTable = 'hive.sre.UFAA_23203159';
            $prodTable    = 'iceberg.adhoc.ufaa_23203159';

            // 1. CLEAR STAGING TABLE
            // User requirement: "Replace" data in Hive first.
            // ERROR: "Cannot delete from non-managed Hive table"
            // FIX: We cannot run DELETE. We must use INSERT OVERWRITE for the FIRST batch of data.
            // $pdo->query("DELETE FROM $stagingTable"); // REMOVED
            
            $isFirstBatch = true; // Flag to trigger OVERWRITE on first insert

            // 2. GET COLUMNS (from Staging Table to match CSV)
            // We query 1 row from STAGING to get the EXACT column names (handling BOMs etc)
            $metaStmt = $pdo->query("SELECT * FROM $stagingTable LIMIT 1");
            $metaRow = $metaStmt->fetch(PDO::FETCH_ASSOC);
            
            if (!$metaRow) {
                // Table might be empty (we just deleted it, or it was new).
                // If empty, we can't auto-detect columns from data.
                // WE MUST RELY ON CSV HEADERS matching DB columns if DB is empty.
                // Or we can try DESCRIBE? Trino: SHOW COLUMNS FROM table
                $colsStmt = $pdo->query("SHOW COLUMNS FROM $stagingTable"); // Better way to get cols if empty
                $colsData = $colsStmt->fetchAll();
                $realColumns = array_column($colsData, 'Column');
                
                if (empty($realColumns)) {
                     // Fallback if SHOW COLUMNS fails (unlikely)
                     $realColumns = $headers;
                }
            } else {
                $realColumns = array_keys($metaRow);
            }

            // Create Normalization Map: clean_name -> real_name
            $colMap = [];
            foreach ($realColumns as $realCol) {
                // Remove underscore too, so "owner_name" becomes "ownername"
                $cleanKey = preg_replace('/[^a-zA-Z0-9]/', '', strtolower($realCol));
                $colMap[$cleanKey] = $realCol;
            }

            // 3. Process CSV Headers
            $headers = fgetcsv($handle);
            if (!$headers) {
                throw new Exception("File is empty or invalid CSV.");
            }

            // Map CSV Headers to Staging DB Columns
            $targetColumns = [];
            $targetIndices = [];
            
            foreach ($headers as $idx => $csvCol) {
                $rawHeader = trim($csvCol);
                if ($rawHeader === '') continue; // Skip empty headers

                $cleanCsv = preg_replace('/[^a-zA-Z0-9]/', '', strtolower($rawHeader));
                
                if (isset($colMap[$cleanCsv])) {
                    $targetColumns[] = $colMap[$cleanCsv];
                    $targetIndices[] = $idx;
                } else {
                    $targetColumns[] = $rawHeader; // Fallback
                    $targetIndices[] = $idx;
                }
            }
            
            if (empty($targetColumns)) {
                throw new Exception("No valid columns found in CSV.");
            }

            // Re-identify Date Columns
            $isDateCol = [];
            foreach ($targetColumns as $i => $colName) {
                $lowerCol = strtolower($colName);
                if (strpos($lowerCol, 'dob') !== false || strpos($lowerCol, 'transaction_date') !== false) {
                    $isDateCol[$i] = true;
                }
            }

            // 4. UPLOAD CSV TO STAGING (Bulk Insert)
            $batchSize = 200;
            $rowsBuffer = [];
            $columnListSQL = implode(", ", array_map(function($c) { return "\"$c\""; }, $targetColumns));
            
            $rowCount = 0;

            while (($data = fgetcsv($handle)) !== false) {
                $rowValues = [];
                foreach ($targetIndices as $i => $csvIdx) {
                    $val = isset($data[$csvIdx]) ? trim($data[$csvIdx]) : null;
                    
                    if ($val === '') {
                        $val = "NULL";
                    } else {
                         if (isset($isDateCol[$i])) {
                             $val = transformDate($val);
                         }
                
                         $val = "'" . str_replace("'", "''", $val) . "'"; // Safe Quote
                    }
                    $rowValues[] = $val;
                }
                
                $rowsBuffer[] = "(" . implode(", ", $rowValues) . ")";
                $rowCount++;

                if (count($rowsBuffer) >= $batchSize) {
                    $valuesSQL = implode(", ", $rowsBuffer);
                    
                    // Strategy: First batch OVERWRITES table. Subsequent batches APPEND.
                    $insertCmd = $isFirstBatch ? "INSERT OVERWRITE" : "INSERT INTO";
                    
                    $batchSQL = "$insertCmd $stagingTable ($columnListSQL) VALUES $valuesSQL";
                    $pdo->query($batchSQL);
                    
                    $rowsBuffer = [];
                    $isFirstBatch = false; // Subsequent batches must append
                }
            }
            
            // Flush remaining
            if (count($rowsBuffer) > 0) {
                $valuesSQL = implode(", ", $rowsBuffer);
                
                // If the file was small (only 1 batch), this might be the first and only batch.
                $insertCmd = $isFirstBatch ? "INSERT OVERWRITE" : "INSERT INTO";
                
                $batchSQL = "$insertCmd $stagingTable ($columnListSQL) VALUES $valuesSQL";
                $pdo->query($batchSQL);
            }

            // 5. PROMOTE TO ICEBERG (The magic step)
            // SQL: INSERT INTO iceberg... SELECT * FROM hive...
            $promoSQL = "INSERT INTO $prodTable SELECT * FROM $stagingTable";
            $pdo->query($promoSQL);

            // Log success to registry
            $registry[] = $fileName;
            file_put_contents($registryFile, json_encode($registry));

            fclose($handle);

            $message = "Successfully uploaded $rowCount rows to Staging and promoted to Production.";

        } catch (Exception $e) {
            if ($pdo->inTransaction()) {
                $pdo->rollBack();
            }
            $error = "Error: " . $e->getMessage();
        }
    } else {
        $error = "No file uploaded.";
    }
}

// Helper to convert confusing kinds of dates to YYYY-MM-DD
function transformDate($dateStr) {
    $dateStr = trim($dateStr);
    if (empty($dateStr)) return null;

    // Try Standard Y-m-d first
    $d = DateTime::createFromFormat('Y-m-d', $dateStr);
    if ($d && $d->format('Y-m-d') === $dateStr) return $dateStr;

    // Try Common formats
    $formats = [
        'd/m/Y', 'm/d/Y', 'd-m-Y', 'Y/m/d', 'd.m.Y'
    ];

    foreach ($formats as $fmt) {
        $d = DateTime::createFromFormat($fmt, $dateStr);
        if ($d) {
             return $d->format('Y-m-d');
        }
    }

    // Fallback: Parsing
    $ts = strtotime($dateStr);
    if ($ts) {
        return date('Y-m-d', $ts);
    }

    // Return original if failed (Worst case DB rejects it, but we tried)
    return $dateStr;
}

// Redirect back to index with message
// We use query params for simple state
if ($error) {
    header("Location: index.php?view=upload&error=" . urlencode($error));
} else {
    header("Location: index.php?view=view_data&message=" . urlencode($message));
}
exit;

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
            $prodTable    = 'iceberg.adhoc.ufaa_23203159';
            // Generate a unique staging table name to avoid concurrency issues and manually clearing tables
            $stagingTable = 'hive.sre.ufaa_staging_' . uniqid() . '_' . rand(1000, 9999);

            try {
                // 1. CREATE DYNAMIC STAGING TABLE (Clone structure from Prod)
                // We use WITH NO DATA to just get the columns
                $pdo->query("CREATE TABLE $stagingTable AS SELECT * FROM $prodTable WITH NO DATA");
            } catch (Exception $e) {
                // If creation fails (maybe permissions?), we abort early
               throw new Exception("Failed to create staging table [$stagingTable]: " . $e->getMessage());
            }
            
            // 2. GET COLUMNS (from the New Staging Table - which matches Prod)
            // SHOW COLUMNS is denied, but SELECT is allowed.
            // We run a dummy query to get the column metadata from the Trino JSON response.
            $metaStmt = $pdo->query("SELECT * FROM $stagingTable WHERE 1=0");
            
            // TrinoClient now supports getColumns() to retrieve headers even if no data
            $realColumns = $metaStmt->getColumns();
            
            if (empty($realColumns)) {
                 // Fallback: If for some reason 1=0 returns no column signature (unlikely in Trino),
                 // we rely on the creation itself. The creation was 'AS SELECT * FROM Prod'.
                 // Let's try to query Prod for headers if Staging fails?
                 // But permissions might be same.
                 throw new Exception("Could not retrieve columns from generated staging table.");
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
                    // If column doesn't match DB, we skip it or error? 
                    // User preference: "intelligent mapping". 
                    // If we try to insert into a column that doesn't exist, SQL fails.
                    // So we only include matched columns.
                    continue; 
                }
            }
            
            if (empty($targetColumns)) {
                throw new Exception("No matching columns found between CSV and Database.");
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
            $batchSize = 250; // Trino handles smaller batches better for massive INSERT ... VALUES
            $rowsBuffer = [];
            $columnListSQL = implode(", ", array_map(function($c) { return "\"$c\""; }, $targetColumns));
            
            $rowCount = 0;

            while (($data = fgetcsv($handle)) !== false) {
                $rowValues = [];
                // ONLY iterate targetIndices so we match the columns we defined
                foreach ($targetIndices as $i => $csvIdx) {
                    $val = isset($data[$csvIdx]) ? trim($data[$csvIdx]) : null;
                    
                    if ($val === '' || $val === null) {
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
                    
                    // ALWAYS INSERT INTO (Dynamic table is fresh/empty)
                    $batchSQL = "INSERT INTO $stagingTable ($columnListSQL) VALUES $valuesSQL";
                    $pdo->query($batchSQL);
                    
                    $rowsBuffer = [];
                }
            }
            
            // Flush remaining
            if (count($rowsBuffer) > 0) {
                $valuesSQL = implode(", ", $rowsBuffer);
                $batchSQL = "INSERT INTO $stagingTable ($columnListSQL) VALUES $valuesSQL";
                $pdo->query($batchSQL);
            }

            // 5. PROMOTE TO ICEBERG
            // SQL: INSERT INTO iceberg... SELECT * FROM hive_staging...
            // Note: Since we only inserted matching columns, we should specify columns here too?
            // "INSERT INTO Prod (col1, col2) SELECT col1, col2 FROM Staging"
            // But Staging and Prod have same schema (Created from Prod).
            // So "SELECT *" is safe IF we filled unrelated columns with NULL in staging?
            // Actually, if we ommitted columns in Staging Insert, they are NULL.
            // So Schema matches perfectly.
            
            $promoSQL = "INSERT INTO $prodTable SELECT * FROM $stagingTable";
            $pdo->query($promoSQL);

            // 6. CLEANUP
            $pdo->query("DROP TABLE $stagingTable");

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

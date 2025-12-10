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

            // 1. Fetch Real DB Columns (to handle BOMs mismatch)
            // We query 1 row to get the EXACT column names from the DB
            $metaStmt = $pdo->query("SELECT * FROM $tableName LIMIT 1");
            $metaRow = $metaStmt->fetch(PDO::FETCH_ASSOC);
            
            if (!$metaRow) {
                // Table might be empty, so we can't auto-detect. 
                // Fallback: Use hardcoded list we know from previous context, or fail gracefully.
                // Known columns: ﻿owner_name, owner_dob, owner_id, transaction_date, transaction_time, owner_due_amount
                // We'll try to proceed with CSV headers if empty, but usually we have data.
                $realColumns = $headers; 
            } else {
                $realColumns = array_keys($metaRow);
            }

            // Create Normalization Map: clean_name -> real_name
            // e.g. "owner_name" -> "﻿owner_name"
            $colMap = [];
            foreach ($realColumns as $realCol) {
                // Remove non-alphanumeric to get "clean" key
                $cleanKey = preg_replace('/[^a-zA-Z0-9_]/', '', strtolower($realCol));
                $colMap[$cleanKey] = $realCol;
            }

            // 2. Process CSV Headers
            $headers = fgetcsv($handle);
            if (!$headers) {
                throw new Exception("File is empty or invalid CSV.");
            }

            // Map CSV Headers to Real DB Columns & Track Indices
            $targetColumns = []; // The DB column names we will insert into
            $targetIndices = []; // The CSV index corresponding to that column
            
            foreach ($headers as $idx => $csvCol) {
                $rawHeader = trim($csvCol);
                if ($rawHeader === '') {
                    // Skip empty headers (fixes "Zero-length delimited identifier" error)
                    continue; 
                }

                $cleanCsv = preg_replace('/[^a-zA-Z0-9_]/', '', strtolower($rawHeader));
                
                if (isset($colMap[$cleanCsv])) {
                    $targetColumns[] = $colMap[$cleanCsv];
                    $targetIndices[] = $idx;
                } else {
                    // Fallback using the raw header if not found in DB map
                    // (Ensure it's not empty, which we checked above)
                    $targetColumns[] = $rawHeader;
                    $targetIndices[] = $idx;
                }
            }
            
            if (empty($targetColumns)) {
                throw new Exception("No valid columns found in CSV.");
            }

            // Re-identify Date Columns based on FINAL names
            $dateColIndices = []; // Keyed by position in our NEW target list, or checking name?
            // Actually, we check the name in $targetColumns.
            // But we need to know when processing data...
            // Let's just check the name inside the data loop or build a map: targetIndex => isDate
            $isDateCol = [];
            foreach ($targetColumns as $i => $colName) {
                $lowerCol = strtolower($colName);
                if (strpos($lowerCol, 'dob') !== false || strpos($lowerCol, 'transaction_date') !== false) {
                    $isDateCol[$i] = true;
                }
            }

            // Use the FILTERED list for SQL
            $colString = implode(", ", array_map(function($c) { return "\"$c\""; }, $targetColumns)); 
            $valPlaceholders = implode(", ", array_fill(0, count($targetColumns), "?"));

            // Performance Optimization: BULK INSERT
            // Instead of executing 1 query per row (slow over HTTP), we group 500 rows into ONE query.
            // INSERT INTO table (...) VALUES (r1), (r2), (r3)...
            
            $batchSize = 200; // 200 is safe for URL limits
            $rowsBuffer = [];
            $columnListSQL = implode(", ", array_map(function($c) { return "\"$c\""; }, $targetColumns));
            
            $rowCount = 0;

            while (($data = fgetcsv($handle)) !== false) {
                // Map Data
                $rowValues = [];
                foreach ($targetIndices as $i => $csvIdx) {
                    $val = isset($data[$csvIdx]) ? trim($data[$csvIdx]) : null;
                    
                    if ($val === '') {
                        $val = "NULL";
                    } else {
                         if (isset($isDateCol[$i])) {
                             $val = transformDate($val);
                         }
                         
                         // Trino Safe Quoting:
                         $val = "'" . str_replace("'", "''", $val) . "'";
                    }
                    $rowValues[] = $val;
                }
                
                // Add (v1, v2, v3) to buffer
                $rowsBuffer[] = "(" . implode(", ", $rowValues) . ")";
                $rowCount++;

                // If Buffer Full, Execute
                if (count($rowsBuffer) >= $batchSize) {
                    $valuesSQL = implode(", ", $rowsBuffer);
                    $batchSQL = "INSERT INTO $tableName ($columnListSQL) VALUES $valuesSQL";
                    $pdo->query($batchSQL); // Direct execute
                    
                    $rowsBuffer = []; // Reset
                }
            }
            
            // Flush remaining rows
            if (count($rowsBuffer) > 0) {
                $valuesSQL = implode(", ", $rowsBuffer);
                $batchSQL = "INSERT INTO $tableName ($columnListSQL) VALUES $valuesSQL";
                $pdo->query($batchSQL);
            }

            // Commit (although Trino auto-commits usually, this is fine)
            // No explicit transaction wrapping per batch needed for this generic client

            // Log success to registry
            $registry[] = $fileName;
            file_put_contents($registryFile, json_encode($registry));

            fclose($handle);

            $message = "Successfully uploaded $rowCount rows.";

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

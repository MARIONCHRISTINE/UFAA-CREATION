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

            // Map CSV Headers to Real DB Columns
            $finalCols = [];
            foreach ($headers as $csvCol) {
                $cleanCsv = preg_replace('/[^a-zA-Z0-9_]/', '', strtolower(trim($csvCol)));
                
                if (isset($colMap[$cleanCsv])) {
                    $finalCols[] = $colMap[$cleanCsv];
                } else {
                    // Column in CSV not found in DB? 
                    // We can skip it or try to use it as is. 
                    // Let's use as is (quoted) and hope, but likely will fail if strict.
                    $finalCols[] = trim($csvCol);
                }
            }
            
            // Re-identify Date Columns based on FINAL names
            $dateColIndices = [];
            foreach ($finalCols as $idx => $colName) {
                $lowerCol = strtolower($colName);
                if (strpos($lowerCol, 'dob') !== false || strpos($lowerCol, 'transaction_date') !== false) {
                    $dateColIndices[] = $idx;
                }
            }

            // Quote columns correctly
            $colString = implode(", ", array_map(function($c) { return "\"$c\""; }, $finalCols)); 
            $valPlaceholders = implode(", ", array_fill(0, count($finalCols), "?"));

            $sql = "INSERT INTO $tableName ($colString) VALUES ($valPlaceholders)";
            $stmt = $pdo->prepare($sql);

            $pdo->beginTransaction();
            
            $rowCount = 0;
            $batchCount = 0;

            while (($data = fgetcsv($handle)) !== false) {
                // Skip if row column count doesn't match header count
                if (count($data) !== count($headers)) { // Compare against original header count
                    continue; 
                }
                
                // If we mapped columns, we must ensure data aligns. 
                // Assuming CSV order matches Header order, so index mapping is 1:1.

                // Process Data (Transform Dates & Handle Blanks)
                foreach ($data as $key => $val) {
                    $val = trim($val);
                    if ($val === '') {
                        $data[$key] = null; 
                    } else {
                        if (in_array($key, $dateColIndices)) {
                            $data[$key] = transformDate($val);
                        }
                    }
                }

                $stmt->execute($data);
                $rowCount++;
                $batchCount++;

                // Commit every Batch Size
                if ($batchCount >= $batchSize) {
                    $pdo->commit();
                    $pdo->beginTransaction();
                    $batchCount = 0;
                }
            }

            // Commit remaining
            $pdo->commit();

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

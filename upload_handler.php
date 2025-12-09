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

            // Headers: Just trim, do NOT regex remove characters (Preserve BOM for Trino)
            // We assume CSV headers exactly match DB Column names now.
            $columns = array_map('trim', $headers);
            
            // identify Date Columns for processing (Specific to user request)
            $dateColIndices = [];
            foreach ($columns as $idx => $colName) {
                $lowerCol = strtolower($colName);
                // User specified: owner_dob and transaction_date
                if (strpos($lowerCol, 'dob') !== false || strpos($lowerCol, 'transaction_date') !== false) {
                    $dateColIndices[] = $idx;
                }
            }

            $colString = implode(", ", array_map(function($c) { return "\"$c\""; }, $columns)); // Quote columns for safe SQL
            $valPlaceholders = implode(", ", array_fill(0, count($columns), "?"));

            $sql = "INSERT INTO $tableName ($colString) VALUES ($valPlaceholders)";
            $stmt = $pdo->prepare($sql);

            $pdo->beginTransaction();
            
            $rowCount = 0;
            $batchCount = 0;

            while (($data = fgetcsv($handle)) !== false) {
                // Skip if row column count doesn't match header count
                if (count($data) !== count($columns)) {
                    continue; 
                }

                // Process Data (Transform Dates & Handle Blanks)
                foreach ($data as $key => $val) {
                    $val = trim($val);
                    if ($val === '') {
                        $data[$key] = null; // Send NULL for blanks, don't try to fill
                    } else {
                        // Apply Date Transform only if it's a date column
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

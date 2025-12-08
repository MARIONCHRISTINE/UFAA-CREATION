<?php
require_once 'db.php';

// Configuration
$tableName = 'iceberg.adhoc.ufaa_23203159'; // Updated to full qualified name based on your DBeaver queries
$batchSize = 1000; // Rows per transaction

$message = "";
$error = "";

if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_FILES['csv_file'])) {
    
    $file = $_FILES['csv_file']['tmp_name'];
    
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

            // Sanitize headers for SQL (prevent injections)
            $columns = array_map(function($h) {
                return preg_replace('/[^a-zA-Z0-9_]/', '', trim($h));
            }, $headers);
            
            $colString = implode(", ", $columns);
            $valPlaceholders = implode(", ", array_fill(0, count($columns), "?"));

            // Check if columns exist in DB (Soft check or just try Insert)
            // We will trust the user that CSV headers match DB columns for now.

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
            fclose($handle);

            $message = "Successfully uploaded $rowCount rows to the database.";

        } catch (Exception $e) {
            if ($pdo->inTransaction()) {
                $pdo->rollBack();
            }
            $error = "Error: " . $e->getMessage();
        }
    } else {
        $error = "No file uploaded or upload error.";
    }
} else {
    // If accessed directly without post
    header("Location: index.php");
    exit;
}

// Redirect back to index with message
// We use query params for simple state
if ($error) {
    header("Location: index.php?view=upload&error=" . urlencode($error));
} else {
    header("Location: index.php?view=view_data&message=" . urlencode($message));
}
exit;

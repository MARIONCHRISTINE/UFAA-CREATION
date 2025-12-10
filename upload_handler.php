<?php
require_once 'db.php';
require_once 'helpers.php';

// Configuration
$stagingTable = 'hive.sre.UFAA_23203159'; 
$prodTable    = 'iceberg.adhoc.ufaa_23203159';
$batchSize    = 1000; 

$message = "";
$error = "";

if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_FILES['csv_file'])) {
    
    $file = $_FILES['csv_file']['tmp_name'];
    $fileName = $_FILES['csv_file']['name']; 
    
    // 1. Check duplicate
    $registryFile = 'uploads_registry.json';
    $registry = file_exists($registryFile) ? json_decode(file_get_contents($registryFile), true) : [];
    
    if (in_array($fileName, $registry)) {
        $error = "File '$fileName' has already been uploaded.";
        header("Location: index.php?view=upload&error=" . urlencode($error));
        exit;
    }

    if (is_uploaded_file($file)) {
        try {
            $handle = fopen($file, "r");
            if (!$handle) throw new Exception("Could not open file.");

            // 1. CLEAR STAGING TABLE
            try {
                $pdo->query("DELETE FROM $stagingTable"); 
            } catch (Exception $e) {
                // Ignore delete error (might be non-transactional or empty)
            }
            
            // 2. DETECT HEADERS (Smart Hunt)
            $headers = null;
            for ($i = 0; $i < 5; $i++) {
                $rawRow = fgetcsv($handle);
                if (!$rawRow) break;
                
                // Clean BOM
                if (isset($rawRow[0])) $rawRow[0] = preg_replace('/^[\xEF\xBB\xBF]+/', '', $rawRow[0]);
                
                // Check content
                $hasText = false;
                $hasData = false;
                foreach ($rawRow as $cell) {
                    if (width_contains_text($cell)) $hasText = true;
                    if (preg_match('/\d{2}\/\d{2}\/\d{4}/', $cell) || is_numeric($cell)) $hasData = true;
                }
                
                if ($hasText && !$hasData) {
                    $headers = $rawRow; // Found candidates
                    break; 
                }
                
                if ($hasData) {
                    // Hit data -> Assume headerless or missed it. 
                    // Treat this as the header row to force index-based logic or fail.
                    $headers = $rawRow;
                    break;
                }
            }
            
            if (!$headers) {
                rewind($handle);
                $headers = fgetcsv($handle);
                if (isset($headers[0])) $headers[0] = preg_replace('/^[\xEF\xBB\xBF]+/', '', $headers[0]);
            }

            if (!$headers) throw new Exception("File implies empty.");

            // 3. MAP COLUMNS
            // Blind Trust: "Owner Name" -> "owner_name"
            $targetColumns = [];
            $targetIndices = [];
            
            foreach ($headers as $idx => $val) {
                $val = trim($val);
                if ($val === '') continue;
                
                // Normalize to DB style
                $dbCol = strtolower($val);
                $dbCol = preg_replace('/\s+/', '_', $dbCol); // Space -> _
                $dbCol = preg_replace('/[^a-z0-9_]/', '', $dbCol); // Cleanup
                
                // Heuristic for data-as-header fallback
                // If the "header" is actually "70.00", the dbCol will be "7000". This is fine, Trino will reject it, 
                // but at least we try.
                // UNLESS the user relies on Index Mapping?
                // We'll trust the user's file is usually valid.
                
                $targetColumns[] = $dbCol;
                $targetIndices[] = $idx;
            }
            
            if (empty($targetColumns)) throw new Exception("No valid columns found.");

            // Identify Date Columns for transformation
            $isDateCol = [];
            foreach ($targetColumns as $i => $col) {
                if (strpos($col, 'dob') !== false || strpos($col, 'date') !== false) {
                    $isDateCol[$i] = true;
                }
            }

            // 4. INSERT LOOP
            $batchSize = 250; 
            $rowsBuffer = [];
            $columnListSQL = implode(", ", array_map(function($c) { return "\"$c\""; }, $targetColumns));
            $rowCount = 0;
            
            // SPECIAL: If $headers was actually data?
            // If the last column of "headers" looks like a number, it's probably data.
            $lastVal = $headers[count($headers)-1] ?? '';
            if (is_numeric($lastVal) || preg_match('/\d{2}\/\d{2}\/\d{4}/', $lastVal)) {
                 // Insert this row first
                 $rowValues = [];
                 foreach ($targetIndices as $i => $csvIdx) {
                    $val = isset($headers[$csvIdx]) ? trim($headers[$csvIdx]) : null;
                    if ($val === '' || $val === null) $val = "NULL";
                    else {
                        if (isset($isDateCol[$i])) $val = transformDate($val);
                        $val = "'" . str_replace("'", "''", $val) . "'";
                    }
                    $rowValues[] = $val;
                 }
                 $rowsBuffer[] = "(" . implode(", ", $rowValues) . ")";
                 $rowCount++;
            }

            while (($data = fgetcsv($handle)) !== false) {
                $rowValues = [];
                foreach ($targetIndices as $i => $csvIdx) {
                    $val = isset($data[$csvIdx]) ? trim($data[$csvIdx]) : null;
                    if ($val === '' || $val === null) $val = "NULL";
                    else {
                        if (isset($isDateCol[$i])) $val = transformDate($val);
                        $val = "'" . str_replace("'", "''", $val) . "'";
                    }
                    $rowValues[] = $val;
                }
                
                $rowsBuffer[] = "(" . implode(", ", $rowValues) . ")";
                $rowCount++;

                if (count($rowsBuffer) >= $batchSize) {
                    $valuesSQL = implode(", ", $rowsBuffer);
                    $batchSQL = "INSERT INTO $stagingTable ($columnListSQL) VALUES $valuesSQL";
                    $pdo->query($batchSQL);
                    $rowsBuffer = [];
                }
            }
            
            if (count($rowsBuffer) > 0) {
                $valuesSQL = implode(", ", $rowsBuffer);
                $batchSQL = "INSERT INTO $stagingTable ($columnListSQL) VALUES $valuesSQL";
                $pdo->query($batchSQL);
            }

            // 5. PROMOTE
            $promoSQL = "INSERT INTO $prodTable SELECT * FROM $stagingTable";
            $pdo->query($promoSQL);

            // Log
            $registry[] = $fileName;
            file_put_contents($registryFile, json_encode($registry));
            fclose($handle);

            $message = "Successfully uploaded $rowCount rows.";

        } catch (Exception $e) {
            $error = "Error: " . $e->getMessage();
        }
    } else {
        $error = "No file uploaded.";
    }
}

function transformDate($dateStr) {
    if (empty($dateStr)) return null;
    // 1. Try ISO
    $d = DateTime::createFromFormat('Y-m-d', $dateStr);
    if ($d && $d->format('Y-m-d') === $dateStr) return $dateStr;
    
    // 2. Try Parser
    $ts = strtotime($dateStr);
    if ($ts) return date('Y-m-d', $ts);
    
    // 3. Try Formats
    $formats = ['d/m/Y', 'm/d/Y', 'd.m.Y'];
    foreach ($formats as $fmt) {
        $d = DateTime::createFromFormat($fmt, $dateStr);
        if ($d) return $d->format('Y-m-d');
    }
    return $dateStr;
}

if ($error) {
    header("Location: index.php?view=upload&error=" . urlencode($error));
} else {
    header("Location: index.php?view=view_data&message=" . urlencode($message));
}
exit;
?>

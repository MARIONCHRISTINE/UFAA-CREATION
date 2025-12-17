<?php
require_once 'db_data.php';
require_once 'helpers_data.php';

// Configuration for 2025 TABLES
$stagingTable = 'hive.sre.ufaadata2025'; 
$prodTable    = 'iceberg.adhoc.ufaadata2025';
$batchSize    = 1000; 

$message = "";
$error = "";

// Helper: Get last code from counter file
function getLastCode() {
    $file = __DIR__ . '/code_counter_data.json';
    if (!file_exists($file)) {
        return 0;
    }
    $data = json_decode(file_get_contents($file), true);
    return (int)($data['last_code'] ?? 0);
}

// Helper: Save new code to counter file
function saveLastCode($code, $uploadCount = 0) {
    $file = __DIR__ . '/code_counter_data.json';
    $data = [
        'last_code' => $code,
        'last_updated' => date('Y-m-d H:i:s'),
        'total_uploads' => $uploadCount
    ];
    file_put_contents($file, json_encode($data, JSON_PRETTY_PRINT));
}

// Helper: Format code as 8-digit padded
function formatOwnerCode($number) {
    return sprintf("%08d", $number);
}

if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_FILES['csv_file'])) {
    
    $file = $_FILES['csv_file']['tmp_name'];
    $fileName = $_FILES['csv_file']['name'];
    
    // 1. Check duplicate
    $registryFile = 'uploads_registry_data.json';
    $registry = file_exists($registryFile) ? json_decode(file_get_contents($registryFile), true) : [];
    
    if (in_array($fileName, $registry)) {
        $error = "File '$fileName' has already been uploaded.";
        header("Location: index_data.php?view=upload&error=" . urlencode($error));
        exit;
    }

    if (is_uploaded_file($file)) {
        try {
            $handle = fopen($file, "r");
            if (!$handle) throw new Exception("Could not open file.");

            // Get starting code
            $counterData = json_decode(file_get_contents(__DIR__ . '/code_counter_data.json'), true);
            $codeCounter = (int)($counterData['last_code'] ?? 0);
            $uploadCount = (int)($counterData['total_uploads'] ?? 0);

            // 1. CLEAR STAGING TABLE
            try {
                $pdo->query("DELETE FROM $stagingTable"); 
            } catch (Exception $e) {
                // Ignore if fails
            }
            
            // 2. DETECT HEADERS
            $headers = null;
            for ($i = 0; $i < 5; $i++) {
                $rawRow = fgetcsv($handle);
                if (!$rawRow) break;
                
                if (isset($rawRow[0])) $rawRow[0] = preg_replace('/^[\xEF\xBB\xBF]+/', '', $rawRow[0]);
                
                $hasText = false;
                $hasData = false;
                foreach ($rawRow as $cell) {
                    if (width_contains_text($cell)) $hasText = true;
                    if (preg_match('/\d{2}\/\d{2}\/\d{4}/', $cell) || is_numeric($cell)) $hasData = true;
                }
                
                if ($hasText && !$hasData) {
                    $headers = $rawRow;
                    break; 
                }
                
                if ($hasData) {
                    $headers = $rawRow;
                    break;
                }
            }
            
            if (!$headers) {
                rewind($handle);
                $headers = fgetcsv($handle);
                if (isset($headers[0])) $headers[0] = preg_replace('/^[\xEF\xBB\xBF]+/', '', $headers[0]);
            }

            if (!$headers) throw new Exception("File is empty.");

            // 3. MAP COLUMNS (Expected: 7 columns from CSV, we add owner_code)
            $targetColumns = ['owner_code']; // First column is auto-generated
            $targetIndices = [];
            
            foreach ($headers as $idx => $val) {
                $val = trim($val);
                if ($val === '') continue;
                
                $dbCol = strtolower($val);
                $dbCol = preg_replace('/\s+/', '_', $dbCol);
                $dbCol = preg_replace('/[^a-z0-9_]/', '', $dbCol);
                
                $targetColumns[] = $dbCol;
                $targetIndices[] = $idx;
            }
            
            if (count($targetColumns) < 2) throw new Exception("No valid columns found.");

            // Identify Date Columns
            $isDateCol = [];
            foreach ($targetColumns as $i => $col) {
                if (strpos($col, 'dob') !== false || strpos($col, 'date') !== false) {
                    $isDateCol[$i] = true;
                }
            }

            // 4. INSERT LOOP WITH AUTO-CODE GENERATION
            $rowsBuffer = [];
            $columnListSQL = implode(", ", array_map(function($c) { return "\"$c\""; }, $targetColumns));
            $rowCount = 0;
            
            // Check if first row is data
            $lastVal = $headers[count($headers)-1] ?? '';
            if (is_numeric($lastVal) || preg_match('/\d{2}\/\d{2}\/\d{4}/', $lastVal)) {
                 $codeCounter++;
                 $ownerCode = formatOwnerCode($codeCounter);
                 
                 $rowValues = ["'$ownerCode'"];
                 foreach ($targetIndices as $i => $csvIdx) {
                    $val = isset($headers[$csvIdx]) ? trim($headers[$csvIdx]) : null;
                    if ($val === '' || $val === null) $val = "NULL";
                    else {
                        if (isset($isDateCol[$i+1])) $val = transformDate($val);
                        $val = "'" . str_replace("'", "''", $val) . "'";
                    }
                    $rowValues[] = $val;
                 }
                 $rowsBuffer[] = "(" . implode(", ", $rowValues) . ")";
                 $rowCount++;
            }

            while (($data = fgetcsv($handle)) !== false) {
                $codeCounter++;
                $ownerCode = formatOwnerCode($codeCounter);
                
                $rowValues = ["'$ownerCode'"];
                foreach ($targetIndices as $i => $csvIdx) {
                    $val = isset($data[$csvIdx]) ? trim($data[$csvIdx]) : null;
                    if ($val === '' || $val === null) $val = "NULL";
                    else {
                        if (isset($isDateCol[$i+1])) $val = transformDate($val);
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

            // 5. PROMOTE TO ICEBERG
            $promoSQL = "INSERT INTO $prodTable SELECT * FROM $stagingTable";
            $pdo->query($promoSQL);

            // 6. SAVE NEW COUNTER
            saveLastCode($codeCounter, $uploadCount + 1);

            // Log
            $registry[] = $fileName;
            file_put_contents($registryFile, json_encode($registry));
            fclose($handle);

            $message = "Successfully uploaded $rowCount rows. Codes: " . formatOwnerCode($codeCounter - $rowCount + 1) . " to " . formatOwnerCode($codeCounter);

        } catch (Exception $e) {
            $error = "Error: " . $e->getMessage();
        }
    } else {
        $error = "No file uploaded.";
    }
}

function transformDate($dateStr) {
    if (empty($dateStr)) return null;
    $d = DateTime::createFromFormat('Y-m-d', $dateStr);
    if ($d && $d->format('Y-m-d') === $dateStr) return $dateStr;
    
    $ts = strtotime($dateStr);
    if ($ts) return date('Y-m-d', $ts);
    
    $formats = ['d/m/Y', 'm/d/Y', 'd.m.Y'];
    foreach ($formats as $fmt) {
        $d = DateTime::createFromFormat($fmt, $dateStr);
        if ($d) return $d->format('Y-m-d');
    }
    return $dateStr;
}

if ($error) {
    header("Location: index_data.php?view=upload&error=" . urlencode($error));
} else {
    header("Location: index_data.php?view=view_data&message=" . urlencode($message));
}
exit;
?>

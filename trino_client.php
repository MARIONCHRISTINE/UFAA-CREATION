<?php
// Simple Trino/Presto Client using CURL (Since standard PDO_MYSQL doesn't work with Trino HTTPS)
// Mimics a basic PDO interface for compatibility with existing code

class TrinoClient {
    private $host;
    private $port;
    private $user;
    private $pass;
    private $catalog;
    private $schema;
    private $sslCert;
    private $inTransaction = false;

    public function __construct($host, $port, $user, $pass, $catalog, $schema, $sslCert = null) {
        $this->host = $host;
        $this->port = $port;
        $this->user = $user;
        $this->pass = $pass;
        $this->catalog = $catalog;
        $this->schema = $schema;
        $this->sslCert = $sslCert;
    }

    public function query($sql) {
        // Simple internal query exec
        return $this->execRaw($sql);
    }

    // Main execution logic
    private function execRaw($sql) {
        // Trino API Endpoint
        $url = "https://{$this->host}:{$this->port}/v1/statement";
        
        $headers = [
            "X-Trino-User: {$this->user}",
            "X-Trino-Catalog: {$this->catalog}",
            "X-Trino-Schema: {$this->schema}",
            "Content-Type: text/plain"
        ];

        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_POST, 1);
        curl_setopt($ch, CURLOPT_POSTFIELDS, $sql);
        curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false); 
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0); 
        curl_setopt($ch, CURLOPT_USERPWD, "{$this->user}:{$this->pass}");
        curl_setopt($ch, CURLOPT_TIMEOUT, 120); // Increased timeout for heavy queries

        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $error = curl_error($ch);
        curl_close($ch);

        if ($error) {
            throw new Exception("CURL Error: $error");
        }

        if ($httpCode !== 200) {
            // Try to extract error from JSON if possible
            $json = json_decode($response, true);
            $msg = $json['error']['message'] ?? $response;
            throw new Exception("Trino API Error ($httpCode): $msg");
        }

        return $this->fetchAllResults(json_decode($response, true));
    }

    private function fetchAllResults($initialResponse) {
        $columns = [];
        $data = [];
        
        $currentResponse = $initialResponse;

        while (true) {
            // Process current chunk
            if (isset($currentResponse['columns']) && empty($columns)) {
                foreach ($currentResponse['columns'] as $col) {
                    $columns[] = $col['name'];
                }
            }

            if (isset($currentResponse['data'])) {
                foreach ($currentResponse['data'] as $row) {
                     // Combine keys with values if columns are known
                    if (!empty($columns)) {
                         $data[] = array_combine($columns, $row);
                    } else {
                         return new TrinoStatement([]); // Error state?
                    }
                }
            }

            if (isset($currentResponse['nextUri'])) {
                $ch = curl_init();
                curl_setopt($ch, CURLOPT_URL, $currentResponse['nextUri']);
                curl_setopt($ch, CURLOPT_HTTPHEADER, ["X-Trino-User: {$this->user}"]);
                curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
                curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
                curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0);
                
                $res = curl_exec($ch);
                curl_close($ch);
                $currentResponse = json_decode($res, true);
            } else {
                break;
            }
            
            if (isset($currentResponse['error'])) {
                 throw new Exception("Trino Query Error: " . $currentResponse['error']['message']);
            }
        }

        return new TrinoStatement($data);
    }
    
    public function prepare($sql) {
        return new TrinoStatement([], $this, $sql);
    }
    
    // Transaction Stubs (Trino doesn't support interactive transactions over REST the same way, usually auto-commit)
    public function beginTransaction() { $this->inTransaction = true; return true; }
    public function commit() { $this->inTransaction = false; return true; }
    public function rollBack() { $this->inTransaction = false; return true; }
    public function inTransaction() { return $this->inTransaction; }
}

class TrinoStatement {
    private $data;
    private $client;
    private $pendingSql;
    private $boundParams = [];
    private $iterator = 0;

    public function __construct($data, $client = null, $pendingSql = null) {
        $this->data = $data;
        $this->client = $client;
        $this->pendingSql = $pendingSql;
    }

    // Compatibility: bindValue
    public function bindValue($param, $value, $type = null) {
        $this->boundParams[$param] = ['value' => $value, 'type' => $type];
        return true;
    }

    public function execute($params = []) {
        if ($this->client && $this->pendingSql) {
            $sql = $this->pendingSql;
            
            // Merge direct params with bound params
            // boundParams structure: key => ['value'=>..., 'type'=>...]
            // params structure: key => value (or indexed)
            
            $finalParams = [];
            
            // 1. Add bound params
            foreach ($this->boundParams as $k => $v) {
                $finalParams[$k] = $v;
            }
            
            // 2. Add execute params (override if conflict)
            foreach ($params as $k => $v) {
                $finalParams[$k] = ['value' => $v, 'type' => null];
            }

            // Replace logic
            // Check if we are doing Named (:id) or Indexed (?) replacement
            
            // HEURISTIC: Does SQL contain '?' ?
            if (strpos($sql, '?') !== false) {
                 // Indexed replacement
                 // NOTE: Array keys in $finalParams might be 0,1,2 OR ':name'. 
                 // If using ?, we expect 0,1,2.
                 // We limit to simple sequential logic
                 
                 // Sort by key to ensure order if mixed? Just take values in order is safer for ?
                 $values = array_column($finalParams, 'value');
                 
                 foreach ($values as $val) {
                     // Check if empty string or number
                     $replacement = $this->quote($val);
                     // Replace FIRST occurrence of ?
                     $pos = strpos($sql, '?');
                     if ($pos !== false) {
                         $sql = substr_replace($sql, $replacement, $pos, 1);
                     }
                 }
                 
            } else {
                // Named replacement
                foreach ($finalParams as $key => $info) {
                    $val = $info['value'];
                    $type = $info['type'];
                    
                    // Determine if quoting is needed
                    $replacement = $this->quote($val, $type);
                    
                    if (is_string($key)) {
                        // Ensure key has colon
                        $search = (strpos($key, ':') === 0) ? $key : ":$key";
                        $sql = str_replace($search, $replacement, $sql);
                    }
                }
            }

            // Execute real query
            $resultStmt = $this->client->query($sql);
            $this->data = $resultStmt->fetchAll(); // Copy data from result statement to this statement
            $this->iterator = 0;
        }
        return true;
    }

    private function quote($val, $type = null) {
        if ($val === null) return 'NULL';
        
        // If type is explicitly INT, or looks like INT, return as is (Trino is strict)
        // PDO::PARAM_INT is 1
        if ($type === 1 || is_int($val) || (is_numeric($val) && (int)$val == $val)) {
            return (int)$val;
        }
        
        // String
        return "'" . addslashes($val) . "'";
    }

    public function fetchAll($mode = null) {
        return $this->data;
    }

    public function fetch($mode = null) {
        if (!isset($this->data[$this->iterator])) return false;
        $row = $this->data[$this->iterator];
        $this->iterator++;
        return $row;
    }
    
    public function fetchColumn() {
        $row = $this->fetch();
        if (!$row) return false;
        return reset($row);
    }
    
    public function rowCount() {
        return count($this->data);
    }
}
?>

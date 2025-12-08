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
        // Trino API Endpoint for Statement Execution
        $url = "https://{$this->host}:{$this->port}/v1/statement";
        
        $headers = [
            "X-Trino-User: {$this->user}",
            "X-Trino-Catalog: {$this->catalog}",
            "X-Trino-Schema: {$this->schema}",
            "Content-Type: text/plain"
        ];

        // Basic Auth
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_POST, 1);
        curl_setopt($ch, CURLOPT_POSTFIELDS, $sql);
        curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false); // Bypass Check for verify (simplification for "Simple App")
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0); 
        curl_setopt($ch, CURLOPT_USERPWD, "{$this->user}:{$this->pass}");
        curl_setopt($ch, CURLOPT_TIMEOUT, 30); // 30s timeout

        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $error = curl_error($ch);
        curl_close($ch);

        if ($error) {
            throw new Exception("CURL Error: $error");
        }

        if ($httpCode !== 200) {
            throw new Exception("Trino API Error ($httpCode): $response");
        }

        // Response is JSON with "nextUri" for paging results.
        // We need to loop to get all data.
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
                    // Combine keys with values
                    $data[] = array_combine($columns, $row);
                }
            }

            // Check if there is a next page
            if (isset($currentResponse['nextUri'])) {
                $ch = curl_init();
                curl_setopt($ch, CURLOPT_URL, $currentResponse['nextUri']);
                curl_setopt($ch, CURLOPT_HTTPHEADER, ["X-Trino-User: {$this->user}"]); // Minimal auth as URI contains token usually
                curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
                curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
                curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0);
                
                $res = curl_exec($ch);
                curl_close($ch);
                $currentResponse = json_decode($res, true);
            } else {
                break; // Job done
            }
            
            // Safety break for infinite loops or massive data without pagination handling at app level
            if (isset($currentResponse['error'])) {
                 throw new Exception("Trino Query Error: " . $currentResponse['error']['message']);
            }
        }

        // Return a Pseudo-Statement object
        return new TrinoStatement($data);
    }
    
    // Helper to mimic PDO prepare() -> execute()
    // Trino REST API doesn't support Prepared Statements in the same way, 
    // so we just return self and handle execute manually if possible, or simple query.
    // For this simple app, we will assume direct queries for now or do client-side interpolation (Risk: Injection, but Internal App).
    public function prepare($sql) {
        return new TrinoStatement([], $this, $sql);
    }
}

class TrinoStatement {
    private $data;
    private $client;
    private $pendingSql;

    public function __construct($data, $client = null, $pendingSql = null) {
        $this->data = $data;
        $this->client = $client;
        $this->pendingSql = $pendingSql;
    }

    public function fetchAll($mode = null) {
        return $this->data;
    }

    public function fetch($mode = null) {
        $row = current($this->data);
        next($this->data);
        return $row;
    }
    
    public function fetchColumn() {
        if (empty($this->data)) return 0;
        return reset($this->data[0]);
    }

    // Since Trino is not PDO, we implement a fake execute for the "View Data" page
    public function execute($params = []) {
        if ($this->client && $this->pendingSql) {
            // Simple string replacement for binding (Very basic, treat with caution)
            $sql = $this->pendingSql;
            foreach ($params as $key => $val) {
                // If named param
                 if (is_string($key)) {
                    $sql = str_replace($key, "'" . addslashes($val) . "'", $sql);
                 }
            }
            // Execute real query now
            $resultStmt = $this->client->query($sql);
            $this->data = $resultStmt->fetchAll();
        }
        return true;
    }
    
    public function rowCount() {
        return count($this->data);
    }
}
?>

<?php
// auth_db.php - Handles SQLite connection for user management

$dbFile = __DIR__ . '/users.db';

try {
    $authDb = new PDO("sqlite:$dbFile");
    $authDb->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    $authDb->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);

    // Create users table if not exists
    $query = "CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )";
    $authDb->exec($query);

} catch (PDOException $e) {
    die("Authentication Database Error: " . $e->getMessage());
}
?>

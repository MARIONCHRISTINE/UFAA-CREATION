<?php
require_once 'auth_db.php';
session_start();

$error = '';

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $username = trim($_POST['username'] ?? '');
    $password = $_POST['password'] ?? '';

    if (empty($username) || empty($password)) {
        $error = "Please enter username and password.";
    } else {
        try {
            $stmt = $authDb->prepare("SELECT id, password FROM users WHERE username = :u");
            $stmt->execute([':u' => $username]);
            $user = $stmt->fetch();

            if ($user && password_verify($password, $user['password'])) {
                // Login Success
                $_SESSION['user_id'] = $user['id'];
                $_SESSION['username'] = $username; // Optional: store name
                header("Location: index.php");
                exit;
            } else {
                $error = "Invalid username or password.";
            }
        } catch (Exception $e) {
            $error = "Login Error: " . $e->getMessage();
        }
    }
}
?>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Login - UFAA Manager</title>
    <link rel="stylesheet" href="style.css">
    <style>
        body { display: flex; align-items: center; justify-content: center; min-height: 100vh; padding: 0; }
        .auth-card { width: 100%; max-width: 400px; padding: 2.5rem; }
    </style>
</head>
<body>
    <div class="glass-card auth-card fade-in">
        <h2 style="text-align: center; margin-bottom: 2rem;">Welcome Back</h2>
        
        <?php if ($error): ?>
            <div class="alert alert-error"><?php echo htmlspecialchars($error); ?></div>
        <?php endif; ?>

        <form method="POST" action="">
            <div class="form-group">
                <label class="form-label">Username</label>
                <input type="text" name="username" class="form-control" required autofocus>
            </div>
            <div class="form-group">
                <label class="form-label">Password</label>
                <input type="password" name="password" class="form-control" required>
            </div>
            
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1.5rem;">
                <label style="display: flex; align-items: center; gap: 0.5rem; color: var(--text-muted); font-size: 0.9rem;">
                    <input type="checkbox"> Remember me
                </label>
                <a href="reset_password.php" style="color: var(--primary-color); font-size: 0.9rem;">Forgot Password?</a>
            </div>

            <button type="submit" class="btn btn-primary" style="width: 100%;">Sign In</button>
        </form>
        
        <p style="text-align: center; margin-top: 1.5rem; color: var(--text-muted);">
            Don't have an account? <a href="register.php" style="color: var(--primary-color);">Register</a>
        </p>
    </div>
</body>
</html>

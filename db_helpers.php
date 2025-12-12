<?php
// Gemeenschappelijke database helperfuncties

function default_db_config() {
    return [
        'host' => getenv('DB_HOST') ?: '127.0.0.1',
        'port' => getenv('DB_PORT') ?: '3306',
        'name' => getenv('DB_NAME') ?: 'brugmelding',
        'user' => getenv('DB_USER') ?: 'brugmelding_user',
        'pass' => getenv('DB_PASS') ?: '',
        'table' => 'bridge_status_history'
    ];
}

function load_db_config() {
    $config = default_db_config();
    $credentialsFile = __DIR__ . '/.db_credentials.php';

    if (file_exists($credentialsFile)) {
        $fileConfig = include $credentialsFile;
        if (is_array($fileConfig)) {
            $config = array_merge($config, $fileConfig);
        }
    }

    return $config;
}

function sanitize_table_name($name) {
    if (!preg_match('/^[A-Za-z0-9_]+$/', $name)) {
        die("Ongeldige tabelnaam voor statusgeschiedenis.\n");
    }

    return $name;
}

/**
 * Initialiseer MySQL database voor statusgeschiedenis.
 */
function init_db($config, $table)
{
    $dsn = sprintf(
        'mysql:host=%s;port=%s;dbname=%s;charset=utf8mb4',
        $config['host'],
        $config['port'],
        $config['name']
    );

    $pdo = new PDO($dsn, $config['user'], $config['pass']);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    $pdo->exec(sprintf(
        'CREATE TABLE IF NOT EXISTS `%s` (
            id INT UNSIGNED NOT NULL AUTO_INCREMENT,
            bridge_id VARCHAR(255) NOT NULL,
            status VARCHAR(32) NOT NULL,
            recorded_at VARCHAR(64) NOT NULL,
            PRIMARY KEY (id),
            INDEX bridge_id_idx (bridge_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;',
        $table
    ));

    return $pdo;
}

/**
 * Sla een status op als deze gewijzigd is t.o.v. de laatst bekende.
 */
function log_status(?PDO $pdo, $bridgeId, $status, $timestamp, $table)
{
    // Als de database niet beschikbaar is, slaan we de logging over zodat de hoofdscript kan doorgaan
    if ($pdo === null) {
        return;
    }

    $stmt = $pdo->prepare("SELECT status, recorded_at FROM `{$table}` WHERE bridge_id = ? ORDER BY id DESC LIMIT 1");
    $stmt->execute([$bridgeId]);
    $last = $stmt->fetch(PDO::FETCH_ASSOC);

    if ($last && $last['status'] === $status) {
        return; // geen wijziging ten opzichte van vorige status
    }

    $insert = $pdo->prepare("INSERT INTO `{$table}` (bridge_id, status, recorded_at) VALUES (?, ?, ?)");
    $insert->execute([$bridgeId, $status, $timestamp]);
}

/**
 * Haal de laatste statusmutaties op (nieuwste eerst).
 */
function fetch_history(PDO $pdo, $bridgeId, $table, $limit = 5)
{
    $stmt = $pdo->prepare("SELECT status, recorded_at FROM `{$table}` WHERE bridge_id = ? ORDER BY id DESC LIMIT ?");
    $stmt->bindValue(1, $bridgeId);
    $stmt->bindValue(2, (int)$limit, PDO::PARAM_INT);
    $stmt->execute();

    return $stmt->fetchAll(PDO::FETCH_ASSOC);
}

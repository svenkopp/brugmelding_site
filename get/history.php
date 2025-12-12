<?php
require_once __DIR__ . '/../db_helpers.php';

header('Content-Type: application/json');

$dbConfig = load_db_config();
$historyTable = sanitize_table_name($dbConfig['table']);

$bridgeId = isset($_GET['id']) ? trim((string)$_GET['id']) : '';
$limit = isset($_GET['limit']) ? (int)$_GET['limit'] : 10;
$limit = $limit > 0 ? $limit : 10;
$hours = isset($_GET['hours']) ? (int)$_GET['hours'] : 24;
$hours = $hours > 0 ? $hours : 24;

if ($bridgeId === '') {
    http_response_code(400);
    echo json_encode(['error' => 'Parameter "id" is verplicht.']);
    exit;
}

try {
    $pdo = init_db($dbConfig, $historyTable);
    $history = fetch_history($pdo, $bridgeId, $historyTable, $limit, $hours);
    echo json_encode($history);
} catch (Throwable $e) {
    http_response_code(500);
    echo json_encode(['error' => 'Kon geschiedenis niet ophalen.']);
}

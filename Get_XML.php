<?php
// Get_XML.php
// Snelle, robuuste versie met automatische JSON-check voor foute bruggen.
// Zorg dat map 'get' schrijfbaar is door de webserver voor log & output.

require_once __DIR__ . '/db_helpers.php';

date_default_timezone_set("UTC");

// ---------- Config ----------
$jsonInputFile  = __DIR__ . '/get/bruggen.json';
$jsonOutputFile = __DIR__ . '/get/bruggen_open.json';
$logBadFile     = __DIR__ . '/get/foute_bruggen.log';
$missingNdwFile = __DIR__ . '/get/ontbrekende_ndw_ids.json';
$ndwUrl         = "http://opendata.ndw.nu/brugopeningen.xml.gz";

$dbConfig = load_db_config();

// ---------- Helpers ----------
function safe_get_string($var) {
    return isset($var) ? (string)$var : '';
}

function extract_ndw_identifier(SimpleXMLElement $situation): string {
    $attributes = $situation->attributes();
    $situationId = isset($attributes['id']) ? safe_get_string($attributes['id']) : '';

    if ($situationId === '') {
        return '';
    }

    $parts = explode('_', $situationId);
    return $parts[1] ?? $situationId;
}

function parse_overall_start_time(SimpleXMLElement $situation): ?DateTime {
    $startRaw = safe_get_string($situation->situationRecord->validity->validityTimeSpecification->overallStartTime ?? null);
    if ($startRaw === '') {
        return null;
    }

    try {
        return new DateTime($startRaw);
    } catch (Exception $e) {
        return null;
    }
}

function select_closest_situation(?SimpleXMLElement $current, SimpleXMLElement $candidate, DateTime $now): SimpleXMLElement {
    if ($current === null) {
        return $candidate;
    }

    $candidateStart = parse_overall_start_time($candidate);
    if ($candidateStart === null) {
        return $current;
    }

    $currentStart = parse_overall_start_time($current);
    if ($currentStart === null) {
        return $candidate;
    }

    $diffCandidate = abs($now->getTimestamp() - $candidateStart->getTimestamp());
    $diffCurrent   = abs($now->getTimestamp() - $currentStart->getTimestamp());

    return ($diffCandidate < $diffCurrent) ? $candidate : $current;
}

function load_missing_ndw_log(string $filePath): array {
    if (!file_exists($filePath)) {
        return [];
    }

    $content = file_get_contents($filePath);
    $decoded = json_decode($content, true);

    if (!is_array($decoded)) {
        return [];
    }

    $map = [];
    foreach ($decoded as $entry) {
        if (!isset($entry['ndwID'])) continue;
        $map[$entry['ndwID']] = $entry;
    }

    return $map;
}

function remember_missing_ndw(array &$missingMap, string $ndwId, array $brug): void {
    if ($ndwId === '' || isset($missingMap[$ndwId])) {
        return;
    }

    $missingMap[$ndwId] = [
        'ndwID' => $ndwId,
        'latitude' => isset($brug['latitude']) ? (float)$brug['latitude'] : null,
        'longitude' => isset($brug['longitude']) ? (float)$brug['longitude'] : null,
        'naam' => $brug['naam'] ?? '',
        'firstSeen' => date('c')
    ];
}

function save_missing_ndw_log(string $filePath, array $missingMap): void {
    file_put_contents($filePath, json_encode(array_values($missingMap), JSON_PRETTY_PRINT));
}

// ---------- Inlezen bruggen.json ----------
if (!file_exists($jsonInputFile)) {
    die("Input bestand niet gevonden: $jsonInputFile\n");
}

$json_data = file_get_contents($jsonInputFile);
$bruggen_raw = json_decode($json_data, true);

if (!is_array($bruggen_raw)) {
    // Log en stop
    file_put_contents($logBadFile, date('c') . " - Ongeldige JSON in bruggen.json\n", FILE_APPEND);
    die("bruggen.json kon niet worden geparsed als JSON.\n");
}

// ---------- Valideer en normaliseer JSON (en log foute items) ----------
$bruggen = [];
foreach ($bruggen_raw as $index => $item) {

    // Normalize keys: sommige bestanden hebben Lat/Lon/Naam/ISRS
    // Maak een tolk zodat de rest van de code met id/latitude/longitude/naam kan werken.
    if (isset($item['ISRS'])) {
        $item['id'] = $item['ISRS'];
    }
    if (isset($item['Lat'])) {
        $item['latitude'] = $item['Lat'];
    }
    if (isset($item['Lon'])) {
        $item['longitude'] = $item['Lon'];
    }
    if (isset($item['Naam'])) {
        $item['naam'] = $item['Naam'];
    }
    if (isset($item['ndwid'])) {
        $item['ndwID'] = $item['ndwid'];
    }

    // Vereiste velden (id, latitude, longitude, naam)
    $ok = true;
    $missing = [];

    if (!isset($item['id']) || $item['id'] === '') {
        $ok = false;
        $missing[] = 'id';
    }
    if (!isset($item['latitude']) || $item['latitude'] === '' || !is_numeric($item['latitude'])) {
        $ok = false;
        $missing[] = 'latitude';
    }
    if (!isset($item['longitude']) || $item['longitude'] === '' || !is_numeric($item['longitude'])) {
        $ok = false;
        $missing[] = 'longitude';
    }
    if (!isset($item['naam']) || $item['naam'] === '') {
        $ok = false;
        $missing[] = 'naam';
    }

    if (!$ok) {
        $logLine = date('c') . " - Ongeldige brug op index $index. Missende/invalid keys: " . implode(',', $missing) . "\n";
        $logLine .= print_r($item, true) . "\n---\n";
        file_put_contents($logBadFile, $logLine, FILE_APPEND);
        continue; // skip deze brug
    }

    // Alles OK: voeg toe aan genormaliseerde lijst
    $bruggen[] = $item;
}

if (count($bruggen) === 0) {
    die("Geen geldige bruggen gevonden na validatie. Kijk in $logBadFile voor details.\n");
}

// ---------- Log voor ontbrekende NDW ID's ----------
$missingNdwLog = load_missing_ndw_log($missingNdwFile);

// ---------- Database init ----------
$historyTable = sanitize_table_name($dbConfig['table']);
$pdo = null;

try {
    $pdo = init_db($dbConfig, $historyTable);
} catch (Throwable $e) {
    // Geen database beschikbaar: log dit en ga verder zodat de hoofdscript niet crasht
    file_put_contents(
        $logBadFile,
        date('c') . " - Database niet beschikbaar: " . $e->getMessage() . "\n",
        FILE_APPEND
    );
}

// ---------- NDW XML ophalen en parsen ----------
$xml_gz_content = @file_get_contents($ndwUrl);
if ($xml_gz_content === false) {
    file_put_contents($logBadFile, date('c') . " - Fout bij ophalen NDW URL: $ndwUrl\n", FILE_APPEND);
    die("Kon NDW XML niet ophalen.\n");
}

$xml_content = @gzdecode($xml_gz_content);
if ($xml_content === false) {
    file_put_contents($logBadFile, date('c') . " - Fout bij gzdecode van NDW content\n", FILE_APPEND);
    die("Fout bij gzdecode.\n");
}

$rcXML = @simplexml_load_string($xml_content);
if ($rcXML === false) {
    file_put_contents($logBadFile, date('c') . " - Fout bij simplexml_load_string op NDW content\n", FILE_APPEND);
    die("Fout bij parsen NDW XML.\n");
}

// Navigeer naar situations (defensie tegen ontbrekende nodes)
$envelope = $rcXML->children('http://schemas.xmlsoap.org/soap/envelope/');
$body     = $envelope->Body ?? null;
$datex    = $body ? $body->children('http://datex2.eu/schema/2/2_0') : null;

if (!$datex || !isset($datex->d2LogicalModel->payloadPublication->situation)) {
    file_put_contents($logBadFile, date('c') . " - NDW XML heeft geen situation nodes\n", FILE_APPEND);
    // We gaan door met lege situaties
    $arraySituation = [];
} else {
    $arraySituation = $datex->d2LogicalModel->payloadPublication->situation;
}

// ---------- Indexeer situaties op NDW ID (uit het situation id attribuut) ----------
// We bewaren enkel situaties waarvan overallStartTime +2h > now en kiezen de starttime die het dichtst bij nu ligt.
$situationMap = [];
$now = new DateTime();

foreach ($arraySituation as $situation) {
    $ndwIdentifier = extract_ndw_identifier($situation);
    if ($ndwIdentifier === '') continue;

    $startDt = parse_overall_start_time($situation);
    if ($startDt === null) continue;

    $toekomst = (clone $startDt)->modify("+2 hours");
    if ($toekomst <= $now) continue;

    if (!isset($situationMap[$ndwIdentifier])) {
        $situationMap[$ndwIdentifier] = $situation;
        continue;
    }

    $situationMap[$ndwIdentifier] = select_closest_situation($situationMap[$ndwIdentifier], $situation, $now);
}

// ---------- Verwerk elke brug: lookup op NDW ID ----------
$dataArray = [];

foreach ($bruggen as $brug) {

    $found = null;
    $ndwIdentifier = safe_get_string($brug['ndwID'] ?? '');

    if ($ndwIdentifier !== '' && isset($situationMap[$ndwIdentifier])) {
        $found = $situationMap[$ndwIdentifier];
    } elseif ($ndwIdentifier !== '') {
        remember_missing_ndw($missingNdwLog, $ndwIdentifier, $brug);
    }

    // Vul het output-object (zelfde velden als jouw oude script, maar robuust)
    if ($found) {
        $SituationCurrent   = (string)($found->situationRecord->operatorActionStatus ?? '');
        $SituationVoorspeld = (string)($found->situationRecord->probabilityOfOccurrence ?? '');
        // attributes() kan ontbrekend zijn of korter zijn; bescherm tegen notices
        $attributes = $found->situationRecord->attributes();
        $ndwVersion = isset($attributes[1]) ? (string)$attributes[1] : "0";
        $GetDatumStart = (string)($found->situationRecord->validity->validityTimeSpecification->overallStartTime ?? '');

        if ($SituationVoorspeld === "certain") {
            $status = "open";
        } elseif ($SituationVoorspeld === "probable") {
            $status = "voorspeld";
        } else {
            $status = "dicht";
        }
    } else {
        $SituationCurrent   = "certain";
        $SituationVoorspeld = "beingTerminated";
        $ndwVersion         = "0";
        $status             = "dicht";
        $GetDatumStart      = (new DateTime())->format('Y-m-d\TH:i:s.v\Z');
    }

    $statusMoment = $GetDatumStart ?: (new DateTime())->format('Y-m-d\TH:i:s.v\Z');
    log_status($pdo, $brug['id'], $status, $statusMoment, $historyTable);

    // Bouw output voor deze brug
    $dataArray[] = [
        'Id' => $brug['id'],
        'Data' => [
            'latitude' => (float)$brug['latitude'],
            'longitude' => (float)$brug['longitude'],
            'SituationCurrent' => $SituationCurrent,
            'SituationVoorspeld' => $SituationVoorspeld,
            'ndwVersion' => $ndwVersion,
            'GetDatumStart' => $GetDatumStart,
            'Naam' => $brug['naam'],
            'Provincie' => $brug['provincie'],
            'Stad' => $brug['stad'],
            'status' => $status,
            'open' => ($status === "open") ? true : false
        ]
    ];
}

// ---------- Bewaar ontbrekende NDW ID's ----------
save_missing_ndw_log($missingNdwFile, $missingNdwLog);

// ---------- Sla output op ----------
file_put_contents($jsonOutputFile, json_encode($dataArray, JSON_PRETTY_PRINT));

// ---------- Einde ----------
echo "Verwerking klaar. Output: $jsonOutputFile\n";
echo "Foute items (indien aanwezig) in: $logBadFile\n";
?>
